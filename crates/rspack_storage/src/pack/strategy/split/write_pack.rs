use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use itertools::Itertools;
use rspack_error::{error, Result};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

use super::SplitPackStrategy;
use crate::{
  pack::{
    strategy::split::util::get_name, Pack, PackContents, PackContentsState, PackFileMeta, PackKeys,
    PackKeysState, PackWriteStrategy, UpdatePacksResult,
  },
  PackOptions,
};

#[async_trait]
impl PackWriteStrategy for SplitPackStrategy {
  async fn update_packs(
    &self,
    dir: PathBuf,
    options: &PackOptions,
    packs: HashMap<PackFileMeta, Pack>,
    updates: HashMap<Arc<Vec<u8>>, Option<Arc<Vec<u8>>>>,
  ) -> UpdatePacksResult {
    let mut indexed_packs = packs.into_iter().enumerate().collect::<HashMap<_, _>>();
    let mut indexed_updates = updates.into_iter().enumerate().collect::<HashMap<_, _>>();

    let item_to_pack =
      indexed_packs
        .iter()
        .fold(HashMap::default(), |mut acc, (pack_index, (_, pack))| {
          let PackKeysState::Value(keys) = &pack.keys else {
            return acc;
          };
          for key in keys {
            acc.insert(key.clone(), pack_index.clone());
          }
          acc
        });

    let mut removed_packs = HashSet::default();
    let mut insert_items = HashSet::default();
    let mut removed_items = HashSet::default();

    let mut removed_files = vec![];

    // pour out items from non-full packs
    for (index, (pack_meta, _)) in indexed_packs.iter() {
      if (pack_meta.size as f64) < (options.max_pack_size as f64) * 0.8_f64 {
        removed_packs.insert(*index);
      }
    }

    // get dirty packs and items for insert/remove
    for (index, (key, val)) in indexed_updates.iter() {
      if val.is_some() {
        insert_items.insert(*index);
        if let Some(pack_index) = item_to_pack.get(key) {
          removed_packs.insert(*pack_index);
        }
      } else {
        removed_items.insert(*index);
        if let Some(pack_index) = item_to_pack.get(key) {
          removed_packs.insert(*pack_index);
        }
      }
    }

    // pour out items from removed packs
    let mut items = removed_packs
      .iter()
      .fold(HashMap::default(), |mut acc, pack_index| {
        let (_, old_pack) = indexed_packs
          .remove(pack_index)
          .expect("should have bucket pack");

        removed_files.push(old_pack.path.clone());

        let (PackKeysState::Value(keys), PackContentsState::Value(contents)) =
          (old_pack.keys, old_pack.contents)
        else {
          return acc;
        };
        if keys.len() != contents.len() {
          return acc;
        }

        acc.extend(
          keys
            .into_iter()
            .zip(contents.into_iter())
            .collect::<HashMap<_, _>>(),
        );

        acc
      })
      .into_iter()
      .collect::<HashMap<_, _>>();

    // add insert items
    items.extend(
      insert_items
        .iter()
        .map(|key| {
          let item = indexed_updates.remove(key).expect("should have index item");
          (item.0, item.1.expect("should have item value"))
        })
        .collect::<HashMap<_, _>>(),
    );

    // remove items
    for key in removed_items.iter() {
      let (key, _) = indexed_updates.remove(key).expect("should have index item");
      let _ = items.remove(&key);
    }

    let remain_packs = indexed_packs.into_values().collect_vec();
    let new_packs: Vec<(PackFileMeta, Pack)> = create(&dir, options, items).await;

    UpdatePacksResult {
      new_packs,
      remain_packs,
      removed_files,
    }
  }

  async fn write_pack(&self, pack: &Pack) -> Result<()> {
    let path = self.get_temp_path(&pack.path)?;
    let keys = pack.keys.expect_value();
    let contents = pack.contents.expect_value();
    if keys.len() != contents.len() {
      return Err(error!("pack keys and contents length not match"));
    }
    self
      .fs
      .ensure_dir(&PathBuf::from(path.parent().expect("should have parent")))
      .await?;

    let mut writer = self.fs.write_file(&path).await?;

    // key meta line
    writer
      .line(
        keys
          .iter()
          .map(|key| key.len().to_string())
          .collect::<Vec<_>>()
          .join(" ")
          .as_str(),
      )
      .await?;

    // content meta line
    writer
      .line(
        contents
          .iter()
          .map(|content| content.len().to_string())
          .collect::<Vec<_>>()
          .join(" ")
          .as_str(),
      )
      .await?;

    for key in keys {
      writer.bytes(key).await?;
    }

    for content in contents {
      writer.bytes(content).await?;
    }

    writer.flush().await?;

    Ok(())
  }
}

async fn create(
  dir: &PathBuf,
  options: &PackOptions,
  items: HashMap<Arc<Vec<u8>>, Arc<Vec<u8>>>,
) -> Vec<(PackFileMeta, Pack)> {
  let mut items = items.into_iter().collect_vec();
  items.sort_unstable_by(|a, b| a.1.len().cmp(&b.1.len()));

  let mut new_packs = vec![];

  fn create_pack(dir: &PathBuf, keys: PackKeys, contents: PackContents) -> (PackFileMeta, Pack) {
    let file_name = get_name(&keys, &contents);
    let mut new_pack = Pack::new(dir.join(file_name.clone()));
    new_pack.keys = PackKeysState::Value(keys);
    new_pack.contents = PackContentsState::Value(contents);
    (
      PackFileMeta {
        name: file_name,
        hash: Default::default(),
        size: new_pack.size(),
        writed: false,
      },
      new_pack,
    )
  }

  loop {
    if items.is_empty() {
      break;
    }
    let last_item = items.last().expect("should have first item");
    // handle big single cache
    if last_item.0.len() as f64 + last_item.1.len() as f64 > options.max_pack_size as f64 * 0.8_f64
    {
      let (key, value) = items.pop().expect("shoud have first item");
      new_packs.push(create_pack(dir, vec![key], vec![value]));
    } else {
      break;
    }
  }

  items.reverse();

  loop {
    let mut batch_keys: PackKeys = vec![];
    let mut batch_contents: PackContents = vec![];
    let mut batch_size = 0_usize;

    loop {
      if items.len() == 0 {
        break;
      }

      let last_item = items.last().expect("should have first item");

      if batch_size + last_item.0.len() + last_item.1.len() > options.max_pack_size {
        break;
      }

      let (key, value) = items.pop().expect("shoud have first item");
      batch_size += value.len() + key.len();
      batch_keys.push(key);
      batch_contents.push(value);
    }

    if !batch_keys.is_empty() {
      new_packs.push(create_pack(dir, batch_keys, batch_contents));
    }

    if items.is_empty() {
      break;
    }
  }

  new_packs
}

#[cfg(test)]
mod tests {
  use std::{path::PathBuf, sync::Arc};

  use itertools::Itertools;
  use rspack_error::Result;
  use rustc_hash::FxHashMap as HashMap;

  use crate::{
    pack::{
      strategy::split::test::test_pack_utils::{mock_updates, UpdateVal},
      Pack, PackContentsState, PackFileMeta, PackKeysState, PackWriteStrategy, SplitPackStrategy,
      UpdatePacksResult,
    },
    PackFs, PackMemoryFs, PackOptions,
  };

  async fn test_write_pack(strategy: &SplitPackStrategy) -> Result<()> {
    let mut pack = Pack::new(PathBuf::from("/cache/test_write_pack/pack"));
    pack.keys = PackKeysState::Value(vec![
      Arc::new("key_1".as_bytes().to_vec()),
      Arc::new("key_2".as_bytes().to_vec()),
    ]);
    pack.contents = PackContentsState::Value(vec![
      Arc::new("val_1".as_bytes().to_vec()),
      Arc::new("val_2".as_bytes().to_vec()),
    ]);
    strategy.write_pack(&pack).await?;

    let mut reader = strategy
      .fs
      .read_file(
        &strategy
          .get_temp_path(&pack.path)
          .expect("should get temp path"),
      )
      .await?;
    assert_eq!(reader.line().await?, "5 5");
    assert_eq!(reader.line().await?, "5 5");
    assert_eq!(reader.bytes(5).await?, "key_1".as_bytes());
    assert_eq!(reader.bytes(5).await?, "key_2".as_bytes());
    assert_eq!(reader.bytes(5).await?, "val_1".as_bytes());
    assert_eq!(reader.bytes(5).await?, "val_2".as_bytes());
    Ok(())
  }

  fn update_packs(update_res: UpdatePacksResult) -> HashMap<PackFileMeta, Pack> {
    update_res
      .remain_packs
      .into_iter()
      .chain(
        update_res
          .new_packs
          .into_iter()
          .map(|(meta, pack)| (meta, pack)),
      )
      .collect::<HashMap<PackFileMeta, Pack>>()
  }

  fn get_pack_sizes(update_res: &UpdatePacksResult) -> Vec<usize> {
    let mut sizes = update_res
      .remain_packs
      .iter()
      .map(|(_, pack)| pack.size())
      .chain(update_res.new_packs.iter().map(|(_, pack)| pack.size()))
      .collect_vec();
    sizes.sort_unstable();
    sizes
  }

  async fn test_update_packs(strategy: &SplitPackStrategy) -> Result<()> {
    let dir = PathBuf::from("/cache/test_update_packs");
    let options = PackOptions {
      buckets: 1,
      max_pack_size: 2000,
      expires: 100000,
    };

    // half pack
    let mut packs = HashMap::default();
    let res = strategy
      .update_packs(
        dir.clone(),
        &options,
        packs,
        mock_updates(0, 50, 10, UpdateVal::Value("val".into())),
      )
      .await;
    assert_eq!(res.new_packs.len(), 1);
    assert_eq!(res.remain_packs.len(), 0);
    assert_eq!(get_pack_sizes(&res), vec![1000]);

    packs = update_packs(res);

    // full pack
    let res = strategy
      .update_packs(
        dir.clone(),
        &options,
        packs,
        mock_updates(50, 100, 10, UpdateVal::Value("val".into())),
      )
      .await;
    assert_eq!(res.new_packs.len(), 1);
    assert_eq!(res.remain_packs.len(), 0);
    assert_eq!(res.removed_files.len(), 1);
    assert_eq!(get_pack_sizes(&res), vec![2000]);

    packs = update_packs(res);

    // almost full pack
    let res = strategy
      .update_packs(
        dir.clone(),
        &options,
        packs,
        mock_updates(100, 190, 10, UpdateVal::Value("val".into())),
      )
      .await;
    assert_eq!(res.new_packs.len(), 1);
    assert_eq!(res.remain_packs.len(), 1);
    assert_eq!(res.removed_files.len(), 0);
    assert_eq!(get_pack_sizes(&res), vec![1800, 2000]);

    packs = update_packs(res);

    let res = strategy
      .update_packs(
        dir.clone(),
        &options,
        packs,
        mock_updates(190, 200, 10, UpdateVal::Value("val".into())),
      )
      .await;
    assert_eq!(res.new_packs.len(), 1);
    assert_eq!(res.remain_packs.len(), 2);
    assert_eq!(res.removed_files.len(), 0);
    assert_eq!(get_pack_sizes(&res), vec![200, 1800, 2000]);

    packs = update_packs(res);

    // long item pack
    let mut updates = mock_updates(0, 1, 1200, UpdateVal::Value("val".into()));
    updates.extend(mock_updates(1, 2, 900, UpdateVal::Value("val".into())));
    let res = strategy
      .update_packs(dir.clone(), &options, packs, updates)
      .await;
    assert_eq!(res.new_packs.len(), 3);
    assert_eq!(res.remain_packs.len(), 2);
    assert_eq!(res.removed_files.len(), 1);
    assert_eq!(get_pack_sizes(&res), vec![200, 1800, 1800, 2000, 2400]);

    packs = update_packs(res);

    // remove items pack
    let res = strategy
      .update_packs(
        dir.clone(),
        &options,
        packs,
        mock_updates(100, 130, 10, UpdateVal::Removed),
      )
      .await;
    assert_eq!(res.new_packs.len(), 1);
    assert_eq!(res.remain_packs.len(), 3);
    assert_eq!(res.removed_files.len(), 2);
    assert_eq!(get_pack_sizes(&res), vec![1400, 1800, 2000, 2400]);

    packs = update_packs(res);

    // update items pack
    let mut updates = HashMap::default();
    updates.insert(
      Arc::new(format!("{:0>6}_key", 131).as_bytes().to_vec()),
      Some(Arc::new(format!("{:0>6}_valaaa", 131).as_bytes().to_vec())),
    );
    let res = strategy
      .update_packs(dir.clone(), &options, packs, updates)
      .await;
    assert_eq!(res.new_packs.len(), 1);
    assert_eq!(res.remain_packs.len(), 3);
    assert_eq!(res.removed_files.len(), 1);
    assert_eq!(get_pack_sizes(&res), vec![1403, 1800, 2000, 2400]);

    Ok(())
  }

  #[tokio::test]
  async fn should_write_pack() {
    let fs = Arc::new(PackMemoryFs::default());
    fs.remove_dir(&PathBuf::from("/cache/test_write_pack"))
      .await
      .expect("should clean dir");
    let strategy = SplitPackStrategy::new(
      PathBuf::from("/cache/test_write_pack"),
      PathBuf::from("/temp/test_write_pack"),
      fs.clone(),
    );

    let _ = test_write_pack(&strategy)
      .await
      .map_err(|e| panic!("{}", e));
  }

  #[tokio::test]
  async fn should_update_packs() {
    let fs = Arc::new(PackMemoryFs::default());
    fs.remove_dir(&PathBuf::from("/cache/test_update_packs"))
      .await
      .expect("should clean dir");
    let strategy = SplitPackStrategy::new(
      PathBuf::from("/cache/test_update_packs"),
      PathBuf::from("/temp/test_update_packs"),
      fs.clone(),
    );

    let _ = test_update_packs(&strategy)
      .await
      .map_err(|e| panic!("{}", e));
  }
}
