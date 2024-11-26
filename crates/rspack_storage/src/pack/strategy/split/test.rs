#[cfg(test)]
pub mod test_pack_utils {
  use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
  };

  use itertools::Itertools;
  use rspack_error::Result;
  use rspack_paths::Utf8Path;
  use rustc_hash::FxHashMap as HashMap;

  use crate::{
    pack::{PackScope, SplitPackStrategy},
    PackFs, PackOptions,
  };

  pub async fn mock_meta_file(
    path: &Utf8Path,
    fs: Arc<dyn PackFs>,
    options: &PackOptions,
    pack_count: usize,
  ) -> Result<()> {
    fs.ensure_dir(path.parent().expect("should have parent"))
      .await?;
    let mut writer = fs.write_file(path).await?;
    let current = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .expect("should get current time")
      .as_millis() as u64;
    writer
      .line(format!("{} {} {}", options.buckets, options.max_pack_size, current).as_str())
      .await?;
    for bucket_id in 0..options.buckets {
      let mut pack_meta_list = vec![];
      for pack_no in 0..pack_count {
        let pack_name = format!("pack_name_{}_{}", bucket_id, pack_no);
        let pack_hash = format!("pack_hash_{}_{}", bucket_id, pack_no);
        let pack_size = 100;
        pack_meta_list.push(format!("{},{},{}", pack_name, pack_hash, pack_size));
      }
      writer.line(pack_meta_list.join(" ").as_str()).await?;
    }

    writer.flush().await?;

    Ok(())
  }

  pub async fn mock_pack_file(
    path: &Utf8Path,
    unique_id: &str,
    item_count: usize,
    fs: Arc<dyn PackFs>,
  ) -> Result<()> {
    fs.ensure_dir(path.parent().expect("should have parent"))
      .await?;
    let mut writer = fs.write_file(&path).await?;
    let mut keys = vec![];
    let mut contents = vec![];
    for i in 0..item_count {
      keys.push(format!("key_{}_{}", unique_id, i).as_bytes().to_vec());
      contents.push(format!("val_{}_{}", unique_id, i).as_bytes().to_vec());
    }
    writer
      .line(keys.iter().map(|k| k.len()).join(" ").as_str())
      .await?;
    writer
      .line(contents.iter().map(|k| k.len()).join(" ").as_str())
      .await?;
    for key in keys {
      writer.bytes(&key).await?;
    }
    for content in contents {
      writer.bytes(&content).await?;
    }
    writer.flush().await?;
    Ok(())
  }

  pub enum UpdateVal {
    Value(String),
    Removed,
  }

  pub fn mock_updates(
    start: usize,
    end: usize,
    length: usize,
    value: UpdateVal,
  ) -> HashMap<Vec<u8>, Option<Vec<u8>>> {
    let mut updates = HashMap::default();
    for i in start..end {
      updates.insert(
        format!("{:0>length$}_key", i, length = length - 4)
          .as_bytes()
          .to_vec(),
        match &value {
          UpdateVal::Value(str) => Some(
            format!("{:0>length$}_{}", i, str, length = length - (str.len() + 1))
              .as_bytes()
              .to_vec(),
          ),
          UpdateVal::Removed => None,
        },
      );
    }

    updates
  }

  pub fn count_scope_packs(scope: &PackScope) -> usize {
    scope.packs.expect_value().iter().flatten().count()
  }
  pub fn count_bucket_packs(scope: &PackScope) -> Vec<usize> {
    scope
      .packs
      .expect_value()
      .iter()
      .map(|i| i.len())
      .collect_vec()
  }
  pub fn get_bucket_pack_sizes(scope: &PackScope) -> Vec<usize> {
    let mut res = scope
      .meta
      .expect_value()
      .packs
      .iter()
      .flatten()
      .map(|c| c.size)
      .collect_vec();
    res.sort_unstable();
    res
  }

  pub async fn clean_scope_path(
    scope: &PackScope,
    strategy: &SplitPackStrategy,
    fs: Arc<dyn PackFs>,
  ) {
    fs.remove_dir(&scope.path).await.expect("should remove dir");
    fs.remove_dir(
      &strategy
        .get_temp_path(&scope.path)
        .expect("should get temp path"),
    )
    .await
    .expect("should remove dir");
  }

  pub async fn flush_file_mtime(path: &Utf8Path, fs: Arc<dyn PackFs>) -> Result<()> {
    let content = fs.read_file(path).await?.remain().await?;
    fs.write_file(path).await?.write(&content).await?;

    Ok(())
  }
}
