use async_trait::async_trait;
use futures::{future::join_all, TryFutureExt};
use itertools::Itertools;
use rspack_error::{error, Result};

use super::{util::get_indexed_packs, SplitPackStrategy};
use crate::pack::{PackScope, ScopeValidateStrategy, ValidateResult};

#[async_trait]
impl ScopeValidateStrategy for SplitPackStrategy {
  async fn validate_meta(&self, scope: &mut PackScope) -> Result<ValidateResult> {
    let meta = scope.meta.expect_value();

    if meta.buckets != scope.options.buckets {
      return Ok(ValidateResult::invalid("`options.buckets` changed"));
    }

    if meta.max_pack_size != scope.options.max_pack_size {
      return Ok(ValidateResult::invalid("`options.maxPackSize` changed"));
    }

    if scope.options.is_expired(&meta.last_modified) {
      return Ok(ValidateResult::invalid("expired"));
    }

    return Ok(ValidateResult::Valid);
  }

  async fn validate_packs(&self, scope: &mut PackScope) -> Result<ValidateResult> {
    let (_, pack_list) = get_indexed_packs(scope)?;

    let tasks = pack_list.iter().map(|(pack_meta, pack)| {
      let strategy = self.clone();
      let path = pack.path.to_owned();
      let hash = pack_meta.hash.to_owned();
      let keys = pack.keys.expect_value().to_owned();
      tokio::spawn(async move {
        match strategy
          .get_pack_hash(&path, &keys, &Default::default())
          .await
        {
          Ok(res) => hash == res,
          Err(_) => false,
        }
      })
      .map_err(|e| error!("{}", e))
    });

    let validate_results = join_all(tasks)
      .await
      .into_iter()
      .collect::<Result<Vec<_>>>()?;

    let mut invalid_packs = validate_results
      .iter()
      .zip(pack_list.into_iter())
      .filter(|(is_valid, _)| !*is_valid)
      .collect::<Vec<_>>();
    invalid_packs.sort_by(|a, b| a.1 .1.path.cmp(&b.1 .1.path));
    if invalid_packs.is_empty() {
      return Ok(ValidateResult::Valid);
    } else {
      let invalid_pack_paths = invalid_packs
        .iter()
        .map(|(_, (_, pack))| pack.path.to_string_lossy().to_string())
        .collect_vec();
      Ok(ValidateResult::invalid_with_packs(
        "some packs is modified",
        invalid_pack_paths,
      ))
    }
  }
}

#[cfg(test)]
mod tests {
  use std::{path::PathBuf, sync::Arc};

  use rspack_error::Result;
  use rustc_hash::FxHashSet as HashSet;

  use crate::{
    pack::{
      strategy::split::test::test_pack_utils::{
        flush_file_mtime, mock_meta_file, mock_updates, UpdateVal,
      },
      PackScope, ScopeMeta, ScopeReadStrategy, ScopeValidateStrategy, ScopeWriteStrategy,
      SplitPackStrategy, ValidateResult,
    },
    PackFs, PackMemoryFs, PackOptions,
  };

  async fn test_valid_meta(scope_path: PathBuf, strategy: &SplitPackStrategy) -> Result<()> {
    let same_options = Arc::new(PackOptions {
      buckets: 10,
      max_pack_size: 100,
      expires: 100000,
    });
    let mut scope = PackScope::new(scope_path, same_options);
    strategy.ensure_meta(&mut scope).await?;
    let validated = strategy.validate_meta(&mut scope).await?;
    assert!(validated.is_valid());

    Ok(())
  }

  async fn test_invalid_option_changed(
    scope_path: PathBuf,
    strategy: &SplitPackStrategy,
  ) -> Result<()> {
    let bucket_changed_options = Arc::new(PackOptions {
      buckets: 1,
      max_pack_size: 100,
      expires: 100000,
    });
    let mut scope = PackScope::new(scope_path.clone(), bucket_changed_options.clone());
    strategy.ensure_meta(&mut scope).await?;
    let validated: ValidateResult = strategy.validate_meta(&mut scope).await?;
    assert_eq!(
      validated.to_string(),
      "validation failed due to `options.buckets` changed"
    );

    let max_size_changed_options = Arc::new(PackOptions {
      buckets: 10,
      max_pack_size: 99,
      expires: 100000,
    });
    let mut scope = PackScope::new(scope_path.clone(), max_size_changed_options.clone());
    strategy.ensure_meta(&mut scope).await?;
    let validated: ValidateResult = strategy.validate_meta(&mut scope).await?;
    assert_eq!(
      validated.to_string(),
      "validation failed due to `options.maxPackSize` changed"
    );

    Ok(())
  }

  async fn test_invalid_expired(scope_path: PathBuf, strategy: &SplitPackStrategy) -> Result<()> {
    let expired_options = Arc::new(PackOptions {
      buckets: 10,
      max_pack_size: 100,
      expires: 0,
    });
    let mut scope = PackScope::new(scope_path.clone(), expired_options.clone());
    strategy.ensure_meta(&mut scope).await?;
    let validated: ValidateResult = strategy.validate_meta(&mut scope).await?;
    assert_eq!(validated.to_string(), "validation failed due to expired");

    Ok(())
  }

  async fn test_valid_packs(
    scope_path: PathBuf,
    strategy: &SplitPackStrategy,
    options: Arc<PackOptions>,
  ) -> Result<()> {
    let mut scope = PackScope::new(scope_path, options);
    strategy.ensure_keys(&mut scope).await?;
    let validated = strategy.validate_packs(&mut scope).await?;
    assert!(validated.is_valid());

    Ok(())
  }

  async fn test_invalid_packs_changed(
    scope_path: PathBuf,
    strategy: &SplitPackStrategy,
    fs: Arc<dyn PackFs>,
    options: Arc<PackOptions>,
    files: HashSet<PathBuf>,
  ) -> Result<()> {
    let mut scope = PackScope::new(scope_path, options);
    for file in files {
      if !file.to_string_lossy().to_string().contains("cache_meta") {
        flush_file_mtime(&file, fs.clone()).await?;
      }
    }

    strategy.ensure_keys(&mut scope).await?;
    let validated = strategy.validate_packs(&mut scope).await?;
    assert!(validated
      .to_string()
      .starts_with("validation failed due to some packs is modified:"));
    assert_eq!(validated.to_string().split("\n").count(), 7);

    Ok(())
  }

  #[tokio::test]
  async fn should_validate_scope_meta() {
    let fs = Arc::new(PackMemoryFs::default());
    fs.remove_dir(&PathBuf::from("/cache/test_meta_valid"))
      .await
      .expect("should clean dir");
    let strategy = SplitPackStrategy::new(
      PathBuf::from("/cache/test_meta_valid"),
      PathBuf::from("/temp/test_meta_valid"),
      fs.clone(),
    );
    let scope_path = PathBuf::from("/cache/test_meta_valid/validate_meta");
    let pack_options = Arc::new(PackOptions {
      buckets: 10,
      max_pack_size: 100,
      expires: 100000,
    });
    mock_meta_file(
      &ScopeMeta::get_path(&scope_path),
      fs.clone(),
      pack_options.as_ref(),
      100,
    )
    .await
    .expect("should mock meta file");

    let _ = test_valid_meta(scope_path.clone(), &strategy)
      .await
      .map_err(|e| panic!("{}", e));

    let _ = test_invalid_option_changed(scope_path.clone(), &strategy)
      .await
      .map_err(|e| panic!("{}", e));

    let _ = test_invalid_expired(scope_path.clone(), &strategy)
      .await
      .map_err(|e| panic!("{}", e));
  }

  #[tokio::test]
  async fn should_validate_scope_packs() {
    let fs = Arc::new(PackMemoryFs::default());
    fs.remove_dir(&PathBuf::from("/cache/test_packs_valid"))
      .await
      .expect("should clean dir");
    let strategy = SplitPackStrategy::new(
      PathBuf::from("/cache/test_packs_valid"),
      PathBuf::from("/temp/test_packs_valid"),
      fs.clone(),
    );
    let scope_path = PathBuf::from("/cache/test_packs_valid/validate_packs");
    let pack_options = Arc::new(PackOptions {
      buckets: 10,
      max_pack_size: 100,
      expires: 100000,
    });
    let mut mock_scope = PackScope::empty(scope_path.clone(), pack_options.clone());
    let updates = mock_updates(0, 100, 30, UpdateVal::Value("val".to_string()));
    strategy
      .update_scope(&mut mock_scope, updates)
      .await
      .expect("should update scope");
    let files = strategy
      .write_scope(&mut mock_scope)
      .await
      .expect("should write scope");
    strategy
      .after_save(files.writed_files.clone(), files.removed_files)
      .await
      .expect("should move files");

    let _ = test_valid_packs(scope_path.clone(), &strategy, pack_options.clone())
      .await
      .map_err(|e| panic!("{}", e));

    let _ = test_invalid_packs_changed(
      scope_path.clone(),
      &strategy,
      fs.clone(),
      pack_options.clone(),
      files.writed_files,
    )
    .await
    .map_err(|e| panic!("{}", e));
  }
}
