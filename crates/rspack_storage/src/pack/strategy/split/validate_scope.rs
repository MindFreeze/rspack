use async_trait::async_trait;
use futures::{future::join_all, TryFutureExt};
use rspack_error::{error, Result};

use super::{util::get_indexed_packs, SplitPackStrategy};
use crate::pack::{PackScope, ScopeValidateStrategy, ValidateResult};

#[async_trait]
impl ScopeValidateStrategy for SplitPackStrategy {
  async fn validate_meta(&self, scope: &mut PackScope) -> Result<ValidateResult> {
    let meta = scope.meta.expect_value();
    if meta.buckets != scope.options.buckets || meta.max_pack_size != scope.options.max_pack_size {
      return Ok(ValidateResult::Invalid("scope options changed".to_string()));
    }

    if scope.options.is_expired(&meta.last_modified) {
      return Ok(ValidateResult::Invalid("scope expired".to_string()));
    }

    return Ok(ValidateResult::Valid);
  }

  async fn validate_packs(&self, scope: &mut PackScope) -> Result<ValidateResult> {
    let (_, pack_list) = get_indexed_packs(scope)?;

    let tasks = pack_list.into_iter().map(|(pack_meta, pack)| {
      let strategy = self.clone();
      let path = pack.path.to_owned();
      let hash = pack_meta.hash.to_owned();
      let keys = pack.keys.expect_value().to_owned();
      let contents = pack.contents.expect_value().to_owned();
      tokio::spawn(async move {
        match strategy.get_pack_hash(&path, &keys, &contents).await {
          Ok(res) => hash == res,
          Err(_) => false,
        }
      })
      .map_err(|e| error!("{}", e))
    });

    let validate_results = join_all(tasks)
      .await
      .into_iter()
      .collect::<Result<Vec<bool>>>()?;
    if validate_results.into_iter().all(|v| v) {
      Ok(ValidateResult::Valid)
    } else {
      // TODO: pack invalid detail
      Ok(ValidateResult::Invalid("packs is not validate".to_string()))
    }
  }
}
