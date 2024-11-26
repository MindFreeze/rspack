mod queue;

use std::sync::Arc;

use futures::future::join_all;
use itertools::Itertools;
use queue::TaskQueue;
use rspack_error::{error, Error, Result};
use rustc_hash::FxHashMap as HashMap;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{oneshot, Mutex};

use crate::pack::{PackScope, ScopeStrategy, ValidateResult, WriteScopeResult};
use crate::StorageContent;
use crate::{pack::ScopeUpdates, PackOptions};

type ScopeMap = HashMap<&'static str, PackScope>;

#[derive(Debug)]
pub struct ScopeManager {
  pub strategy: Arc<dyn ScopeStrategy>,
  pub options: Arc<PackOptions>,
  pub scopes: Arc<Mutex<ScopeMap>>,
  pub queue: TaskQueue,
}

impl ScopeManager {
  pub fn new(options: PackOptions, strategy: Arc<dyn ScopeStrategy>) -> Self {
    ScopeManager {
      strategy,
      options: Arc::new(options),
      scopes: Default::default(),
      queue: TaskQueue::new(),
    }
  }
  pub fn save(&self, updates: ScopeUpdates) -> Receiver<Result<()>> {
    let scopes = self.scopes.clone();
    let options = self.options.clone();
    let strategy = self.strategy.clone();

    let (tx, rx) = oneshot::channel();
    self.queue.add_task(Box::pin(async move {
      let old_scopes = std::mem::take(&mut *scopes.lock().await);
      let _ = match save_scopes(old_scopes, updates, options, strategy.as_ref()).await {
        Ok(new_scopes) => {
          let _ = std::mem::replace(&mut *scopes.lock().await, new_scopes);
          tx.send(Ok(()))
        }
        Err(e) => tx.send(Err(e)),
      };
    }));

    rx
  }

  pub async fn get_all(&self, name: &'static str) -> Result<StorageContent> {
    let mut scopes = self.scopes.lock().await;
    let scope = scopes
      .entry(name)
      .or_insert_with(|| PackScope::new(self.strategy.get_path(name), self.options.clone()));

    match validate_scope(scope, self.strategy.as_ref()).await {
      Ok(res) => {
        if res.is_valid() {
          self.strategy.ensure_contents(scope).await?;
          Ok(scope.get_contents())
          // Ok(vec![])
        } else {
          scope.clear();
          Err(error!(res.to_string()))
        }
      }
      Err(e) => {
        scope.clear();
        Err(Error::from(e))
      }
    }
  }
}

async fn validate_scope(
  scope: &mut PackScope,
  strategy: &dyn ScopeStrategy,
) -> Result<ValidateResult> {
  strategy.ensure_meta(scope).await?;
  let is_meta_valid = strategy.validate_meta(scope).await?;

  if is_meta_valid.is_valid() {
    strategy.ensure_keys(scope).await?;
    strategy.validate_packs(scope).await
  } else {
    Ok(is_meta_valid)
  }
}

async fn save_scopes(
  mut scopes: ScopeMap,
  mut updates: ScopeUpdates,
  options: Arc<PackOptions>,
  strategy: &dyn ScopeStrategy,
) -> Result<ScopeMap> {
  strategy.before_save().await?;

  for (scope_name, _) in updates.iter() {
    scopes
      .entry(scope_name)
      .or_insert_with(|| PackScope::empty(strategy.get_path(scope_name), options.clone()));
  }

  let update_tasks = join_all(
    scopes
      .iter_mut()
      .map(|(name, scope)| (scope, updates.remove(name).unwrap_or_default()))
      .collect_vec()
      .into_iter()
      .map(|(scope, updates)| strategy.update_scope(scope, updates)),
  );

  update_tasks.await.into_iter().collect::<Result<()>>()?;

  let mut scopes = scopes.into_iter().collect_vec();
  let save_tasks = join_all(
    scopes
      .iter_mut()
      .map(|(_, scope)| async move { strategy.write_scope(scope).await })
      .collect_vec(),
  );
  let write_res = save_tasks
    .await
    .into_iter()
    .collect::<Result<Vec<WriteScopeResult>>>()?
    .into_iter()
    .fold(WriteScopeResult::default(), |mut acc, s| {
      acc.extend(s);
      acc
    });

  strategy
    .after_save(write_res.writed_files, write_res.removed_files)
    .await?;

  Ok(scopes.into_iter().collect())
}
