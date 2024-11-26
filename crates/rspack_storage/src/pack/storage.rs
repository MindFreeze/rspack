use std::{
  path::PathBuf,
  sync::{Arc, Mutex},
};

use rspack_error::Result;
use rspack_paths::AssertUtf8;
use rustc_hash::FxHashMap as HashMap;
use tokio::sync::oneshot::Receiver;

use super::{PackFs, PackOptions, ScopeManager, ScopeStrategy, SplitPackStrategy};
use crate::Storage;

pub type ScopeUpdates = HashMap<&'static str, HashMap<Vec<u8>, Option<Vec<u8>>>>;
#[derive(Debug)]
pub struct PackStorage {
  manager: ScopeManager,
  updates: Mutex<ScopeUpdates>,
}

pub struct PackStorageOptions {
  root: PathBuf,
  temp_root: PathBuf,
  fs: Arc<dyn PackFs>,
  buckets: usize,
  max_pack_size: usize,
  expires: u64,
}

impl PackStorage {
  pub fn new(options: PackStorageOptions) -> Self {
    let strategy: Arc<dyn ScopeStrategy> = Arc::new(SplitPackStrategy::new(
      options.root.assert_utf8(),
      options.temp_root.assert_utf8(),
      options.fs,
    ));
    Self {
      manager: ScopeManager::new(
        PackOptions {
          buckets: options.buckets,
          max_pack_size: options.max_pack_size,
          expires: options.expires,
        },
        strategy,
      ),
      updates: Default::default(),
    }
  }
}

#[async_trait::async_trait]
impl Storage for PackStorage {
  async fn get_all(&self, name: &'static str) -> Result<Vec<(Arc<Vec<u8>>, Arc<Vec<u8>>)>> {
    self.manager.get_all(name).await
  }
  fn set(&self, scope: &'static str, key: Vec<u8>, value: Vec<u8>) {
    let mut updates = self.updates.lock().expect("should get lock");
    let scope_update = updates.entry(scope).or_default();
    scope_update.insert(key, Some(value));
  }
  fn remove(&self, scope: &'static str, key: &[u8]) {
    let mut updates = self.updates.lock().expect("should get lock");
    let scope_update = updates.entry(scope).or_default();
    scope_update.insert(key.to_vec(), None);
  }
  fn idle(&self) -> Receiver<Result<()>> {
    self.manager.save(std::mem::take(
      &mut *self.updates.lock().expect("should get lock"),
    ))
  }
}
