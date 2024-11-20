mod cacheable_context;
mod occasion;
pub mod snapshot;
pub mod storage;
use std::sync::Arc;

use cacheable_context::CacheableContext;
pub use cacheable_context::FromContext;
use occasion::MakeOccasion;
use rspack_fs::FileSystem;
use rspack_paths::{AssertUtf8, Utf8PathBuf};
use rustc_hash::FxHashSet as HashSet;

use self::{
  snapshot::{Snapshot, SnapshotOptions},
  storage::{MemoryStorage, Storage, StorageOptions},
};
use super::Cache;
use crate::{make::MakeArtifact, Compilation, CompilerOptions};

#[derive(Debug, Clone)]
pub struct PersistentCacheOptions {
  pub snapshot: SnapshotOptions,
  pub storage: Vec<StorageOptions>,
}

/// Persistent cache implementation
#[derive(Debug)]
pub struct PersistentCache {
  storage: Arc<dyn Storage>,
  snapshot: Snapshot,
  make_occasion: MakeOccasion,
}

impl PersistentCache {
  pub fn new(
    option: &PersistentCacheOptions,
    fs: Arc<dyn FileSystem>,
    compiler_options: Arc<CompilerOptions>,
  ) -> Self {
    let storage = Arc::new(MemoryStorage::default());
    let context = Arc::new(CacheableContext {
      options: compiler_options,
    });
    let make_occasion = MakeOccasion::new(storage.clone(), context);
    Self {
      snapshot: Snapshot::new(option.snapshot.clone(), fs, storage.clone()),
      storage,
      make_occasion,
    }
  }
}

impl Cache for PersistentCache {
  fn before_compile(&self, compilation: &mut Compilation) {
    if compilation.modified_files.is_empty() && compilation.removed_files.is_empty() {
      // inject modified_files and removed_files
      let (modified_paths, removed_paths) = self.snapshot.calc_modified_paths();
      compilation.modified_files = modified_paths
        .into_iter()
        .map(|item| item.into_std_path_buf())
        .collect();
      compilation.removed_files = removed_paths
        .into_iter()
        .map(|item| item.into_std_path_buf())
        .collect();
    }
  }

  fn after_compile(&self, compilation: &Compilation) {
    // TODO add a all_dependencies to collect dependencies
    let (_, file_added, file_removed) = compilation.file_dependencies();
    let (_, context_added, context_removed) = compilation.context_dependencies();
    let (_, missing_added, missing_removed) = compilation.missing_dependencies();
    let (_, build_added, build_removed) = compilation.build_dependencies();
    let modified_paths: HashSet<Utf8PathBuf> = compilation
      .modified_files
      .iter()
      .chain(file_added)
      .chain(context_added)
      .chain(missing_added)
      .chain(build_added)
      // TODO remove clone after replace all of PathBuf to Utf8PathBuf
      .map(|item| item.clone().assert_utf8())
      .collect();
    let removed_paths: HashSet<Utf8PathBuf> = compilation
      .removed_files
      .iter()
      .chain(file_removed)
      .chain(context_removed)
      .chain(missing_removed)
      .chain(build_removed)
      .map(|item| item.clone().assert_utf8())
      .collect();
    self.snapshot.remove(removed_paths.iter());
    self.snapshot.add(modified_paths.iter());

    self.storage.idle();
  }

  fn before_make(&self, make_artifact: &mut MakeArtifact) {
    //        self.make_occasion.
  }

  fn after_make(&self, make_artifact: &MakeArtifact) {
    self.make_occasion.save(make_artifact);
  }
}
