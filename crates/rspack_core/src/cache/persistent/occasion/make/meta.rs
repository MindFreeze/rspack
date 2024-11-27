use rspack_cacheable::{cacheable, with::Inline};
use rspack_collections::IdentifierSet;
use rustc_hash::FxHashSet as HashSet;

use crate::{BuildDependency, FileCounter};

#[cacheable]
pub struct Meta {
  pub make_failed_dependencies: HashSet<BuildDependency>,
  pub make_failed_module: IdentifierSet,
  pub file_dependencies: FileCounter,
  pub context_dependencies: FileCounter,
  pub missing_dependencies: FileCounter,
  pub build_dependencies: FileCounter,
  pub next_dependencies_id: u32,
}

#[cacheable(as=Meta)]
pub struct MetaRef<'a> {
  #[cacheable(with=Inline)]
  pub make_failed_dependencies: &'a HashSet<BuildDependency>,
  #[cacheable(with=Inline)]
  pub make_failed_module: &'a IdentifierSet,
  #[cacheable(with=Inline)]
  pub file_dependencies: &'a FileCounter,
  #[cacheable(with=Inline)]
  pub context_dependencies: &'a FileCounter,
  #[cacheable(with=Inline)]
  pub missing_dependencies: &'a FileCounter,
  #[cacheable(with=Inline)]
  pub build_dependencies: &'a FileCounter,
  pub next_dependencies_id: u32,
}
