use std::{
  path::PathBuf,
  time::{SystemTime, UNIX_EPOCH},
};

use crate::PackOptions;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PackFileMeta {
  pub hash: String,
  pub name: String,
  pub size: usize,
  pub writed: bool,
}

#[derive(Debug, Default)]
pub struct ScopeMeta {
  pub path: PathBuf,
  pub buckets: usize,
  pub max_pack_size: usize,
  pub last_modified: u64,
  pub packs: Vec<Vec<PackFileMeta>>,
}
impl ScopeMeta {
  pub fn new(dir: &PathBuf, options: &PackOptions) -> Self {
    let mut packs = vec![];
    for _ in 0..options.buckets {
      packs.push(vec![]);
    }
    Self {
      path: Self::get_path(dir),
      buckets: options.buckets,
      max_pack_size: options.max_pack_size,
      last_modified: SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("should get current time")
        .as_millis() as u64,
      packs,
    }
  }

  pub fn get_path(dir: &PathBuf) -> PathBuf {
    dir.join("cache_meta")
  }
}
