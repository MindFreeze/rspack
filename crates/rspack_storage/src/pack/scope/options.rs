use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct PackOptions {
  pub buckets: usize,
  pub max_pack_size: usize,
  pub expires: u64,
}

impl PackOptions {
  pub fn is_expired(&self, last_modified: &u64) -> bool {
    let current_time = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .expect("get current time failed")
      .as_millis() as u64;
    current_time - last_modified > self.expires
  }
}
