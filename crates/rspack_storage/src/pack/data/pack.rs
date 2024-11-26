use rspack_paths::Utf8PathBuf;

pub type PackKeys = Vec<Vec<u8>>;
pub type PackContents = Vec<Vec<u8>>;

#[derive(Debug, Default, Clone)]
pub enum PackKeysState {
  #[default]
  Pending,
  Value(PackKeys),
}

impl PackKeysState {
  pub fn expect_value(&self) -> &PackKeys {
    match self {
      PackKeysState::Value(v) => v,
      PackKeysState::Pending => panic!("pack key is not ready"),
    }
  }
}

#[derive(Debug, Default, Clone)]
pub enum PackContentsState {
  #[default]
  Pending,
  Value(PackContents),
}

impl PackContentsState {
  pub fn expect_value(&self) -> &PackContents {
    match self {
      PackContentsState::Value(v) => v,
      PackContentsState::Pending => panic!("pack content is not ready"),
    }
  }
}

#[derive(Debug, Clone)]
pub struct Pack {
  pub path: Utf8PathBuf,
  pub keys: PackKeysState,
  pub contents: PackContentsState,
}

impl Pack {
  pub fn new(path: Utf8PathBuf) -> Self {
    Self {
      path,
      keys: Default::default(),
      contents: Default::default(),
    }
  }

  pub fn loaded(&self) -> bool {
    matches!(self.keys, PackKeysState::Value(_))
      && matches!(self.contents, PackContentsState::Value(_))
  }

  pub fn size(&self) -> usize {
    self
      .keys
      .expect_value()
      .iter()
      .chain(self.contents.expect_value().iter())
      .fold(0_usize, |acc, item| acc + item.len())
  }
}
