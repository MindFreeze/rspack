use rspack_cacheable::{
  cacheable,
  with::{AsOption, AsTuple2, AsVec, Inline},
};

use crate::{
  AsyncDependenciesBlock, AsyncDependenciesBlockIdentifier, BoxDependency, BoxModule,
  ModuleGraphConnection, ModuleGraphModule,
};

#[cacheable]
pub struct Node {
  pub mgm: ModuleGraphModule,
  pub module: BoxModule,
  pub dependencies: Vec<(BoxDependency, Option<AsyncDependenciesBlockIdentifier>)>,
  pub connections: Vec<ModuleGraphConnection>,
  pub blocks: Vec<AsyncDependenciesBlock>,
}

#[cacheable(as=Node)]
pub struct NodeRef<'a> {
  #[cacheable(with=Inline)]
  pub mgm: &'a ModuleGraphModule,
  #[cacheable(with=Inline)]
  pub module: &'a BoxModule,
  #[cacheable(with=AsVec<AsTuple2<Inline, AsOption<Inline>>>)]
  pub dependencies: Vec<(
    &'a BoxDependency,
    Option<&'a AsyncDependenciesBlockIdentifier>,
  )>,
  #[cacheable(with=AsVec<Inline>)]
  pub connections: Vec<&'a ModuleGraphConnection>,
  #[cacheable(with=AsVec<Inline>)]
  pub blocks: Vec<&'a AsyncDependenciesBlock>,
}
