use crate::payload::Value;
use crate::transport::model::Key;
use anyhow::Result;
use std::fmt::Debug;

pub trait Storage: Debug + Send + Sync {

    fn push(&self, key: Key, payload: Value) -> Result<()>;

}

#[derive(Debug)]
pub struct InMemoryStorage;

impl Storage for InMemoryStorage {
    fn push(&self, _key: Key, _payload: Value) -> Result<()> {
        todo!()
    }
}
