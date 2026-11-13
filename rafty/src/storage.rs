use crate::transport::model::Key;
use crate::transport::model::command::Payload;
use anyhow::Result;
use std::fmt::Debug;

pub trait Storage: Debug + Send + Sync {
    fn push(&self, key: Key, payload: Payload) -> Result<()>;

    fn get(&self, key: Key) -> Result<Option<Payload>>;
}

#[derive(Debug, Default)]
pub struct InMemoryStorage {}

impl Storage for InMemoryStorage {
    fn push(&self, key: Key, payload: Payload) -> Result<()> {
        todo!()
    }

    fn get(&self, key: Key) -> Result<Option<Payload>> {
        todo!()
    }
}
