use crate::payload::{Key, Value};
use anyhow::Result;
use std::fmt::Debug;

pub trait Storage: Debug + Send + Sync {

    fn push(&self, key: Key, payload: Value) -> Result<()>;

}

#[derive(Debug)]
pub struct InMemoryStorage;

impl Storage for InMemoryStorage {
    fn push(&self, key: Key, payload: Value) -> Result<()> {
        todo!()
    }
}
