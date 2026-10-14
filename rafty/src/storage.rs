use std::fmt::Debug;
use anyhow::Result;
use crate::payload::{Command, Payload, ValueId};

pub trait Storage: Debug + Send + Sync {

    fn push(&self, key: ValueId, payload: Payload) -> Result<()>;

}

#[derive(Debug)]
pub struct InMemoryStorage;

impl Storage for InMemoryStorage {
    fn push(&self, key: ValueId, payload: Payload) -> Result<()> {
        todo!()
    }
}
