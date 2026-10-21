use crate::payload::{Key, StorageCommand, Value};
use crate::transport::model::command::Command;
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::fmt::Debug;
use std::sync::Arc;

pub trait CommandLog: Debug + Send + Sync {

    fn push(&self, cmd: Arc<Command>) -> Result<()>;

    fn commit(&self, key: Key) -> Result<Value>;

    fn get_commands(&self, since: DateTime<Utc>) -> Result<Vec<StorageCommand>>;

}

#[derive(Debug, Default)]
pub struct InMemoryCommandLog;

impl CommandLog for InMemoryCommandLog {

    fn push(&self, cmd: Arc<Command>) -> Result<()> {
        todo!()
    }

    fn commit(&self, key: Key) -> Result<Value> {
        todo!()
    }

    fn get_commands(&self, since: DateTime<Utc>) -> Result<Vec<StorageCommand>> {
        todo!()
    }

}
