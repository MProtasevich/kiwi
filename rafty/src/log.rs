use std::fmt::Debug;
use crate::payload::{Command, Value, Key};
use anyhow::Result;
use chrono::{DateTime, Utc};

pub trait CommandLog: Debug + Send + Sync {

    fn push(&self, key: Key, cmd: Command) -> Result<()>;

    fn commit(&self, key: Key) -> Result<Value>;

    fn get_commands(&self, since: DateTime<Utc>) -> Result<Vec<Command>>;

}

#[derive(Debug, Default)]
pub struct InMemoryCommandLog;

impl CommandLog for InMemoryCommandLog {

    fn push(&self, key: Key, cmd: Command) -> Result<()> {
        todo!()
    }

    fn commit(&self, key: Key) -> Result<Value> {
        todo!()
    }

    fn get_commands(&self, since: DateTime<Utc>) -> Result<Vec<Command>> {
        todo!()
    }

}
