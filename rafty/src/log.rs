use std::fmt::Debug;
use crate::payload::{Command, Payload, ValueId};
use anyhow::Result;
use chrono::{DateTime, Utc};

pub trait CommandLog: Debug + Send + Sync {

    fn push(&self, key: ValueId, cmd: Command) -> Result<()>;
    fn commit(&self, key: ValueId) -> Result<Payload>;
    fn get_commands(&self, since: DateTime<Utc>) -> Result<Vec<Command>>;

}

#[derive(Debug, Default)]
pub struct InMemoryCommandLog;

impl CommandLog for InMemoryCommandLog {

    fn push(&self, key: ValueId, cmd: Command) -> Result<()> {
        todo!()
    }

    fn commit(&self, key: ValueId) -> Result<Payload> {
        todo!()
    }

    fn get_commands(&self, since: DateTime<Utc>) -> Result<Vec<Command>> {
        todo!()
    }

}
