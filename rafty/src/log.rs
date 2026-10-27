use crate::payload::Value;
use crate::transport::model::Key;
use crate::transport::model::command::Command;
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::fmt::Debug;

pub trait CommandLog: Debug + Send + Sync {

    fn push(&self, cmd: Command) -> Result<()>;

    fn push_all(&self, cmds: Vec<Command>) -> Result<()>;

    fn commit(&self, key: Key) -> Result<Value>;

    fn get_commands(&self, since: DateTime<Utc>) -> Result<Vec<Command>>;

    fn get_last_change(&self) -> Option<DateTime<Utc>>;

}

#[derive(Debug, Default)]
pub struct InMemoryCommandLog;

impl CommandLog for InMemoryCommandLog {

    fn push(&self, _cmd: Command) -> Result<()> {
        todo!()
    }

    fn push_all(&self, _cmds: Vec<Command>) -> Result<()> {
        todo!()
    }

    fn commit(&self, _key: Key) -> Result<Value> {
        todo!()
    }

    fn get_commands(&self, _since: DateTime<Utc>) -> Result<Vec<Command>> {
        todo!()
    }

    fn get_last_change(&self) -> Option<DateTime<Utc>> {
        todo!()
    }
}
