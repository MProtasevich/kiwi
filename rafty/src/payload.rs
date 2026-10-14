use chrono::{DateTime, Utc};
use std::fmt::Debug;
use std::sync::Arc;

pub type ValueId = Arc<str>;

pub trait Key: Debug + Send + Sync {
    fn key(&self) -> ValueId;
}

#[derive(Debug, Clone)]
pub enum Payload {
    Integer(i64),
    Float(f64),
    String(String),
    DateTime(DateTime<Utc>),
    Object, // reserved for future implementation
}

#[derive(Debug, Default)]
pub(crate) struct Value(u64);

impl Key for Value {
    fn key(&self) -> ValueId {
        Arc::from(self.0.to_string())
    }
}

#[derive(Debug)]
pub enum Command {
    Set(Payload), // fixme wrapping with Arc may have sense
    Get(ValueId),
    Update, // reserved for future implementation
    Delete(ValueId),
}

impl Command {

    pub fn get_type(&self) -> CommandType {
        match self {
            Command::Set(_) | Command::Delete(_) => CommandType::Write,
            Command::Get(_) => CommandType::Read,
            Command::Update => CommandType::Modify,
        }
    }

}

impl Clone for Command {
    fn clone(&self) -> Self {
        match self {
            Command::Set(payload) => Command::Set(payload.clone()),
            Command::Get(key) => Command::Get(Arc::clone(key)),
            Command::Delete(key) => Command::Delete(Arc::clone(key)),
            Command::Update => Command::Update,
        }
    }
}

#[derive(Debug)]
pub enum CommandType {
    Read,
    Write,
    Modify,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PersistenceState {
    Committed,
    Uncommitted,
}

#[derive(Debug)]
pub struct Log {
    pub command: Command,
    pub state: PersistenceState,
}
