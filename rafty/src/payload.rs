use chrono::{DateTime, Utc};
use std::fmt::Debug;
use std::sync::Arc;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Key(Arc<str>);

impl Key {
    pub fn new(s: &str) -> Self {
        Key(Arc::from(s))
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Integer(i64),
    Float(f64),
    String(String),
    DateTime(DateTime<Utc>),
    Object, // reserved for future implementation
}

#[derive(Debug)]
pub enum Command {
    Set(Value), // fixme wrapping with Arc may have sense
    Get(Key),
    Update, // reserved for future implementation
    Delete(Key),
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
            Command::Get(key) => Command::Get(key.clone()),
            Command::Delete(key) => Command::Delete(key.clone()),
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
