use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;

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
pub enum StorageCommand {
    Set(Key, Value), // fixme wrapping with Arc may have sense
    Get(Key),
    Commit(Key),
    Update, // reserved for future implementation
    Delete(Key),
}

impl StorageCommand {

    pub fn get_type(&self) -> CommandType {
        match self {
            StorageCommand::Set(_, _) | StorageCommand::Delete(_) => CommandType::Write,
            StorageCommand::Get(_) | StorageCommand::Commit(_) => CommandType::Read,
            StorageCommand::Update => CommandType::Modify,
        }
    }

}

impl Clone for StorageCommand {
    fn clone(&self) -> Self {
        match self {
            StorageCommand::Set(key, payload) => StorageCommand::Set(key.clone(), payload.clone()),
            StorageCommand::Get(key) => StorageCommand::Get(key.clone()),
            StorageCommand::Commit(key) => StorageCommand::Commit(key.clone()),
            StorageCommand::Delete(key) => StorageCommand::Delete(key.clone()),
            StorageCommand::Update => StorageCommand::Update,
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
