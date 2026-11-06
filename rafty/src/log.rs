use crate::payload::Value;
use crate::transport::model::Key;
use crate::transport::model::command::{Command, TimeAware};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use std::fmt::Debug;
use std::sync::Mutex;

pub trait CommandLog: Debug + Send + Sync {

    fn push(&self, cmd: Command) -> Result<()>;

    fn push_all(&self, cmds: Vec<Command>) -> Result<()>;

    fn commit(&self, key: Key) -> Result<Value>;

    fn get_commands(&self, since: DateTime<Utc>) -> Result<Vec<Command>>;

    fn get_last_change(&self) -> Option<DateTime<Utc>>;

}

/// A reference implementation of [`CommandLog`] that keeps every command in memory.
///
/// Commands are ordered by their embedded timestamp, which is also the cursor used by
/// [`CommandLog::get_commands`]. Commands without a timestamp (e.g. `Get`) are stored at
/// the current wall-clock time; they are retained so that replication can replay them.
#[derive(Debug, Default)]
pub struct InMemoryCommandLog {
    entries: Mutex<Vec<(DateTime<Utc>, Command)>>,
}

impl InMemoryCommandLog {
    #[inline]
    pub fn len(&self) -> usize {
        self.entries.lock().expect("command log poisoned").len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl CommandLog for InMemoryCommandLog {

    fn push(&self, cmd: Command) -> Result<()> {
        let timestamp = cmd.timestamp().unwrap_or_else(Utc::now);
        let mut entries = self.entries.lock().map_err(|_| anyhow!("command log poisoned"))?;
        entries.push((timestamp, cmd));
        // Stable sort keeps insertion order for commands sharing a timestamp.
        entries.sort_by_key(|(timestamp, _)| *timestamp);
        Ok(())
    }

    fn push_all(&self, cmds: Vec<Command>) -> Result<()> {
        for cmd in cmds {
            self.push(cmd)?;
        }
        Ok(())
    }

    fn commit(&self, key: Key) -> Result<Value> {
        let entries = self.entries.lock().map_err(|_| anyhow!("command log poisoned"))?;
        entries
            .iter()
            .rev()
            .find_map(|(_, cmd)| match cmd {
                Command::Set(stored_key, payload, _) if *stored_key == key => Some(Value::from(payload)),
                _ => None,
            })
            .ok_or_else(|| anyhow!("No value has been logged for key [{key:?}]"))
    }

    fn get_commands(&self, since: DateTime<Utc>) -> Result<Vec<Command>> {
        let entries = self.entries.lock().map_err(|_| anyhow!("command log poisoned"))?;
        Ok(entries
            .iter()
            .filter(|(timestamp, _)| *timestamp > since)
            .map(|(_, cmd)| cmd.clone())
            .collect())
    }

    fn get_last_change(&self) -> Option<DateTime<Utc>> {
        self.entries
            .lock()
            .expect("command log poisoned")
            .last()
            .map(|(timestamp, _)| *timestamp)
    }
}
