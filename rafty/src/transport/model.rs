use crate::node_state::NodeId;
use serde::{Deserialize, Serialize};
use std::ops::Deref;

pub trait NodeAware {
    fn node_id(&self) -> &NodeId;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub node_id: NodeId,
    pub term: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterMessage<T> {
    pub metadata: Metadata,
    pub payload: T,
}

impl<T> NodeAware for ClusterMessage<T> {
    #[inline(always)]
    fn node_id(&self) -> &NodeId {
        &self.metadata.node_id
    }
}

impl<T> Deref for ClusterMessage<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

impl<T> ClusterMessage<T> {
    pub fn new(node_id: NodeId, term: u64, payload: T) -> Self {
        Self {
            metadata: Metadata {
                node_id,
                term,
            },
            payload,
        }
    }
}

pub mod common {
    use crate::payload::Key;
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    pub struct HeartbeatRequest {
        pub timestamp: DateTime<Utc>,
        pub last_change: Option<DateTime<Utc>>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct HeartbeatResponse {
        pub timestamp: DateTime<Utc>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct CommitRequest {
        pub key: Key,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct CommitResponse {
        pub key: Key,
    }
}

pub mod command {
    use crate::payload::{CommandType, Key};
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    pub trait TimeAware {
        fn timestamp(&self) -> Option<DateTime<Utc>>;
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum Command {
        Set(Key, Payload, DateTime<Utc>), // fixme wrapping with Arc may have sense
        Commit(Key, DateTime<Utc>),
        Get(Key),
        Update(DateTime<Utc>), // reserved for future implementation
        Delete(Key, DateTime<Utc>),
    }

    impl Command {
        pub fn get_type(&self) -> CommandType {
            match self {
                Command::Set(_, _, _) | Command::Delete(_, _) => CommandType::Write,
                Command::Get(_) | Command::Commit(_, _) => CommandType::Read,
                Command::Update(_) => CommandType::Modify,
            }
        }
    }

    impl TimeAware for Command {
        fn timestamp(&self) -> Option<DateTime<Utc>> {
            match self {
                Command::Set(_, _, timestamp) => Some(timestamp.clone()),
                Command::Update(timestamp) => Some(timestamp.clone()),
                Command::Commit(_, timestamp) => Some(timestamp.clone()),
                Command::Delete(_, timestamp) => Some(timestamp.clone()),
                Command::Get(_) => None,
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub enum Payload {
        Integer(i64),
        Float(f64),
        String(Arc<str>),
        DateTime(DateTime<Utc>),
        Object, // reserved for future implementation
    }
}

pub mod log {
    use crate::transport::model::command::Command;
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    pub struct LogReplicationRequest {
        pub cursor_from: CursorId,
        pub chunk_size: usize,
    }

    pub type CursorId = DateTime<Utc>;

    #[derive(Debug, Serialize, Deserialize)]
    pub struct LogReplicationResponse {
        pub cursor_id: CursorId,
        pub commands: Vec<Command>,
    }
}

pub mod vote {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    pub struct VoteRequest;

    #[derive(Debug, Serialize, Deserialize)]
    pub struct VoteResponse {
        pub voted: bool,
    }

}
