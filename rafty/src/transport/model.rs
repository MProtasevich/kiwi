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
        pub last_change: DateTime<Utc>,
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

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum Command {
        Set(Key, Payload), // fixme wrapping with Arc may have sense
        Commit(Key),
        Get(Key),
        Update, // reserved for future implementation
        Delete(Key),
    }

    impl Command {
        pub fn get_type(&self) -> CommandType {
            match self {
                Command::Set(_, _) | Command::Delete(_) => CommandType::Write,
                Command::Get(_) | Command::Commit(_) => CommandType::Read,
                Command::Update => CommandType::Modify,
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
    use crate::node_state::NodeId;
    use crate::transport::model::command::Command;
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    pub struct LogReplicationRequest {
        pub node_id: NodeId,
        pub from: DateTime<Utc>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct LogReplicationResponse {
        pub node_id: NodeId,
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
