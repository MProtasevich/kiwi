use crate::node_state::NodeId;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::sync::Arc;

pub trait NodeAware {
    fn node_id(&self) -> &NodeId;
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Key(Arc<str>);

impl Key {
    pub fn new(s: &str) -> Self {
        Key(Arc::from(s))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub node_id: NodeId,
    pub term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
            metadata: Metadata { node_id, term },
            payload,
        }
    }
}

pub mod common {
    use crate::transport::model::Key;
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct HeartbeatRequest {
        pub timestamp: DateTime<Utc>,
        pub last_change: DateTime<Utc>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct HeartbeatResponse {
        pub timestamp: DateTime<Utc>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct CommitRequest {
        pub key: Key,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct CommitResponse {
        pub key: Key,
    }
}

pub mod command {
    use crate::transport::model::Key;
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    pub trait TimeAware {
        fn timestamp(&self) -> Option<DateTime<Utc>>;
    }

    #[derive(Debug)]
    pub enum CommandType {
        Read,
        Write,
        Modify,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum Command {
        Set(Key, Payload, DateTime<Utc>), // TODO wrapping with Arc may have sense
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
            use self::Command::*;
            match self {
                Set(_, _, timestamp)
                | Update(timestamp)
                | Commit(_, timestamp)
                | Delete(_, timestamp) => Some(*timestamp),
                Get(_) => None,
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
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

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct LogReplicationRequest {
        pub cursor_from: CursorId,
        pub chunk_size: usize,
    }

    pub type CursorId = DateTime<Utc>;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct LogReplicationResponse {
        pub cursor_id: CursorId,
        pub commands: Vec<Command>,
    }
}

pub mod vote {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct VoteRequest;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct VoteResponse {
        pub voted: bool,
    }
}

/// Messages used to discover the cluster and to replicate its configuration.
///
/// The distinction matters: a [`JoinRequest`] is an *edge* operation (a node that is
/// not a member yet asks an existing one for admission), while a
/// [`ConfigurationBroadcast`] is a *Raft* operation (a committed configuration entry is
/// replicated to every member).
pub mod membership {
    use crate::cluster::{ClusterConfiguration, ClusterId, ConfigurationEntry, NodeAddress};
    use crate::node_state::NodeId;
    use serde::{Deserialize, Serialize};

    /// Sent by a node that wants to become a member.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct JoinRequest {
        /// Must match the receiver's cluster, otherwise the node is rejected.
        pub cluster_id: ClusterId,
        pub node_id: NodeId,
        /// Address the joining node advertises to the rest of the cluster.
        pub address: NodeAddress,
    }

    impl JoinRequest {
        pub fn new(cluster_id: ClusterId, node_id: NodeId, address: NodeAddress) -> Self {
            Self {
                cluster_id,
                node_id,
                address,
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub enum JoinResponse {
        /// The leader admitted the node as a learner (or it was already a member).
        Accepted {
            leader_id: NodeId,
            configuration: ClusterConfiguration,
            /// `false` while the node is still catching up as a learner.
            voter: bool,
        },
        /// This node is not the leader and points at the one that is.
        Redirect {
            leader_id: NodeId,
            address: NodeAddress,
        },
        Rejected {
            reason: String,
        },
    }

    /// A committed configuration entry plus the full configuration it produces.
    ///
    /// Followers validate the entry against their current configuration and then
    /// install the snapshot, which makes the update idempotent under retries.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ConfigurationBroadcast {
        pub entry: ConfigurationEntry,
        pub configuration: ClusterConfiguration,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ConfigurationAck {
        pub accepted: bool,
        pub applied_index: u64,
        pub reason: Option<String>,
    }

    impl ConfigurationAck {
        pub fn accepted(applied_index: u64) -> Self {
            Self {
                accepted: true,
                applied_index,
                reason: None,
            }
        }

        pub fn rejected(applied_index: u64, reason: impl Into<String>) -> Self {
            Self {
                accepted: false,
                applied_index,
                reason: Some(reason.into()),
            }
        }
    }
}
