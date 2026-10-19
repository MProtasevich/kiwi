use crate::transport::model::common::NodeResponse;

macro_rules! impl_node_response {
    ($($id_type:ty),* $(,)?) => {
        use crate::node_state::NodeId;
        $(
            impl NodeResponse for $id_type {
                fn node_id(&self) -> &NodeId {
                    &self.node_id
                }
            }
        )*
    };
}

impl_node_response!(
    common::HeartbeatResponse,
    common::CommitResponse,
    log::LogReplicationResponse,
    vote::VoteResponse,
);

pub mod common {
    use crate::node_state::NodeId;
    use crate::payload::Key;
    use chrono::{DateTime, Utc};

    pub trait NodeResponse {
        fn node_id(&self) -> &NodeId;
    }

    pub struct HeartbeatRequest {
        pub node_id: NodeId,
        pub term: u64,
        pub timestamp: DateTime<Utc>,
        pub last_change: DateTime<Utc>,
    }

    pub struct HeartbeatResponse {
        pub node_id: NodeId,
        pub term: u64,
        pub timestamp: DateTime<Utc>,
    }

    pub struct CommitRequest {
        pub node_id: NodeId,
        pub term: u64,
        pub key: Key,
    }

    pub struct CommitResponse {
        pub node_id: NodeId,
        pub term: u64,
        pub key: Key,
    }
}

pub mod command {
    use crate::payload::Key;
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    pub enum CommandRequest {
        Set(Key, Payload), // fixme wrapping with Arc may have sense
        Commit(Key),
        Get(Key),
        Update, // reserved for future implementation
        Delete(Key),
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub enum Payload {
        Integer(i64),
        Float(f64),
        String(String),
        DateTime(DateTime<Utc>),
        Object, // reserved for future implementation
    }
}

pub mod log {
    use crate::node_state::NodeId;
    use crate::transport::model::command::CommandRequest;
    use chrono::{DateTime, Utc};

    pub struct LogReplicationRequest {
        pub node_id: NodeId,
        pub from: DateTime<Utc>,
    }

    pub struct LogReplicationResponse {
        pub node_id: NodeId,
        pub commands: Vec<CommandRequest>,
    }
}

pub mod vote {
    use crate::node_state::NodeId;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    pub struct VoteRequest {
        pub node_id: NodeId,
        pub term: u64,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct VoteResponse {
        pub node_id: NodeId,
        pub term: u64,
        pub voted: bool,
    }

}
