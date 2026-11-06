use crate::node_state::NodeId;
use crate::transport::model::command::Command;
use crate::transport::model::common::{CommitRequest, CommitResponse, HeartbeatRequest, HeartbeatResponse};
use crate::transport::model::log::{LogReplicationRequest, LogReplicationResponse};
use crate::transport::model::vote::{VoteRequest, VoteResponse};
use crate::transport::model::ClusterMessage;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

#[async_trait]
pub trait CandidateClient: Debug + Send + Sync {
    async fn ask_for_vote(&self, req: &ClusterMessage<VoteRequest>) -> ClusterMessage<VoteResponse>;

}

#[async_trait]
pub trait FollowerClient: Debug + Send + Sync {

    async fn request_log(&self, req: ClusterMessage<LogReplicationRequest>) -> ClusterMessage<LogReplicationResponse>;

}

#[async_trait]
pub trait LeaderClient: Debug + Send + Sync {

    async fn send_payload(&self, req: &ClusterMessage<Command>) -> ClusterMessage<()>;

    async fn commit(&self, req: &ClusterMessage<CommitRequest>) -> ClusterMessage<CommitResponse>;

    async fn heartbeat(&self, req: &ClusterMessage<HeartbeatRequest>) -> ClusterMessage<HeartbeatResponse>;

}

#[derive(Debug)]
pub struct RemoteNode {
    candidate_client: Arc<dyn CandidateClient>,
    follower_client: Arc<dyn FollowerClient>,
    leader_client: Arc<dyn LeaderClient>,
}

impl RemoteNode {
    /// Builds a remote node from its per-role transport clients.
    ///
    /// A peer may implement only a subset of the roles; in that case the remaining
    /// clients can be supplied by the same object (as an in-memory transport does).
    pub fn new(
        candidate_client: Arc<dyn CandidateClient>,
        follower_client: Arc<dyn FollowerClient>,
        leader_client: Arc<dyn LeaderClient>,
    ) -> Self {
        Self {
            candidate_client,
            follower_client,
            leader_client,
        }
    }
}

#[derive(Debug, Default)]
pub struct ClusterClient {
    node_id: NodeId,
    remote_nodes: Arc<HashMap<NodeId, RemoteNode>>,
    max_timeout: Duration,
}

impl ClusterClient {

    #[inline(always)]
    fn _empty_map<K, V>() -> HashMap<K, V> {
        HashMap::<K, V>::with_capacity(0)
    }

    pub fn new(node_id: NodeId, remote_nodes: Arc<HashMap<NodeId, RemoteNode>>, max_timeout: Duration) -> Self {
        ClusterClient {
            node_id,
            remote_nodes,
            max_timeout,
        }
    }

    pub async fn heartbeat(&self, request: ClusterMessage<HeartbeatRequest>) -> Result<()> {
        todo!()
    }

    pub async fn push(&self, request: ClusterMessage<Command>) -> Result<()> {
        todo!()
    }

    pub async fn commit(&self, request: ClusterMessage<CommitRequest>) -> Result<()> {
        todo!()
    }

    pub async fn ask_for_vote(&self, request: ClusterMessage<VoteRequest>) -> Result<()> {
        todo!()
    }

    pub async fn ask_for_log(&self, leader_id: NodeId, request: ClusterMessage<LogReplicationRequest>) -> Result<LogReplicationResponse> {
        todo!()
    }
    
    pub fn exists(&self, node_id: &NodeId) -> bool {
        self.remote_nodes.contains_key(node_id)
    }

}
