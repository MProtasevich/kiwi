use crate::node_state::NodeId;
use crate::transport::model::command::Command;
use crate::transport::model::common::{CommitRequest, CommitResponse, HeartbeatRequest, HeartbeatResponse};
use crate::transport::model::log::{LogReplicationRequest, LogReplicationResponse};
use crate::transport::model::vote::{VoteRequest, VoteResponse};
use crate::transport::model::{ClusterMessage, NodeAware};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

#[async_trait]
pub trait CandidateClient: Debug + Send + Sync {

    async fn ask_for_vote(&self, node_id: &ClusterMessage<VoteRequest>) -> ClusterMessage<VoteResponse>;

}

#[async_trait]
pub trait FollowerClient: Debug + Send + Sync {

    async fn request_log(&self, request: ClusterMessage<LogReplicationRequest>) -> ClusterMessage<LogReplicationResponse>;

}

#[async_trait]
pub trait LeaderClient: Debug + Send + Sync {

    async fn send_payload(&self, payload: &ClusterMessage<Command>) -> ClusterMessage<()>;

    async fn commit(&self, req: &ClusterMessage<CommitRequest>) -> ClusterMessage<CommitResponse>;

    async fn heartbeat(&self, req: &ClusterMessage<HeartbeatRequest>) -> ClusterMessage<HeartbeatResponse>;

}

#[derive(Debug)]
pub struct RemoteNode {
    candidate_client: Arc<dyn CandidateClient>,
    follower_client: Arc<dyn FollowerClient>,
    leader_client: Arc<dyn LeaderClient>,
}

#[derive(Debug, Default)]
pub struct ClusterClient {
    node_id: NodeId,
    remote_nodes: Arc<HashMap<NodeId, RemoteNode>>,
}

fn accept_all<T>(_response: &T) -> bool {
    true
}

impl ClusterClient {

    const QUORUM_MULTIPLIER: usize = 2;

    pub fn new(node_id: NodeId, remote_nodes: Arc<HashMap<NodeId, RemoteNode>>) -> Self {
        ClusterClient {
            node_id,
            remote_nodes,
        }
    }

    pub async fn heartbeat(&self, request: ClusterMessage<HeartbeatRequest>) -> Result<()> {
        self.broadcast_call(&request,
                async |node, req| node.leader_client.heartbeat(req).await,
                accept_all,
                |_| true)
            .await
            .map(|_| ())
    }

    pub async fn push(&self, request: ClusterMessage<Command>) -> Result<()> {
        self.broadcast_call_quorum(&request,
                async |node, command| node.leader_client.send_payload(command).await,
                accept_all)
            .await
            .map(|_| ())
    }

    pub async fn commit(&self, request: ClusterMessage<CommitRequest>) -> Result<()> {
        self.broadcast_call_quorum(&request,
                async |node, req| node.leader_client.commit(req).await,
                accept_all)
            .await
            .map(|_| ())
    }

    pub async fn ask_for_vote(&self, request: ClusterMessage<VoteRequest>) -> Result<()> {
        self.broadcast_call_quorum(&request,
                async |node, req| node.candidate_client.ask_for_vote(req).await,
                accept_all)
            .await
            .map(|_| ())
    }

    pub async fn ask_for_log(&self, request: ClusterMessage<LogReplicationRequest>) -> Result<LogReplicationResponse> {
        let Some(leader_node) = self.remote_nodes.get(request.node_id()) else {
            return Err(anyhow!("leader with id [{:?}] not found", request.node_id()));
        };
        let message = leader_node.follower_client.request_log(request).await;
        Ok(message.payload)
    }

    #[inline]
    async fn broadcast_call_quorum<Req: Debug, Resp: NodeAware, RemCall, SuccessPred>(
            &self, request: &Req, remote_call_fn: RemCall, success_predicate: SuccessPred)
        -> Result<HashMap<NodeId, Resp>>
    where RemCall: AsyncFn(&RemoteNode, &Req) -> Resp,
          SuccessPred: Fn(&Resp) -> bool
    {
        // remote_nodes should contain all cluster nodes excluding the current
        self.broadcast_call(request, remote_call_fn, success_predicate,
                |successful_requests| successful_requests * Self::QUORUM_MULTIPLIER >= self.remote_nodes.len()).await
    }

    // FIXME Response should be a Result<Resp, Error>, so in case http call failed, it won't be classified as succeeded
    async fn broadcast_call<Req: Debug, Resp: NodeAware, RemCall, SuccessPred, QuorumPred>(&self, request: &Req,
            remote_call_fn: RemCall, success_criteria: SuccessPred, quorum_predicate: QuorumPred)
            -> Result<HashMap<NodeId, Resp>>
    where RemCall: AsyncFn(&RemoteNode, &Req) -> Resp,
          SuccessPred: Fn(&Resp) -> bool,
          QuorumPred: Fn(usize) -> bool,
    {
        let mut remote_calls = self.remote_nodes.iter()
            .filter(|(node_id, _)| self.node_id != **node_id)
            .map(|(_, remote_node)| remote_call_fn(remote_node, request))
            .collect::<FuturesUnordered<_>>();

        let mut succeeded_requests = HashMap::with_capacity(remote_calls.len());
        while let Some(response) = remote_calls.next().await {
            if success_criteria(&response) {
                succeeded_requests.insert(response.node_id().clone(), response);
            }
            if quorum_predicate(succeeded_requests.len()) {
                return Ok(succeeded_requests);
            }
        }
        Err(anyhow!("Batch request failed. Quorum hasn't been achieved for request: {request:?}"))
    }

}
