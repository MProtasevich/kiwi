use crate::payload::{Value, Key};
use crate::transport::model::VoteResp;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use crate::node_state::InstanceId;

#[async_trait]
pub trait CandidateClient: Debug + Send + Sync {

    async fn ask_for_vote(&self, instance_id: InstanceId) -> VoteResp;

}

#[async_trait]
pub trait FollowerClient: Debug + Send + Sync {

    // async fn ack(&self, key: ValueId);

    // async fn ack_commit(&self, key: &ValueId);

}

#[async_trait]
pub trait LeaderClient: Debug + Send + Sync {

    async fn send_payload(&self, payload: Value);

    async fn commit(&self, key: Key);

    async fn heartbeat(&self);

}

#[derive(Debug)]
pub struct RemoteNode {
    candidate_client: Arc<dyn CandidateClient>,
    follower_client: Arc<dyn FollowerClient>,
    leader_client: Arc<dyn LeaderClient>,
}

impl RemoteNode {

    pub async fn call_for_vote(&self) -> VoteResp {
        todo!()
    }

    pub async fn vote_for(&self, id: InstanceId) {
        todo!()
    }

    pub async fn commit(&self, key: Key) {
        todo!()
    }

}

#[derive(Debug, Default)]
pub struct ClusterClient {
    instance_id: InstanceId,
    remote_nodes: Arc<HashMap<InstanceId, RemoteNode>>,
}

fn accept_all<T>(_response: &T) -> bool {
    true
}

impl ClusterClient {

    const QUORUM_MULTIPLIER: usize = 2;

    pub fn new(instance_id: InstanceId, remote_nodes: Arc<HashMap<InstanceId, RemoteNode>>) -> Self {
        ClusterClient {
            instance_id,
            remote_nodes,
        }
    }

    pub async fn commit(&self, key: Key) -> Result<()> {
        self.broadcast_call_quorum(&key, // Double wrap on Arc because I can't assign str as generic param
               async |node, req| node.leader_client.commit(req.clone()).await,
               accept_all)
            .await
            .map(|_| ())
    }

    pub async fn ask_for_vote(&self, instance_id: InstanceId) -> Result<()> {
        self.broadcast_call_quorum(&instance_id,
               async |node, req| node.candidate_client.ask_for_vote(req.clone()).await,
               accept_all)
            .await
            .map(|_| ())
    }

    #[inline]
    async fn broadcast_call_quorum<Req: Debug, Resp, RemCall, SuccessPred>(
        &self, request: &Req, remote_call_fn: RemCall, success_predicate: SuccessPred) -> Result<Vec<Resp>>
    where RemCall: AsyncFn(&RemoteNode, &Req) -> Resp,
          SuccessPred: Fn(&Resp) -> bool
    {
        self.broadcast_call(request, remote_call_fn, success_predicate,
                |successful_requests| successful_requests * Self::QUORUM_MULTIPLIER > self.remote_nodes.len()).await
    }

    async fn broadcast_call<Req: Debug, Resp, RemCall, SuccessPred, QuorumPred>(&self, request: &Req,
            remote_call_fn: RemCall, success_criteria: SuccessPred, quorum_predicate: QuorumPred) -> Result<Vec<Resp>>
    where RemCall: AsyncFn(&RemoteNode, &Req) -> Resp,
          SuccessPred: Fn(&Resp) -> bool,
          QuorumPred: Fn(usize) -> bool,
    {
        let mut remote_calls = self.remote_nodes.iter()
            .filter(|(instance_id, _)| self.instance_id != **instance_id)
            .map(|(_, remote_node)| remote_call_fn(remote_node, request))
            .collect::<FuturesUnordered<_>>();

        let mut succeeded_requests = Vec::with_capacity(remote_calls.len());
        while let Some(response) = remote_calls.next().await {
            if success_criteria(&response) {
                succeeded_requests.push(response);
            }
            if quorum_predicate(succeeded_requests.len()) {
                return Ok(succeeded_requests);
            }
        }
        Err(anyhow!("Batch request failed. Quorum hasn't been achieved for request: {request:?}"))
    }

}
