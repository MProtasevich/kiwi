use crate::payload::{Payload, ValueId};
use std::fmt::Debug;
use std::sync::Arc;

type InstanceId = str;

pub trait CandidateClient: Debug + Send + Sync {
    fn call_for_vote(&self, instance_id: &InstanceId);
}

pub trait FollowerClient: Debug + Send + Sync {
    fn vote_for(&self, instance_id: &InstanceId);
    fn ack(&self, key: &ValueId);
    fn ack_commit(&self, key: &ValueId);
    fn heartbeat(&self);
}

pub trait LeaderClient: Debug + Send + Sync {
    fn send_payload(&self, payload: Payload);
    fn commit(&self, key: &ValueId);
    fn heartbeat(&self);
}

#[derive(Debug)]
pub struct RemoteNode {
    candidate_client: Arc<dyn CandidateClient>,
    follower_client: Arc<dyn FollowerClient>,
    leader_client: Arc<dyn LeaderClient>,
}

impl RemoteNode {
    pub fn call_for_vote(&self) {
        todo!()
    }

    pub fn vote_for(&self, id: &InstanceId) {
        todo!()
    }
}
