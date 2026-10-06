use crate::payload::{Key, Payload};
use std::sync::Arc;

type InstanceId = str;

pub trait CandidateClient {
    /* Candidate */
    fn call_for_vote(&self, instance_id: &InstanceId);
}

pub trait FollowerClient {
    /* Follower */
    fn vote_for(&self, instance_id: &InstanceId);
    fn ack(&self, key: &dyn Key);
    fn ack_commit(&self, key: &dyn Key);
    fn heartbeat(&self);
}

pub trait LeaderClient {
    /* Leader */
    fn send_payload(&self, payload: Box<dyn Payload>);
    fn commit(&self, key: &dyn Key);
    fn heartbeat(&self);
}

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
