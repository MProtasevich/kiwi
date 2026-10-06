use crate::payload::{Vault, Payload, Value};
use anyhow::{Context, Result};
use std::collections::HashMap;

pub struct Follower {
    leader_id: InstanceId,
}
pub struct Candidate;
pub struct Leader;

pub struct RemoteNode {
    client: (),
}

pub type InstanceId = String;

impl RemoteNode {
    pub fn call_for_vote(&self) {
        todo!()
    }

    pub fn vote_for(&self, id: &InstanceId) {
        todo!()
    }
}

pub struct Node<State> {
    cluster_nodes: HashMap<InstanceId, RemoteNode>,
    value: Vault,
    state: State,
}

impl<T> Node<T> {
    pub fn new(cluster_nodes: HashMap<InstanceId, RemoteNode>, leader_id: InstanceId) -> Node<Follower> {
        Node {
            cluster_nodes,
            value: Vault::new(Box::new(Value::default())),
            state: Follower { leader_id },
        }
    }

    pub fn into_follower(self, leader_id: InstanceId) -> Node<Follower> {
        self.into(Follower { leader_id })
    }

    fn into<State>(self, state: State) -> Node<State> {
        Node {
            cluster_nodes: self.cluster_nodes,
            value: self.value,
            state,
        }
    }
}

impl Node<Follower> {
    pub fn into_candidate(self) -> Node<Candidate> {
        self.into(Candidate)
    }

    pub fn set_leader(&mut self, leader_id: InstanceId) {
        self.state.leader_id = leader_id;
    }

    pub fn accept(&mut self, payload: impl Payload + 'static) -> Result<()> {
        self.value = Vault::new(Box::new(payload));
        Ok(())
    }

    pub fn committed(&mut self, payload: impl Payload + 'static) -> Result<()> {
        self.value.commit();
        Ok(())
    }

    pub fn vote(&self, leader: &InstanceId) -> Result<()> {
        self.cluster_nodes.get(leader)
            .with_context(|| format!("Couldn't find leader {leader}"))?
            .vote_for(leader);
        Ok(())
    }

    pub fn pong(&self, leader: &Node<Leader>) -> Result<()> {
        unimplemented!()
    }
}

impl Node<Candidate> {
    pub fn into_leader(self) -> Node<Leader> {
        self.into(Leader)
    }

    pub fn start_election(&self) -> Result<()> {
        for remote_node in self.cluster_nodes.values() {
            remote_node.call_for_vote();
        }
        Ok(())
    }
}

impl Node<Leader> {
    pub fn push(&mut self, payload: impl Payload + 'static) -> Result<()> {
        self.value = Vault::new(Box::new(payload));
        Ok(())
    }

    pub fn commit(&mut self, payload: impl Payload + 'static) -> Result<()> {
        self.value.commit();
        Ok(())
    }

    pub fn ping(&self, followers: &[Node<Follower>]) -> Result<()> {
        unimplemented!()
    }
}
