use crate::payload::{Log, Payload, Value};
use crate::remote_node::RemoteNode;
use anyhow::{Context, Result, anyhow};
use papaya::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

pub trait NodeState: Debug {
}

#[derive(Debug, Clone, Default)]
pub struct Follower {
    leader_id: Option<InstanceId>,
}

impl Follower {
    fn new(leader_id: InstanceId) -> Self {
        Self { leader_id: Some(leader_id) }
    }
}
impl NodeState for Follower {}

#[derive(Debug, Clone)]
pub struct Candidate;
impl NodeState for Candidate {}

#[derive(Debug, Clone)]
pub struct Leader;
impl NodeState for Leader {}

#[derive(Debug)]
pub enum Node {
    Follower(NodeStateMachine<Follower>),
    Candidate(NodeStateMachine<Candidate>),
    Leader(NodeStateMachine<Leader>),
}

macro_rules! impl_try_from_node {
    ($enum_type:ident { $($fn_name:tt => $variant:ident($nested_type:ty)),* $(,)? }) => {
        $(
            pub fn $fn_name (&self) -> Result<$nested_type> {
                match self {
                    $enum_type::$variant(state) => Ok(state.clone()),
                    _ => Err(anyhow!("Invalid node state: [{:?}]. Expected: [{:?}]", self.state(), stringify!($variant))),
                }
            }
        )*
    };
}

impl Node {
    fn state(&self) -> &dyn NodeState {
        match self {
            Node::Follower(state) => &state.state,
            Node::Candidate(state) => &state.state,
            Node::Leader(state) => &state.state,
        }
    }

    impl_try_from_node!(Node {
        try_follower => Follower(NodeStateMachine<Follower>),
        try_candidate => Candidate(NodeStateMachine<Candidate>),
        try_leader => Leader(NodeStateMachine<Leader>),
    });
}

pub type InstanceId = Arc<str>;

#[derive(Debug, Clone)]
pub struct NodeStateMachine<State: NodeState> {
    cluster_nodes: Arc<HashMap<InstanceId, RemoteNode>>,
    log: Arc<Log>,
    state: State,
}

impl<T: NodeState> NodeStateMachine<T> {
    pub fn new(cluster_nodes: HashMap<InstanceId, RemoteNode>) -> NodeStateMachine<Follower> {
        NodeStateMachine {
            cluster_nodes: Arc::new(cluster_nodes),
            log: Arc::new(Log::new(Box::<Value>::default())),
            state: Follower::default(),
        }
    }

    fn into<State: NodeState>(self, state: State) -> NodeStateMachine<State> {
        NodeStateMachine {
            cluster_nodes: self.cluster_nodes,
            log: self.log,
            state,
        }
    }
}

impl NodeStateMachine<Follower> {
    pub fn into_candidate(self) -> NodeStateMachine<Candidate> {
        self.into(Candidate)
    }

    pub fn set_leader(&mut self, leader_id: InstanceId) {
        self.state.leader_id = Some(leader_id);
    }

    pub fn accept(&mut self, payload: impl Payload + 'static) -> Result<()> {
        self.log = Arc::new(Log::new(Box::new(payload)));
        todo!()
    }

    pub fn committed(&mut self, payload: impl Payload + 'static) -> Result<()> {
        todo!()
    }

    pub fn vote(&self, leader: &InstanceId) -> Result<()> {
        self.cluster_nodes.pin().get(leader)
            .with_context(|| format!("Couldn't find leader {leader}"))?
            .vote_for(leader);
        Ok(())
    }

    pub fn verify_leader(&self, leader: &InstanceId) -> Result<()> {
        if let Some(leader_id) = &self.state.leader_id && leader_id == leader {
            Ok(())
        } else {
            Err(anyhow!("Verification failed. Node's leader_id = [{:?}], request's leader_id = [{:?}]", self.state.leader_id.as_ref(), leader.as_ref()))
        }
    }
}

impl NodeStateMachine<Candidate> {
    pub fn into_leader(self) -> NodeStateMachine<Leader> {
        self.into(Leader)
    }

    pub fn into_follower(self, leader_id: InstanceId) -> NodeStateMachine<Follower> {
        self.into(Follower::new(leader_id))
    }

    pub fn start_election(&self) -> Result<()> {
        for remote_node in self.cluster_nodes.pin().values() {
            remote_node.call_for_vote();
        }
        todo!("Consider concurrent broadcast call instead")
    }
}

impl NodeStateMachine<Leader> {
    pub fn into_follower(self, leader_id: InstanceId) -> NodeStateMachine<Follower> {
        self.into(Follower::new(leader_id))
    }

    pub fn push(&mut self, payload: impl Payload + 'static) -> Result<()> {
        self.log = Arc::new(Log::new(Box::new(payload)));
        todo!()
    }

    pub fn commit(&mut self, payload: impl Payload + 'static) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::node_state::{Follower, NodeStateMachine};
    use papaya::HashMap;

    fn change_state() {
        let node = NodeStateMachine::<Follower>::new(HashMap::new());
        let node1 = node.into_candidate();
        let node2 = node1.into_leader();
    }
}