use crate::log::CommandLog;
use crate::payload::{Command, CommandType, ValueId};
use crate::remote_node::RemoteNode;
use crate::storage::Storage;
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
    ($enum_type:ident { $($fn_name:tt($($leader_id:ident: $id_type:ty)?) => $variant:ident($nested_type:ty)),* $(,)? }) => {
        $(
            pub fn $fn_name (&self$(, $leader_id: $id_type)?) -> Result<$nested_type> {
                match self {
                    $enum_type::$variant(state) => {
                        $(Node::verify_leader(&state, &$leader_id)?;)?
                        Ok(state.clone())
                    },
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
        try_follower(leader_id: &InstanceId) => Follower(NodeStateMachine<Follower>),
        try_candidate() => Candidate(NodeStateMachine<Candidate>),
        try_leader() => Leader(NodeStateMachine<Leader>),
    });

    #[inline]
    fn verify_leader(follower: &NodeStateMachine<Follower>, leader: &InstanceId) -> Result<()> {
        if let Some(leader_id) = &follower.state.leader_id && leader_id == leader {
            Ok(())
        } else {
            Err(anyhow!("Verification failed. Node's leader_id = [{:?}], request's leader_id = [{:?}]", follower.state.leader_id.as_ref(), leader.as_ref()))
        }
    }

}

pub type InstanceId = Arc<str>;

#[derive(Debug, Clone)]
pub struct NodeStateMachine<State: NodeState> {
    cluster_nodes: Arc<HashMap<InstanceId, RemoteNode>>,
    log: Arc<dyn CommandLog>,
    storage: Arc<dyn Storage>,
    state: State,
}

impl<T: NodeState> NodeStateMachine<T> {
    pub fn new(cluster_nodes: Arc<HashMap<InstanceId, RemoteNode>>, log: Arc<dyn CommandLog>, storage: Arc<dyn Storage>) -> NodeStateMachine<Follower> {
        NodeStateMachine {
            cluster_nodes,
            log,
            storage,
            state: Follower::default(),
        }
    }

    fn into<State: NodeState>(self, state: State) -> NodeStateMachine<State> {
        NodeStateMachine {
            cluster_nodes: self.cluster_nodes,
            log: self.log,
            storage: self.storage,
            state,
        }
    }

    #[inline(always)]
    fn _push(&self, key: ValueId, command: Command) -> Result<()> {
        match command.get_type() {
            CommandType::Read => Ok(()),
            CommandType::Write | CommandType::Modify => self.log.push(key, command),
        }
    }

    #[inline(always)]
    fn _commit(&self, key: ValueId) -> Result<()> {
        let payload = self.log.commit(Arc::clone(&key))?;
        self.storage.push(key, payload)?;
        Ok(())
    }

}

impl NodeStateMachine<Follower> {
    pub fn into_candidate(self) -> NodeStateMachine<Candidate> {
        self.into(Candidate)
    }

    pub fn set_leader(&mut self, leader_id: InstanceId) {
        self.state.leader_id = Some(leader_id);
    }

    pub fn push(&self, key: ValueId, command: Command) -> Result<()> {
        self._push(key, command)
    }

    pub fn commit(&self, key: ValueId) -> Result<()> {
        self._commit(key)
    }

    pub fn vote(&self, leader: &InstanceId) -> Result<()> {
        self.cluster_nodes.pin().get(leader)
            .with_context(|| format!("Couldn't find leader {leader}"))?
            .vote_for(leader);
        Ok(())
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

    pub fn push(&self, key: ValueId, command: Command) -> Result<()> {
        self._push(key, command)
    }

    pub fn commit(&self, key: ValueId) -> Result<()> {
        self._commit(key)
    }
}

#[cfg(test)]
mod tests {
    use crate::log::InMemoryCommandLog;
    use crate::node_state::{Follower, NodeStateMachine};
    use crate::storage::InMemoryStorage;
    use papaya::HashMap;
    use std::sync::Arc;

    fn change_state() {
        let node = NodeStateMachine::<Follower>::new(Arc::new(HashMap::new()), Arc::new(InMemoryCommandLog), Arc::new(InMemoryStorage));
        let node1 = node.into_candidate();
        let node2 = node1.into_leader();
    }
}