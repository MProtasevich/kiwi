use crate::log::CommandLog;
use crate::node_state::states::{Candidate, Follower, Leader, StateOption};
use crate::payload::{Command, CommandType, Key};
use crate::remote_node::ClusterClient;
use crate::storage::Storage;
use crate::transport::model::vote::VoteResponse;
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeId(Arc<str>);

impl NodeId {
    pub fn new(value: &str) -> Self {
        NodeId(Arc::from(value))
    }
}

pub mod states {
    use crate::node_state::NodeId;
    use std::fmt::Debug;

    pub(super) trait StateOption: Debug {}

    #[derive(Debug, Clone, Default)]
    pub struct Follower {
        pub(super) leader_id: Option<NodeId>,
    }

    impl Follower {
        pub(super) fn new(leader_id: NodeId) -> Self {
            Self { leader_id: Some(leader_id) }
        }
    }
    impl StateOption for Follower {}

    #[derive(Debug, Clone)]
    pub struct Candidate;
    impl StateOption for Candidate {}

    #[derive(Debug, Clone)]
    pub struct Leader;
    impl StateOption for Leader {}
}

#[derive(Debug)]
pub enum Node {
    Follower(State<Follower>),
    Candidate(State<Candidate>),
    Leader(State<Leader>),
}

macro_rules! impl_try_from_node {
    ($($fn_name:tt($($leader_id:ident: $id_type:ty)?) => $variant:ident($nested_type:ty)),* $(,)?) => {
        $(
            pub fn $fn_name (&self$(, $leader_id: $id_type)?) -> Result<$nested_type> {
                match self {
                    Self::$variant(state) => {
                        $(let _ = &state._verify_leader($leader_id)?;)?
                        Ok(state.clone())
                    },
                    _ => Err(anyhow!("Invalid node state: [{:?}]. Expected: [{:?}]", self._state(), stringify!($variant))),
                }
            }
        )*
    };
}

impl Node {

    #[inline]
    fn _state(&self) -> &dyn StateOption {
        match self {
            Node::Follower(state) => &state.state,
            Node::Candidate(state) => &state.state,
            Node::Leader(state) => &state.state,
        }
    }

    impl_try_from_node!(
        try_follower(leader_id: NodeId) => Follower(State<Follower>),
        try_candidate() => Candidate(State<Candidate>),
        try_leader() => Leader(State<Leader>),
    );

}

#[allow(private_bounds)]
#[derive(Debug, Clone)]
pub struct State<S: StateOption> {
    node_id: NodeId,
    cluster_nodes: Arc<ClusterClient>,
    log: Arc<dyn CommandLog>,
    storage: Arc<dyn Storage>,
    term: Arc<AtomicU64>,
    state: S,
}

#[allow(private_bounds)]
impl<S: StateOption> State<S> {

    pub fn new(node_id: NodeId, cluster_nodes: Arc<ClusterClient>, log: Arc<dyn CommandLog>, storage: Arc<dyn Storage>) -> State<Follower> {
        State {
            node_id,
            cluster_nodes,
            log,
            storage,
            term: Arc::default(),
            state: Follower::default(),
        }
    }

    #[inline(always)]
    fn _into<IntoState: StateOption>(self, state: IntoState) -> State<IntoState> {
        State {
            node_id: self.node_id,
            cluster_nodes: self.cluster_nodes,
            log: self.log,
            storage: self.storage,
            term: self.term,
            state,
        }
    }

    #[inline(always)]
    async fn _push(&self, key: Key, command: Command) -> Result<()> {
        match command.get_type() {
            CommandType::Read => Ok(()),
            CommandType::Write | CommandType::Modify => {
                let result = self.log.push(key.clone(), command);
                if result.is_ok() {
                    self._commit(key).await?
                }
                result
            },
        }
    }

    #[inline(always)]
    async fn _commit(&self, key: Key) -> Result<()> {
        // FIXME Send result to another thread, which processes heartbeats, via mspc channel instead
        self.cluster_nodes.commit(&key).await?;
        let payload = self.log.commit(key.clone())?;
        self.storage.push(key, payload)?;
        Ok(())
    }

    #[inline]
    fn _vote(&self, proposed_term: u64) -> VoteResponse {
        let current_term = self.term.fetch_max(proposed_term, Ordering::Relaxed);

        VoteResponse {
            node_id: self.node_id.clone(),
            term: proposed_term,
            voted: proposed_term > current_term,
        }
    }

}

impl State<Follower> {

    pub fn into_candidate(self) -> State<Candidate> {
        self._into(Candidate)
    }

    pub fn set_leader(&mut self, leader_id: NodeId) {
        self.state.leader_id = Some(leader_id);
    }

    pub async fn push(&self, key: Key, command: Command) -> Result<()> {
        // FIXME Follower should just apply changes, without notification
        self._push(key, command).await
    }

    pub async fn commit(&self, key: Key) -> Result<()> {
        // FIXME Follower should just apply changes, without notification
        self._commit(key).await
    }

    pub fn vote(&self, leader: NodeId, proposed_term: u64) -> Result<VoteResponse> {
        self._verify_leader(leader)?;
        Ok(self._vote(proposed_term))
    }

    #[inline]
    fn _verify_leader(&self, leader: NodeId) -> Result<()> {
        if let Some(leader_id) = &self.state.leader_id && leader_id == &leader {
            Ok(())
        } else {
            Err(anyhow!("Verification failed. Node's leader_id = [{:?}], request's leader_id = [{:?}]",
                self.state.leader_id.as_ref(), leader))
        }
    }

}

impl State<Candidate> {

    pub fn into_leader(self) -> State<Leader> {
        self._into(Leader)
    }

    pub fn into_follower(self, leader_id: NodeId) -> State<Follower> {
        self._into(Follower::new(leader_id))
    }

    pub async fn start_election(self) -> Result<State<Leader>, State<Follower>> {
        let result = self.cluster_nodes.ask_for_vote(&self.node_id).await;
        match result {
            Ok(_) => Ok(self.into_leader()),
            Err(_) => Err(self._into(Follower { leader_id: None })),
        }
    }

    pub fn vote(&self, proposed_term: u64) -> VoteResponse {
        self._vote(proposed_term)
    }

}

impl State<Leader> {

    pub fn into_follower(self, leader_id: NodeId) -> State<Follower> {
        self._into(Follower::new(leader_id))
    }

    pub async fn push(&self, key: Key, command: Command) -> Result<()> {
        self._push(key, command).await
    }

    pub async fn commit(&self, key: Key) -> Result<()> {
        self._commit(key).await
    }

}

#[cfg(test)]
mod tests {
    use crate::log::InMemoryCommandLog;
    use crate::node_state::{Follower, NodeId, State};
    use crate::storage::InMemoryStorage;
    use std::sync::Arc;

    fn change_state() {
        let node = State::<Follower>::new(NodeId::default(), Arc::default(), Arc::new(InMemoryCommandLog), Arc::new(InMemoryStorage));
        let node1 = node.into_candidate();
        let node2 = node1.into_leader();
    }
}