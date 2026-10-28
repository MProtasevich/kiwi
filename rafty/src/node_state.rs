use crate::job::log_replication::LogReplication;
use crate::job::registry::{JobKey, JobRegistry};
use crate::log::CommandLog;
use crate::node_state::states::{Candidate, Follower, Leader, StateOption};
use crate::payload::CommandType;
use crate::remote_node::ClusterClient;
use crate::storage::Storage;
use crate::transport::model::command::Command;
use crate::transport::model::common::{CommitRequest, HeartbeatRequest};
use crate::transport::model::vote::{VoteRequest, VoteResponse};
use crate::transport::model::{ClusterMessage, Key, NodeAware};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::timeout;

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
        try_follower(leader_id: Option<NodeId>) => Follower(State<Follower>),
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
    job_registry: Arc<JobRegistry>,
    term: Arc<AtomicU64>,
    state: S,
}

#[allow(private_bounds)]
impl<S: StateOption> State<S> {

    pub fn new(node_id: NodeId, cluster_nodes: Arc<ClusterClient>, log: Arc<dyn CommandLog>, storage: Arc<dyn Storage>, job_registry: Arc<JobRegistry>) -> State<Follower> {
        State {
            node_id,
            cluster_nodes,
            log,
            storage,
            job_registry,
            term: Arc::default(),
            state: Follower::default(),
        }
    }

    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn term(&self) -> u64 {
        self.term.load(Ordering::Relaxed)
    }

    #[inline(always)]
    fn _into<IntoState: StateOption>(self, state: IntoState) -> State<IntoState> {
        State {
            node_id: self.node_id,
            cluster_nodes: self.cluster_nodes,
            log: self.log,
            storage: self.storage,
            job_registry: self.job_registry,
            term: self.term,
            state,
        }
    }

    #[inline(always)]
    async fn _push(&self, command: Command, nodes_notification_fn: impl AsyncFn(&ClusterClient, ClusterMessage<Command>) -> Result<()>) -> Result<()> {
        match command.get_type() {
            CommandType::Read => Ok(()),
            CommandType::Write | CommandType::Modify => {
                // let request = Wrapper::new(self.node_id.clone(), self.term.load(Ordering::Relaxed), command);
                // FIXME get rid of cloning
                let command_to_log = command.clone();
                let result = self.log.push(command_to_log);
                if result.is_ok() {
                    // FIXME Send result to another thread, which processes heartbeats, via mspc channel instead
                    let message = ClusterMessage::new(self.node_id.clone(), self.term.load(Ordering::Relaxed), command);
                    nodes_notification_fn(self.cluster_nodes.as_ref(), message).await?
                }
                result
            },
        }
    }

    #[inline(always)]
    async fn _commit(&self, key: Key, nodes_notification_fn: impl AsyncFn(&ClusterClient, ClusterMessage<CommitRequest>) -> Result<()>) -> Result<()> {
        let request = ClusterMessage::new(self.node_id.clone(), self.term.load(Ordering::Relaxed), CommitRequest { key: key.clone() });
        // FIXME Send result to another thread, which processes heartbeats, via mspc channel instead
        nodes_notification_fn(self.cluster_nodes.as_ref(), request).await?;
        // self.cluster_nodes.commit(&key).await?;
        let payload = self.log.commit(key.clone())?;
        self.storage.push(key, payload)?;
        Ok(())
    }

    #[inline]
    fn _vote(&self, proposed_term: u64) -> ClusterMessage<VoteResponse> {
        let current_term = self.term.fetch_max(proposed_term, Ordering::Relaxed);
        ClusterMessage::new(self.node_id.clone(), proposed_term, VoteResponse { voted: proposed_term > current_term })
    }

    fn _construct_msg<T>(&self, payload: T) -> ClusterMessage<T> {
        ClusterMessage::new(self.node_id.clone(), self.term.load(Ordering::Relaxed), payload)
    }

}

impl State<Follower> {

    const CHUNK_SIZE: usize = 1024;

    pub fn into_candidate(self) -> State<Candidate> {
        self._into(Candidate)
    }

    pub fn set_leader(&mut self, leader_id: NodeId) {
        self.state.leader_id = Some(leader_id);
    }

    pub async fn push(&self, command: Command) -> Result<()> {
        self._push(command, async |_, _| Ok(())).await
    }

    pub async fn commit(&self, key: Key) -> Result<()> {
        self._commit(key, async |_, _| Ok(())).await
    }

    pub fn vote(&self, _leader: NodeId, proposed_term: u64) -> Result<ClusterMessage<VoteResponse>> {
        // self._verify_leader(leader)?;
        Ok(self._vote(proposed_term))
    }

    pub fn process_heartbeat(&mut self, request: ClusterMessage<HeartbeatRequest>) -> Result<()> {
        if self.state.leader_id.is_none() {
            self.set_leader(request.node_id().clone())
        }
        let (Some(node_last_entry), Some(leader_last_entry)) = (self.log.get_last_change(), request.last_change) else {
            return Ok(())
        };
        if node_last_entry <= leader_last_entry {
            let Some(_leader_id) = &self.state.leader_id else {
                return Ok(())
            };
            let cluster_nodes = Arc::clone(&self.cluster_nodes);
            let log = Arc::clone(&self.log);
            let term = Arc::clone(&self.term);
            let replication = LogReplication::new(cluster_nodes, log, node_last_entry, term, self.node_id.clone(), Self::CHUNK_SIZE);
            self.job_registry.register_and_start(JobKey::LogReplication, replication);
        }
        Ok(())
    }

    async fn replicate_log(since: DateTime<Utc>) {
        todo!()
    }

    // TODO change Option enum to custom MasterKey, where MasterKey::Key instance can be created exclusively by crate
    #[inline]
    fn _verify_leader(&self, leader: Option<NodeId>) -> Result<()> {
        if leader.is_none() || self.state.leader_id == leader {
            Ok(())
        } else {
            Err(anyhow!("Verification failed. Node's leader_id = [{:?}], request's leader_id = [{:?}]",
                self.state.leader_id.as_ref(), leader))
        }
    }

}

const MAX_TIMEOUT: Duration = Duration::from_millis(500);

impl State<Candidate> {

    pub fn into_leader(self) -> State<Leader> {
        self._into(Leader)
    }

    pub fn into_follower(self, leader_id: NodeId) -> State<Follower> {
        self._into(Follower::new(leader_id))
    }

    pub async fn start_election(self) -> Result<State<Leader>, State<Follower>> {
        self.term.fetch_add(1, Ordering::Relaxed);
        let request = ClusterMessage::new(self.node_id.clone(), self.term.load(Ordering::Relaxed), VoteRequest);
        let result = timeout(MAX_TIMEOUT, self.cluster_nodes.ask_for_vote(request)).await;
        match result {
            Ok(Ok(_)) => Ok(self.into_leader()),
            Ok(Err(_)) | Err(_) => Err(self._into(Follower { leader_id: None })),
        }
    }

    pub fn vote(&self, proposed_term: u64) -> ClusterMessage<VoteResponse> {
        self._vote(proposed_term)
    }

}

impl State<Leader> {

    pub fn into_follower(self, leader_id: NodeId) -> State<Follower> {
        self._into(Follower::new(leader_id))
    }

    pub async fn push(&self, command: Command) -> Result<()> {
        self._push(command, ClusterClient::push).await
    }

    pub async fn commit(&self, key: Key) -> Result<()> {
        self._commit(key, ClusterClient::commit).await
    }

    pub async fn send_heartbeat(&self) -> Result<()> {
        let request = HeartbeatRequest { timestamp: Utc::now(), last_change: self.log.get_last_change() };
        let message = ClusterMessage::new(self.node_id.clone(), self.term.load(Ordering::Relaxed), request);
        timeout(Duration::from_secs(2), self.cluster_nodes.heartbeat(message)).await?
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