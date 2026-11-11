use crate::cluster::{ClusterConfiguration, ConfigChange};
use crate::job::log_replication::LogReplicationJob;
use crate::job::registry::JobRegistry;
use crate::log::CommandLog;
pub use crate::node_state::states::{Candidate, Follower, Leader, StateOption};
use crate::remote_node::ClusterClient;
use crate::storage::Storage;
use crate::transport::model::command::{Command, CommandType};
use crate::transport::model::common::{CommitRequest, HeartbeatRequest};
use crate::transport::model::membership::{
    ConfigurationAck, ConfigurationBroadcast, JoinRequest, JoinResponse,
};
use crate::transport::model::vote::{VoteRequest, VoteResponse};
use crate::transport::model::{ClusterMessage, Key, NodeAware};
use anyhow::{Result, anyhow, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::timeout;

#[derive(Debug, Default, Clone, Hash, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeId(Arc<str>);

impl NodeId {
    pub fn new(value: &str) -> Self {
        NodeId(Arc::from(value))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for NodeId {
    type Err = std::convert::Infallible;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Self::new(value))
    }
}

pub mod states {
    use crate::node_state::NodeId;
    use std::fmt::Debug;

    pub trait StateOption: Debug {}

    #[derive(Debug, Clone, Default)]
    pub struct Follower {
        pub(super) leader_id: Option<NodeId>,
    }

    impl Follower {
        pub(super) fn new(leader_id: NodeId) -> Self {
            Self {
                leader_id: Some(leader_id),
            }
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

    /// Handles a join request in any state: the leader admits, followers redirect.
    pub async fn handle_join(
        &self,
        request: ClusterMessage<JoinRequest>,
    ) -> Result<ClusterMessage<JoinResponse>> {
        match self {
            Node::Follower(state) => Ok(state.handle_join(request)),
            Node::Candidate(state) => Ok(state.handle_join(request)),
            Node::Leader(state) => state.handle_join(request).await,
        }
    }

    pub fn apply_configuration(
        &self,
        request: &ClusterMessage<ConfigurationBroadcast>,
    ) -> ClusterMessage<ConfigurationAck> {
        match self {
            Node::Follower(state) => state.apply_configuration(request),
            Node::Candidate(state) => state.apply_configuration(request),
            Node::Leader(state) => state.apply_configuration(request),
        }
    }
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
    pub fn new(
        node_id: NodeId,
        cluster_nodes: Arc<ClusterClient>,
        log: Arc<dyn CommandLog>,
        storage: Arc<dyn Storage>,
        job_registry: Arc<JobRegistry>,
    ) -> State<Follower> {
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
    async fn _push(
        &self,
        command: Command,
        nodes_notification_fn: impl AsyncFn(&ClusterClient, ClusterMessage<Command>) -> Result<()>,
    ) -> Result<()> {
        match command.get_type() {
            CommandType::Read => Ok(()),
            CommandType::Write | CommandType::Modify => {
                // let request = Wrapper::new(self.node_id.clone(), self.term.load(Ordering::Relaxed), command);
                // FIXME get rid of cloning
                let command_to_log = command.clone();
                let result = self.log.push(command_to_log);
                if result.is_ok() {
                    // FIXME Send result to another thread, which processes heartbeats, via mspc channel instead
                    let message = ClusterMessage::new(
                        self.node_id.clone(),
                        self.term.load(Ordering::Relaxed),
                        command,
                    );
                    nodes_notification_fn(self.cluster_nodes.as_ref(), message).await?
                }
                result
            }
        }
    }

    #[inline(always)]
    async fn _commit(
        &self,
        key: Key,
        nodes_notification_fn: impl AsyncFn(&ClusterClient, ClusterMessage<CommitRequest>) -> Result<()>,
    ) -> Result<()> {
        let request = ClusterMessage::new(
            self.node_id.clone(),
            self.term.load(Ordering::Relaxed),
            CommitRequest { key: key.clone() },
        );
        // FIXME Send result to another thread, which processes heartbeats, via mspc channel instead
        nodes_notification_fn(self.cluster_nodes.as_ref(), request).await?;
        // self.cluster_nodes.commit(&key).await?;
        let payload = self.log.commit(key.clone())?;
        self.storage.push(key, payload)?;
        Ok(())
    }

    #[inline]
    fn _vote(
        &self,
        candidate_id: NodeId,
        proposed_term: u64,
    ) -> Result<ClusterMessage<VoteResponse>> {
        if !self.cluster_nodes.exists(&candidate_id) {
            return Err(anyhow!("Node [{candidate_id}] not found."));
        }
        let current_term = self.term.fetch_max(proposed_term, Ordering::Relaxed);
        let payload = VoteResponse {
            voted: proposed_term > current_term,
        };
        Ok(ClusterMessage::new(
            self.node_id.clone(),
            self.term.load(Ordering::Relaxed),
            payload,
        ))
    }

    fn _construct_msg<T>(&self, payload: T) -> ClusterMessage<T> {
        ClusterMessage::new(
            self.node_id.clone(),
            self.term.load(Ordering::Relaxed),
            payload,
        )
    }

    pub fn cluster_nodes(&self) -> &Arc<ClusterClient> {
        &self.cluster_nodes
    }

    pub fn configuration(&self) -> Arc<ClusterConfiguration> {
        self.cluster_nodes.configuration()
    }

    /// Applies a configuration entry received from the leader.
    ///
    /// Stale-term messages are rejected; the configuration itself is applied atomically
    /// and is idempotent under retries (see [`ClusterClient::install_configuration`]).
    pub fn apply_configuration(
        &self,
        request: &ClusterMessage<ConfigurationBroadcast>,
    ) -> ClusterMessage<ConfigurationAck> {
        let current_term = self.term.load(Ordering::Relaxed);
        let ack = if request.metadata.term < current_term {
            ConfigurationAck::rejected(
                self.configuration().index,
                format!(
                    "stale term: message term {} < local term {current_term}",
                    request.metadata.term
                ),
            )
        } else {
            self.term
                .fetch_max(request.metadata.term, Ordering::Relaxed);
            self.cluster_nodes.install_configuration(request)
        };
        ClusterMessage::new(self.node_id.clone(), self.term.load(Ordering::Relaxed), ack)
    }

    #[inline]
    fn _join_response(&self, payload: JoinResponse) -> ClusterMessage<JoinResponse> {
        self._construct_msg(payload)
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

    pub fn leader_id(&self) -> Option<&NodeId> {
        self.state.leader_id.as_ref()
    }

    /// A follower cannot admit nodes; it redirects the joiner to the leader it knows.
    pub fn handle_join(
        &self,
        request: ClusterMessage<JoinRequest>,
    ) -> ClusterMessage<JoinResponse> {
        let payload = &request.payload;
        if payload.cluster_id != self.configuration().cluster_id {
            return self._join_response(JoinResponse::Rejected {
                reason: format!(
                    "cluster [{}] does not match local cluster [{}]",
                    payload.cluster_id,
                    self.configuration().cluster_id
                ),
            });
        }
        let leader = self.state.leader_id.as_ref().and_then(|leader_id| {
            self.cluster_nodes
                .address_of(leader_id)
                .map(|address| (leader_id.clone(), address))
        });
        match leader {
            Some((leader_id, address)) => {
                self._join_response(JoinResponse::Redirect { leader_id, address })
            }
            None => self._join_response(JoinResponse::Rejected {
                reason: "this node does not know a leader; retry another seed".to_owned(),
            }),
        }
    }

    pub async fn push(&self, command: Command) -> Result<()> {
        self._push(command, async |_, _| Ok(())).await
    }

    pub async fn commit(&self, key: Key) -> Result<()> {
        self._commit(key, async |_, _| Ok(())).await
    }

    pub fn vote(
        &self,
        candidate_id: NodeId,
        proposed_term: u64,
    ) -> Result<ClusterMessage<VoteResponse>> {
        self._vote(candidate_id, proposed_term)
    }

    pub fn process_heartbeat(&mut self, request: ClusterMessage<HeartbeatRequest>) -> Result<()> {
        let previous_term = self
            .term
            .fetch_max(request.metadata.term, Ordering::Relaxed);
        if previous_term > request.metadata.term {
            return Err(anyhow::anyhow!(
                "Request's heartbeat term is lower than node's term"
            ));
        }
        if self.state.leader_id.is_none() || previous_term < request.metadata.term {
            self.set_leader(request.node_id().clone())
        }
        let node_last_entry = self.log.get_last_change().unwrap_or_default();
        if node_last_entry < request.last_change {
            let cluster_nodes = Arc::clone(&self.cluster_nodes);
            let log = Arc::clone(&self.log);
            let term = Arc::clone(&self.term);
            let Some(leader_id) = self.state.leader_id.clone() else {
                return Err(anyhow!("Unexpected state. Leader ID is None"));
            };
            let replication = LogReplicationJob::new(
                cluster_nodes,
                log,
                node_last_entry,
                term,
                self.node_id.clone(),
                leader_id,
                Self::CHUNK_SIZE,
            );
            self.job_registry.register_and_start(Arc::new(replication));
        }
        Ok(())
    }

    async fn replicate_log(_since: DateTime<Utc>) {
        todo!()
    }

    // TODO change Option enum to custom MasterKey, where MasterKey::Key instance can be created exclusively by crate
    #[inline]
    fn _verify_leader(&self, leader: Option<NodeId>) -> Result<()> {
        if leader.is_none() || self.state.leader_id == leader {
            Ok(())
        } else {
            Err(anyhow!(
                "Verification failed. Node's leader_id = [{:?}], request's leader_id = [{:?}]",
                self.state.leader_id.as_ref(),
                leader
            ))
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

    pub fn handle_join(
        &self,
        _request: ClusterMessage<JoinRequest>,
    ) -> ClusterMessage<JoinResponse> {
        self._join_response(JoinResponse::Rejected {
            reason: "election in progress; retry another seed".to_owned(),
        })
    }

    pub async fn start_election(self) -> Result<State<Leader>, State<Follower>> {
        self.term.fetch_add(1, Ordering::Relaxed);
        let request = ClusterMessage::new(
            self.node_id.clone(),
            self.term.load(Ordering::Relaxed),
            VoteRequest,
        );
        let result = timeout(MAX_TIMEOUT, self.cluster_nodes.ask_for_vote(request)).await;
        match result {
            Ok(Ok(_)) => Ok(self.into_leader()),
            Ok(Err(_)) | Err(_) => Err(self._into(Follower { leader_id: None })),
        }
    }

    pub fn vote(
        &self,
        candidate_id: NodeId,
        proposed_term: u64,
    ) -> Result<ClusterMessage<VoteResponse>> {
        self._vote(candidate_id, proposed_term)
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
        let request = HeartbeatRequest {
            timestamp: Utc::now(),
            last_change: self.log.get_last_change().unwrap_or_default(),
        };
        let message = ClusterMessage::new(
            self.node_id.clone(),
            self.term.load(Ordering::Relaxed),
            request,
        );
        timeout(
            Duration::from_secs(2),
            self.cluster_nodes.heartbeat(message),
        )
        .await?
    }

    /// Admits a node as a learner and replicates the new configuration.
    ///
    /// The learner starts receiving log entries immediately, but it does not vote until
    /// [`State::<Leader>::promote_member`] is called after it has caught up.
    pub async fn handle_join(
        &self,
        request: ClusterMessage<JoinRequest>,
    ) -> Result<ClusterMessage<JoinResponse>> {
        let payload = &request.payload;
        let current = self.configuration();

        if payload.cluster_id != current.cluster_id {
            return Ok(self._join_response(JoinResponse::Rejected {
                reason: format!(
                    "cluster [{}] does not match local cluster [{}]",
                    payload.cluster_id, current.cluster_id
                ),
            }));
        }
        if payload.node_id == self.node_id {
            return Ok(self._join_response(JoinResponse::Rejected {
                reason: "a node cannot join itself".to_owned(),
            }));
        }

        if let Some(member) = current.get(&payload.node_id) {
            if member.address != payload.address {
                return Ok(self._join_response(JoinResponse::Rejected {
                    reason: format!(
                        "node [{}] is already a member at [{}]; address changes require remove + add",
                        payload.node_id, member.address
                    ),
                }));
            }
            return Ok(self._join_response(JoinResponse::Accepted {
                leader_id: self.node_id.clone(),
                configuration: (*current).clone(),
                voter: member.is_voter(),
            }));
        }

        let next = self
            .cluster_nodes
            .change_configuration(
                ConfigChange::AddLearner {
                    node_id: payload.node_id.clone(),
                    address: payload.address.clone(),
                },
                self.term(),
            )
            .await?;

        Ok(self._join_response(JoinResponse::Accepted {
            leader_id: self.node_id.clone(),
            configuration: (*next).clone(),
            voter: false,
        }))
    }

    /// Promotes a caught-up learner to a voter.
    pub async fn promote_member(&self, node_id: NodeId) -> Result<Arc<ClusterConfiguration>> {
        if node_id == self.node_id {
            bail!("the leader is already a voter");
        }
        if !self.configuration().is_learner(&node_id) {
            bail!("node [{node_id}] is not a learner");
        }
        self.cluster_nodes
            .change_configuration(ConfigChange::Promote { node_id }, self.term())
            .await
    }

    /// Removes a member and replicates the new configuration.
    pub async fn remove_member(&self, node_id: NodeId) -> Result<Arc<ClusterConfiguration>> {
        if node_id == self.node_id {
            bail!("the leader must step down before removing itself");
        }
        self.cluster_nodes
            .change_configuration(ConfigChange::Remove { node_id }, self.term())
            .await
    }
}
