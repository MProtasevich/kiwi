//! Shared test doubles for the `rafty` integration tests.
//!
//! Everything here is deliberately small and inspectable: the fakes record what the node
//! asked them to do, can be told to fail, and can simulate slow or unresponsive peers.

#![allow(dead_code)]

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rafty::job::registry::JobRegistry;
use rafty::log::CommandLog;
use rafty::node_state::{NodeId, State, states::Follower};
use rafty::remote_node::{CandidateClient, ClusterClient, FollowerClient, LeaderClient, RemoteNode};
use rafty::storage::Storage;
use rafty::transport::model::ClusterMessage;
use rafty::transport::model::Key;
use rafty::transport::model::command::{Command, Payload, TimeAware};
use rafty::transport::model::common::{
    CommitRequest, CommitResponse, HeartbeatRequest, HeartbeatResponse,
};
use rafty::transport::model::log::{LogReplicationRequest, LogReplicationResponse};
use rafty::transport::model::vote::{VoteRequest, VoteResponse};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub fn node_id(value: &str) -> NodeId {
    NodeId::new(value)
}

pub fn key(value: &str) -> Key {
    Key::new(value)
}

/// Yields to the runtime so that spawned jobs get a chance to run.
pub async fn settle() {
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
}

// ---------------------------------------------------------------------------
// Command log
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct FakeLog {
    commands: Mutex<Vec<(DateTime<Utc>, Command)>>,
    fail_push: AtomicBool,
    fail_push_all: AtomicBool,
    fail_commit: AtomicBool,
    /// `None` -> derive from the stored commands; `Some(override)` -> return it verbatim.
    last_change_override: Mutex<Option<Option<DateTime<Utc>>>>,
}

impl Default for FakeLog {
    fn default() -> Self {
        Self {
            commands: Mutex::new(Vec::new()),
            fail_push: AtomicBool::new(false),
            fail_push_all: AtomicBool::new(false),
            fail_commit: AtomicBool::new(false),
            last_change_override: Mutex::new(None),
        }
    }
}

impl FakeLog {
    pub fn empty() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn with_last_change(timestamp: DateTime<Utc>) -> Arc<Self> {
        let log = Self::default();
        *log.last_change_override.lock().unwrap() = Some(Some(timestamp));
        Arc::new(log)
    }

    pub fn without_last_change() -> Arc<Self> {
        let log = Self::default();
        *log.last_change_override.lock().unwrap() = Some(None);
        Arc::new(log)
    }

    pub fn fail_push(&self) -> &Self {
        self.fail_push.store(true, Ordering::SeqCst);
        self
    }

    pub fn fail_push_all(&self) -> &Self {
        self.fail_push_all.store(true, Ordering::SeqCst);
        self
    }

    pub fn fail_commit(&self) -> &Self {
        self.fail_commit.store(true, Ordering::SeqCst);
        self
    }

    pub fn push_count(&self) -> usize {
        self.commands.lock().unwrap().len()
    }

    pub fn commands(&self) -> Vec<Command> {
        self.commands
            .lock()
            .unwrap()
            .iter()
            .map(|(_, command)| command.clone())
            .collect()
    }

    pub fn timestamps(&self) -> Vec<DateTime<Utc>> {
        self.commands
            .lock()
            .unwrap()
            .iter()
            .map(|(timestamp, _)| *timestamp)
            .collect()
    }
}

impl CommandLog for FakeLog {
    fn push(&self, cmd: Command) -> Result<()> {
        if self.fail_push.load(Ordering::SeqCst) {
            return Err(anyhow!("injected push failure"));
        }
        let timestamp = cmd.timestamp().unwrap_or_else(Utc::now);
        self.commands.lock().unwrap().push((timestamp, cmd));
        Ok(())
    }

    fn push_all(&self, cmds: Vec<Command>) -> Result<()> {
        if self.fail_push_all.load(Ordering::SeqCst) {
            return Err(anyhow!("injected push_all failure"));
        }
        for cmd in cmds {
            self.push(cmd)?;
        }
        Ok(())
    }

    fn commit(&self, key: Key) -> Result<Payload> {
        if self.fail_commit.load(Ordering::SeqCst) {
            return Err(anyhow!("injected commit failure"));
        }
        self.commands
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find_map(|(_, command)| match command {
                Command::Set(stored_key, payload, _) if *stored_key == key => Some(payload.clone()),
                _ => None,
            })
            .ok_or_else(|| anyhow!("no value logged for key [{key:?}]"))
    }

    fn get_commands(&self, since: DateTime<Utc>) -> Result<Vec<Command>> {
        Ok(self
            .commands
            .lock()
            .unwrap()
            .iter()
            .filter(|(timestamp, _)| *timestamp > since)
            .map(|(_, command)| command.clone())
            .collect())
    }

    fn get_last_change(&self) -> Option<DateTime<Utc>> {
        if let Some(overridden) = *self.last_change_override.lock().unwrap() {
            return overridden;
        }
        self.commands.lock().unwrap().last().map(|(timestamp, _)| *timestamp)
    }
}

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct FakeStorage {
    values: Mutex<HashMap<Key, Payload>>,
    pushes: AtomicUsize,
    fail: AtomicBool,
}

impl FakeStorage {
    pub fn fail_push(&self) -> &Self {
        self.fail.store(true, Ordering::SeqCst);
        self
    }

    pub fn push_count(&self) -> usize {
        self.pushes.load(Ordering::SeqCst)
    }

    pub fn is_empty(&self) -> bool {
        self.values.lock().unwrap().is_empty()
    }
}

impl Storage for FakeStorage {
    fn push(&self, key: Key, payload: Payload) -> Result<()> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(anyhow!("injected storage failure"));
        }
        self.values.lock().unwrap().insert(key, payload);
        self.pushes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn get(&self, key: Key) -> Result<Option<Payload>> {
        Ok(self.values.lock().unwrap().get(&key).cloned())
    }
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct Counters {
    pub heartbeats: AtomicUsize,
    pub votes: AtomicUsize,
    pub log_requests: AtomicUsize,
    pub payloads: AtomicUsize,
    pub commits: AtomicUsize,
    terms: Mutex<Vec<u64>>,
}

impl Counters {
    pub fn record_term(&self, term: u64) {
        self.terms.lock().unwrap().push(term);
    }

    pub fn terms(&self) -> Vec<u64> {
        self.terms.lock().unwrap().clone()
    }

    pub fn total(&self) -> usize {
        self.heartbeats.load(Ordering::SeqCst)
            + self.votes.load(Ordering::SeqCst)
            + self.log_requests.load(Ordering::SeqCst)
            + self.payloads.load(Ordering::SeqCst)
            + self.commits.load(Ordering::SeqCst)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Behavior {
    pub voted: bool,
    pub delay: Option<Duration>,
    pub hang: bool,
}

impl Default for Behavior {
    fn default() -> Self {
        Self {
            voted: true,
            delay: None,
            hang: false,
        }
    }
}

impl Behavior {
    pub fn approve() -> Self {
        Self::default()
    }

    pub fn reject() -> Self {
        Self {
            voted: false,
            ..Self::default()
        }
    }

    pub fn hanging() -> Self {
        Self {
            hang: true,
            ..Self::default()
        }
    }

    pub fn delayed(delay: Duration) -> Self {
        Self {
            delay: Some(delay),
            ..Self::default()
        }
    }
}

#[derive(Debug)]
pub struct Recorder {
    pub id: NodeId,
    pub counters: Arc<Counters>,
    pub behavior: Behavior,
}

impl Recorder {
    async fn pause(&self) {
        if let Some(delay) = self.behavior.delay {
            tokio::time::sleep(delay).await;
        }
        if self.behavior.hang {
            futures::future::pending::<()>().await;
        }
    }
}

#[async_trait]
impl CandidateClient for Recorder {
    async fn ask_for_vote(&self, request: &ClusterMessage<VoteRequest>) -> ClusterMessage<VoteResponse> {
        self.counters.votes.fetch_add(1, Ordering::SeqCst);
        self.counters.record_term(request.metadata.term);
        self.pause().await;
        ClusterMessage::new(
            self.id.clone(),
            request.metadata.term,
            VoteResponse { voted: self.behavior.voted },
        )
    }
}

#[async_trait]
impl FollowerClient for Recorder {
    async fn request_log(&self, request: ClusterMessage<LogReplicationRequest>) -> ClusterMessage<LogReplicationResponse> {
        self.counters.log_requests.fetch_add(1, Ordering::SeqCst);
        self.counters.record_term(request.metadata.term);
        self.pause().await;
        ClusterMessage::new(
            self.id.clone(),
            request.metadata.term,
            LogReplicationResponse {
                cursor_id: request.payload.cursor_from,
                commands: Vec::new(),
            },
        )
    }
}

#[async_trait]
impl LeaderClient for Recorder {
    async fn send_payload(&self, payload: &ClusterMessage<Command>) -> ClusterMessage<()> {
        self.counters.payloads.fetch_add(1, Ordering::SeqCst);
        self.counters.record_term(payload.metadata.term);
        self.pause().await;
        ClusterMessage::new(self.id.clone(), payload.metadata.term, ())
    }

    async fn commit(&self, request: &ClusterMessage<CommitRequest>) -> ClusterMessage<CommitResponse> {
        self.counters.commits.fetch_add(1, Ordering::SeqCst);
        self.counters.record_term(request.metadata.term);
        self.pause().await;
        ClusterMessage::new(
            self.id.clone(),
            request.metadata.term,
            CommitResponse { key: request.payload.key.clone() },
        )
    }

    async fn heartbeat(&self, request: &ClusterMessage<HeartbeatRequest>) -> ClusterMessage<HeartbeatResponse> {
        self.counters.heartbeats.fetch_add(1, Ordering::SeqCst);
        self.counters.record_term(request.metadata.term);
        self.pause().await;
        ClusterMessage::new(
            self.id.clone(),
            request.metadata.term,
            HeartbeatResponse { timestamp: Utc::now() },
        )
    }
}

pub fn remote_node(id: &str, counters: Arc<Counters>, behavior: Behavior) -> RemoteNode {
    let recorder = Arc::new(Recorder {
        id: node_id(id),
        counters,
        behavior,
    });
    RemoteNode::new(recorder.clone(), recorder.clone(), recorder)
}

/// Builds a client for `local` whose peer set is exactly `peers` (self excluded).
pub fn cluster_client(local: &str, peers: &[(&str, Arc<Counters>, Behavior)]) -> Arc<ClusterClient> {
    let mut nodes = HashMap::new();
    for (id, counters, behavior) in peers {
        nodes.insert(node_id(id), remote_node(id, Arc::clone(counters), *behavior));
    }
    Arc::new(ClusterClient::new(node_id(local), Arc::new(nodes), Duration::from_millis(40)))
}

pub fn test_registry() -> Arc<JobRegistry> {
    Arc::new(JobRegistry::from_current())
}

pub fn follower_state(
    id: &str,
    log: Arc<dyn CommandLog>,
    storage: Arc<dyn Storage>,
    client: Arc<ClusterClient>,
    registry: Arc<JobRegistry>,
) -> State<Follower> {
    State::<Follower>::new(node_id(id), client, log, storage, registry)
}

// ---------------------------------------------------------------------------
// Scripted log replication source
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct ScriptedFollower {
    pub id: NodeId,
    requests: Mutex<Vec<LogReplicationRequest>>,
    responses: Mutex<VecDeque<LogReplicationResponse>>,
}

impl ScriptedFollower {
    pub fn new(id: &str) -> Arc<Self> {
        Arc::new(Self {
            id: node_id(id),
            requests: Mutex::new(Vec::new()),
            responses: Mutex::new(VecDeque::new()),
        })
    }

    pub fn enqueue(&self, cursor_id: DateTime<Utc>, commands: Vec<Command>) {
        self.responses
            .lock()
            .unwrap()
            .push_back(LogReplicationResponse { cursor_id, commands });
    }

    pub fn requests(&self) -> Vec<LogReplicationRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait]
impl FollowerClient for ScriptedFollower {
    async fn request_log(&self, request: ClusterMessage<LogReplicationRequest>) -> ClusterMessage<LogReplicationResponse> {
        self.requests.lock().unwrap().push(request.payload.clone());
        let response = self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(LogReplicationResponse {
                cursor_id: request.payload.cursor_from,
                commands: Vec::new(),
            });
        ClusterMessage::new(self.id.clone(), request.metadata.term, response)
    }
}

#[async_trait]
impl CandidateClient for ScriptedFollower {
    async fn ask_for_vote(&self, _: &ClusterMessage<VoteRequest>) -> ClusterMessage<VoteResponse> {
        unreachable!("scripted follower is only used for log replication")
    }
}

#[async_trait]
impl LeaderClient for ScriptedFollower {
    async fn send_payload(&self, _: &ClusterMessage<Command>) -> ClusterMessage<()> {
        unreachable!("scripted follower is only used for log replication")
    }

    async fn commit(&self, _: &ClusterMessage<CommitRequest>) -> ClusterMessage<CommitResponse> {
        unreachable!("scripted follower is only used for log replication")
    }

    async fn heartbeat(&self, _: &ClusterMessage<HeartbeatRequest>) -> ClusterMessage<HeartbeatResponse> {
        unreachable!("scripted follower is only used for log replication")
    }
}

pub fn scripted_client(leader: &str, scripted: Arc<ScriptedFollower>) -> Arc<ClusterClient> {
    let node = RemoteNode::new(scripted.clone(), scripted.clone(), scripted);
    let nodes = HashMap::from([(node_id(leader), node)]);
    Arc::new(ClusterClient::new(node_id("follower"), Arc::new(nodes), Duration::from_millis(40)))
}
