//! Time-driven jobs, driven by [`JobRegistry`].
//!
//! The registry owns the timers; each job only reacts when its tick fires.

mod support;

use arc_swap::ArcSwap;
use rafty::job::election_countdown::ElectionCountdownJob;
use rafty::job::heartbeat_polling::HeartbeatPollingJob;
use rafty::job::registry::{JobKey, JobRegistry};
use rafty::node_state::{Node, State, states::Follower, states::Leader};
use support::*;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

const PERIOD: Duration = Duration::from_millis(150);
const HEARTBEAT_PERIOD: Duration = Duration::from_millis(100);

fn swap(state: State<Follower>) -> Arc<ArcSwap<Node>> {
    Arc::new(ArcSwap::from_pointee(Node::Follower(state)))
}

fn follower_with_peer(peer: &Arc<Counters>, behavior: Behavior) -> State<Follower> {
    follower_state(
        "node1",
        FakeLog::empty(),
        Arc::new(FakeStorage::default()),
        cluster_client("node1", &[("node2", peer.clone(), behavior)]),
        test_registry(),
    )
}

fn start_countdown(registry: &JobRegistry, node: Arc<ArcSwap<Node>>) {
    registry.register_and_start(Arc::new(ElectionCountdownJob::new(PERIOD, node)));
}

#[tokio::test(start_paused = true)]
async fn countdown_does_not_fire_before_the_period() {
    let peer = Arc::new(Counters::default());
    let node = swap(follower_with_peer(&peer, Behavior::approve()));
    let registry = JobRegistry::from_current();
    start_countdown(&registry, node.clone());
    settle().await;

    tokio::time::advance(PERIOD - Duration::from_millis(1)).await;
    settle().await;

    assert_eq!(peer.votes.load(Ordering::SeqCst), 0);
    assert!(node.load().try_leader().is_err());

    registry.stop(JobKey::HeartbeatCountdown).await;
}

#[tokio::test(start_paused = true)]
async fn countdown_starts_an_election_after_the_period() {
    let peer = Arc::new(Counters::default());
    let node = swap(follower_with_peer(&peer, Behavior::approve()));
    let registry = JobRegistry::from_current();
    start_countdown(&registry, node.clone());
    settle().await;

    tokio::time::advance(PERIOD + Duration::from_millis(1)).await;
    settle().await;

    assert_eq!(peer.votes.load(Ordering::SeqCst), 1);
    assert!(node.load().try_leader().is_ok(), "a successful election must replace the follower state");
    assert!(!registry.is_running(JobKey::HeartbeatCountdown), "a leader stops the countdown");
}

#[tokio::test(start_paused = true)]
async fn countdown_honours_the_timeout_range() {
    const MIN_TIMEOUT: Duration = Duration::from_millis(100);
    const MAX_TIMEOUT: Duration = Duration::from_millis(300);

    let peer = Arc::new(Counters::default());
    let node = swap(follower_with_peer(&peer, Behavior::approve()));
    let registry = JobRegistry::from_current();
    registry.register_and_start(Arc::new(ElectionCountdownJob::with_timeout_range(
        (MIN_TIMEOUT..=MAX_TIMEOUT).into(),
        node.clone(),
    )));
    settle().await;

    tokio::time::advance(MIN_TIMEOUT - Duration::from_millis(1)).await;
    settle().await;
    assert_eq!(peer.votes.load(Ordering::SeqCst), 0);
    assert!(node.load().try_leader().is_err());

    tokio::time::advance(MAX_TIMEOUT - MIN_TIMEOUT + Duration::from_millis(1)).await;
    settle().await;
    assert_eq!(peer.votes.load(Ordering::SeqCst), 1);
    assert!(node.load().try_leader().is_ok());

    registry.stop(JobKey::HeartbeatCountdown).await;
}

#[tokio::test(start_paused = true)]
async fn reset_postpones_the_election() {
    let peer = Arc::new(Counters::default());
    let node = swap(follower_with_peer(&peer, Behavior::approve()));
    let registry = JobRegistry::from_current();
    start_countdown(&registry, node.clone());
    settle().await;

    tokio::time::advance(Duration::from_millis(100)).await;
    registry.reset(JobKey::HeartbeatCountdown);
    settle().await;
    tokio::time::advance(Duration::from_millis(100)).await;
    settle().await;

    assert_eq!(peer.votes.load(Ordering::SeqCst), 0, "a heartbeat must restart the countdown");

    tokio::time::advance(Duration::from_millis(60)).await;
    settle().await;

    assert_eq!(peer.votes.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn repeated_resets_are_coalesced() {
    let peer = Arc::new(Counters::default());
    let node = swap(follower_with_peer(&peer, Behavior::approve()));
    let registry = JobRegistry::from_current();
    start_countdown(&registry, node.clone());
    settle().await;

    registry.reset(JobKey::HeartbeatCountdown);
    registry.reset(JobKey::HeartbeatCountdown);
    registry.reset(JobKey::HeartbeatCountdown);
    settle().await;
    tokio::time::advance(PERIOD - Duration::from_millis(1)).await;
    settle().await;

    assert_eq!(peer.votes.load(Ordering::SeqCst), 0);

    registry.stop(JobKey::HeartbeatCountdown).await;
}

#[tokio::test(start_paused = true)]
async fn stop_prevents_the_election() {
    let peer = Arc::new(Counters::default());
    let node = swap(follower_with_peer(&peer, Behavior::approve()));
    let registry = JobRegistry::from_current();
    start_countdown(&registry, node.clone());
    settle().await;

    registry.stop(JobKey::HeartbeatCountdown).await;
    settle().await;
    tokio::time::advance(PERIOD * 3).await;
    settle().await;

    assert_eq!(peer.votes.load(Ordering::SeqCst), 0);
    assert!(node.load().try_leader().is_err());
}

#[tokio::test(start_paused = true)]
async fn countdown_does_not_elect_while_already_leader() {
    let peer = Arc::new(Counters::default());
    let leader = follower_with_peer(&peer, Behavior::approve()).into_candidate().into_leader();
    let node = Arc::new(ArcSwap::from_pointee(Node::Leader(leader)));
    let registry = JobRegistry::from_current();
    start_countdown(&registry, node.clone());
    settle().await;

    tokio::time::advance(PERIOD + Duration::from_millis(1)).await;
    settle().await;

    assert_eq!(peer.votes.load(Ordering::SeqCst), 0);
    assert!(node.load().try_leader().is_ok());

    registry.stop(JobKey::HeartbeatCountdown).await;
}

#[tokio::test(start_paused = true)]
async fn failed_election_is_retried() {
    let peer = Arc::new(Counters::default());
    let node = swap(follower_with_peer(&peer, Behavior::delayed(Duration::from_secs(1))));
    let registry = JobRegistry::from_current();
    start_countdown(&registry, node.clone());
    settle().await;

    tokio::time::advance(PERIOD + Duration::from_millis(1)).await;
    settle().await;
    assert_eq!(peer.votes.load(Ordering::SeqCst), 1);

    // Let the 500ms election timeout fire, then wait for another period.
    tokio::time::advance(Duration::from_millis(501)).await;
    settle().await;
    tokio::time::advance(PERIOD + Duration::from_millis(1)).await;
    settle().await;

    assert_eq!(peer.votes.load(Ordering::SeqCst), 2, "the watchdog must try again");

    registry.stop(JobKey::HeartbeatCountdown).await;
}

#[tokio::test(start_paused = true)]
async fn heartbeat_polling_sends_on_every_tick() {
    let peer = Arc::new(Counters::default());
    let leader = Arc::new(follower_with_peer(&peer, Behavior::approve()).into_candidate().into_leader());
    let registry = JobRegistry::from_current();
    registry.register_and_start(Arc::new(HeartbeatPollingJob::new(HEARTBEAT_PERIOD, leader)));
    settle().await;
    assert_eq!(peer.heartbeats.load(Ordering::SeqCst), 1, "the first tick is immediate");

    for tick in 2..=5 {
        tokio::time::advance(HEARTBEAT_PERIOD).await;
        settle().await;
        assert_eq!(peer.heartbeats.load(Ordering::SeqCst), tick);
    }

    registry.stop(JobKey::HeartbeatPush).await;
}

#[tokio::test(start_paused = true)]
async fn heartbeat_polling_keeps_running_without_configured_peers() {
    let leader: State<Leader> = follower_state(
        "node1",
        FakeLog::empty(),
        Arc::new(FakeStorage::default()),
        cluster_client("node1", &[]),
        test_registry(),
    )
    .into_candidate()
    .into_leader();

    assert!(
        leader.send_heartbeat().await.is_ok(),
        "an empty peer set short-circuits to a successful no-op"
    );

    let registry = JobRegistry::from_current();
    registry.register_and_start(Arc::new(HeartbeatPollingJob::new(HEARTBEAT_PERIOD, Arc::new(leader))));
    settle().await;
    tokio::time::advance(Duration::from_millis(300)).await;
    settle().await;

    assert!(registry.is_running(JobKey::HeartbeatPush), "a no-op heartbeat must not terminate polling");

    registry.stop(JobKey::HeartbeatPush).await;
}

#[tokio::test(start_paused = true)]
async fn heartbeat_polling_accepts_a_self_only_peer_set() {
    let local = Arc::new(Counters::default());
    let leader: State<Leader> = follower_state(
        "node1",
        FakeLog::empty(),
        Arc::new(FakeStorage::default()),
        cluster_client("node1", &[("node1", local.clone(), Behavior::approve())]),
        test_registry(),
    )
    .into_candidate()
    .into_leader();
    let leader = Arc::new(leader);

    assert!(
        leader.send_heartbeat().await.is_ok(),
        "a peer set containing only the local node short-circuits instead of failing"
    );
    assert_eq!(
        local.heartbeats.load(Ordering::SeqCst), 0,
        "the local node must never be contacted"
    );

    let registry = JobRegistry::from_current();
    registry.register_and_start(Arc::new(HeartbeatPollingJob::new(HEARTBEAT_PERIOD, leader)));
    settle().await;
    tokio::time::advance(HEARTBEAT_PERIOD * 3).await;
    settle().await;

    assert!(registry.is_running(JobKey::HeartbeatPush));

    registry.stop(JobKey::HeartbeatPush).await;
}

#[tokio::test(start_paused = true)]
async fn heartbeat_polling_survives_an_unreachable_peer() {
    let peer = Arc::new(Counters::default());
    let leader = Arc::new(follower_with_peer(&peer, Behavior::hanging()).into_candidate().into_leader());

    // The transport really fails: a hung peer trips the per-call timeout.
    let send = tokio::spawn({
        let leader = Arc::clone(&leader);
        async move { leader.send_heartbeat().await }
    });
    settle().await;
    // `cluster_client` gives each remote call a 40ms budget.
    tokio::time::advance(Duration::from_millis(40)).await;
    settle().await;

    assert!(
        send.await.unwrap().is_err(),
        "an unreachable peer must surface as an error"
    );

    let registry = JobRegistry::from_current();
    registry.register_and_start(Arc::new(HeartbeatPollingJob::new(HEARTBEAT_PERIOD, leader)));
    settle().await;
    tokio::time::advance(HEARTBEAT_PERIOD).await;
    settle().await;

    assert!(registry.is_running(JobKey::HeartbeatPush), "a timed-out heartbeat must not terminate polling");
    assert!(
        peer.heartbeats.load(Ordering::SeqCst) >= 2,
        "every polling attempt still reaches the transport"
    );

    registry.stop(JobKey::HeartbeatPush).await;
}

#[tokio::test(start_paused = true)]
async fn aborting_heartbeat_polling_stops_the_ticks() {
    let peer = Arc::new(Counters::default());
    let leader = Arc::new(follower_with_peer(&peer, Behavior::approve()).into_candidate().into_leader());
    let registry = JobRegistry::from_current();
    registry.register_and_start(Arc::new(HeartbeatPollingJob::new(HEARTBEAT_PERIOD, leader)));
    settle().await;

    registry.stop(JobKey::HeartbeatPush).await;
    settle().await;
    let after_abort = peer.heartbeats.load(Ordering::SeqCst);

    tokio::time::advance(Duration::from_millis(500)).await;
    settle().await;

    assert_eq!(peer.heartbeats.load(Ordering::SeqCst), after_abort);
}
