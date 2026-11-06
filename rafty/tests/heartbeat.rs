//! `Follower::process_heartbeat`: leader discovery, term handling and replication scheduling.

mod support;

use chrono::{DateTime, Utc};
use rafty::job::registry::JobKey;
use rafty::node_state::Node;
use rafty::transport::model::ClusterMessage;
use rafty::transport::model::common::HeartbeatRequest;
use support::*;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

fn heartbeat(from: &str, term: u64, last_change: DateTime<Utc>) -> ClusterMessage<HeartbeatRequest> {
    ClusterMessage::new(
        node_id(from),
        term,
        HeartbeatRequest {
            timestamp: Utc::now(),
            last_change,
        },
    )
}

#[tokio::test]
async fn first_heartbeat_records_its_sender_as_leader() {
    let mut state = follower_state(
        "node1",
        FakeLog::without_last_change(),
        Arc::new(FakeStorage::default()),
        cluster_client("node1", &[]),
        test_registry(),
    );

    state.process_heartbeat(heartbeat("leader-a", 1, Default::default())).unwrap();

    assert!(Node::Follower(state).try_follower(Some(node_id("leader-a"))).is_ok());
}

#[tokio::test]
async fn heartbeat_from_the_known_leader_keeps_the_leader() {
    let mut state = follower_state(
        "node1",
        FakeLog::without_last_change(),
        Arc::new(FakeStorage::default()),
        cluster_client("node1", &[]),
        test_registry(),
    );
    state.set_leader(node_id("leader-a"));

    state.process_heartbeat(heartbeat("leader-a", 3, Default::default())).unwrap();

    assert!(Node::Follower(state).try_follower(Some(node_id("leader-a"))).is_ok());
}

#[tokio::test]
// #[ignore = "known bug: the heartbeat term is never merged into the node's term"]
async fn heartbeat_advances_the_local_term() {
    let mut state = follower_state(
        "node1",
        FakeLog::without_last_change(),
        Arc::new(FakeStorage::default()),
        cluster_client("node1", &[]),
        test_registry(),
    );

    state.process_heartbeat(heartbeat("leader-a", 7, Default::default())).unwrap();

    assert_eq!(state.term(), 7);
}

#[tokio::test]
// #[ignore = "known bug: the leader is only set when unknown, so a stale leader survives a new election"]
async fn heartbeat_from_a_new_leader_replaces_the_stale_one() {
    let mut state = follower_state(
        "node1",
        FakeLog::without_last_change(),
        Arc::new(FakeStorage::default()),
        cluster_client("node1", &[]),
        test_registry(),
    );
    state.set_leader(node_id("old-leader"));

    state.process_heartbeat(heartbeat("new-leader", 8, Default::default())).unwrap();

    assert!(Node::Follower(state).try_follower(Some(node_id("new-leader"))).is_ok());
}

#[tokio::test]
// #[ignore = "known bug: heartbeats carry no term validation, so a stale leader is accepted"]
async fn heartbeat_with_a_lower_term_is_rejected() {
    let mut state = follower_state(
        "node1",
        FakeLog::without_last_change(),
        Arc::new(FakeStorage::default()),
        cluster_client("node1", &[]),
        test_registry(),
    );

    assert!(state.process_heartbeat(heartbeat("leader", 10, Default::default())).is_ok());
    assert!(state.process_heartbeat(heartbeat("stale-leader", 1, Default::default())).is_err());

    assert!(Node::Follower(state).try_follower(Some(node_id("leader"))).is_ok());
}

#[tokio::test]
async fn follower_behind_the_leader_schedules_log_replication() {
    let now = Utc::now();
    let registry = test_registry();
    let log = FakeLog::with_last_change(now);
    let mut state = follower_state(
        "node1",
        log,
        Arc::new(FakeStorage::default()),
        cluster_client("node1", &[]),
        registry.clone(),
    );

    state
        .process_heartbeat(heartbeat("leader", 1, now + Duration::from_millis(5)))
        .unwrap();

    assert!(registry.is_registered(JobKey::LogReplication));
    assert!(registry.is_running(JobKey::LogReplication));
}

#[tokio::test]
async fn follower_at_the_leader_cursor_does_not_schedule_replication() {
    let now = Utc::now();
    let registry = test_registry();
    let log = FakeLog::with_last_change(now);
    let mut state = follower_state(
        "node1",
        log,
        Arc::new(FakeStorage::default()),
        cluster_client("node1", &[]),
        registry.clone(),
    );

    state.process_heartbeat(heartbeat("leader", 1, now)).unwrap();

    assert!(!registry.is_registered(JobKey::LogReplication));
    assert!(!registry.is_running(JobKey::LogReplication));
}

#[tokio::test]
async fn follower_ahead_of_the_leader_does_not_replicate() {
    let now = Utc::now();
    let registry = test_registry();
    let mut state = follower_state(
        "node1",
        FakeLog::with_last_change(now + Duration::from_millis(10)),
        Arc::new(FakeStorage::default()),
        cluster_client("node1", &[]),
        registry.clone(),
    );

    state.process_heartbeat(heartbeat("leader", 1, now)).unwrap();

    assert!(!registry.is_running(JobKey::LogReplication));
}

#[tokio::test]
async fn heartbeat_without_a_cursor_is_a_noop() {
    let registry = test_registry();
    let mut state = follower_state(
        "node1",
        FakeLog::empty(),
        Arc::new(FakeStorage::default()),
        cluster_client("node1", &[]),
        registry.clone(),
    );

    state.process_heartbeat(heartbeat("leader", 1, Default::default())).unwrap();

    assert!(!registry.is_running(JobKey::LogReplication));
}

#[tokio::test]
async fn fresh_follower_with_an_empty_log_starts_replication() {
    let registry = test_registry();
    let mut state = follower_state(
        "node1",
        FakeLog::without_last_change(),
        Arc::new(FakeStorage::default()),
        cluster_client("node1", &[]),
        registry.clone(),
    );

    state
        .process_heartbeat(heartbeat("leader", 1, Utc::now()))
        .unwrap();

    assert!(registry.is_running(JobKey::LogReplication));
}

#[tokio::test]
async fn replication_request_is_addressed_to_the_leader_node() {
    let now = Utc::now();
    let leader = Arc::new(Counters::default());
    let registry = test_registry();
    let mut state = follower_state(
        "node1",
        FakeLog::with_last_change(now),
        Arc::new(FakeStorage::default()),
        cluster_client("node1", &[("leader", leader.clone(), Behavior::approve())],),
        registry,
    );

    state
        .process_heartbeat(heartbeat("leader", 1, now + Duration::from_millis(5)))
        .unwrap();
    settle().await;

    assert_eq!(leader.log_requests.load(Ordering::SeqCst), 1);
}
