//! Quorum and broadcast semantics of [`rafty::remote_node::ClusterClient`].

mod support;

use chrono::Utc;
use rafty::transport::model::ClusterMessage;
use rafty::transport::model::command::{Command, Payload};
use rafty::transport::model::common::HeartbeatRequest;
use rafty::transport::model::log::LogReplicationRequest;
use rafty::transport::model::vote::VoteRequest;
use support::*;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::timeout;

fn heartbeat(from: &str) -> ClusterMessage<HeartbeatRequest> {
    ClusterMessage::new(
        node_id(from),
        1,
        HeartbeatRequest {
            timestamp: Utc::now(),
            last_change: Default::default(),
        },
    )
}

fn write() -> ClusterMessage<Command> {
    ClusterMessage::new(
        node_id("node1"),
        1,
        Command::Set(key("k"), Payload::Integer(1), Utc::now()),
    )
}

#[tokio::test]
async fn empty_cluster_reach_quorum_with_itself() {
    let client = cluster_client("node1", &[]);

    assert!(client.heartbeat(heartbeat("node1")).await.is_ok());
    assert!(client
        .ask_for_vote(ClusterMessage::new(node_id("node1"), 1, VoteRequest))
        .await
        .is_ok());
    assert!(client.push(write()).await.is_ok());
}

#[tokio::test]
// #[ignore = "known bug: a single-node cluster has an empty peer set, so quorum is never reached"]
async fn single_node_cluster_can_reach_quorum() {
    let client = cluster_client("node1", &[]);

    assert!(client.heartbeat(heartbeat("node1")).await.is_ok());
    assert!(client
        .ask_for_vote(ClusterMessage::new(node_id("node1"), 1, VoteRequest))
        .await
        .is_ok());
}

#[tokio::test]
async fn a_single_peer_is_enough_for_a_two_node_cluster() {
    let peer = Arc::new(Counters::default());
    let client = cluster_client("node1", &[("node2", peer.clone(), Behavior::approve())]);

    client.heartbeat(heartbeat("node1")).await.unwrap();

    assert_eq!(peer.heartbeats.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn heartbeat_reaches_every_peer() {
    let peer_a = Arc::new(Counters::default());
    let peer_b = Arc::new(Counters::default());
    let peer_c = Arc::new(Counters::default());
    let client = cluster_client(
        "node1",
        &[
            ("node2", peer_a.clone(), Behavior::approve()),
            ("node3", peer_b.clone(), Behavior::approve()),
            ("node4", peer_c.clone(), Behavior::approve()),
        ],
    );

    client.heartbeat(heartbeat("node1")).await.unwrap();

    assert_eq!(peer_a.heartbeats.load(Ordering::SeqCst), 1);
    assert_eq!(peer_b.heartbeats.load(Ordering::SeqCst), 1);
    assert_eq!(peer_c.heartbeats.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn vote_quorum_ignores_no_votes() {
    let peer = Arc::new(Counters::default());
    let peer_2 = Arc::new(Counters::default());
    let client = cluster_client(
        "node1",
        &[
            ("node2", peer.clone(), Behavior::reject()),
            ("node3", peer_2.clone(), Behavior::reject()),
        ],
    );

    let result = client
        .ask_for_vote(ClusterMessage::new(node_id("node1"), 1, VoteRequest))
        .await;

    assert!(result.is_err(), "a candidate must not win an election without yes-votes");
}

#[tokio::test]
#[ignore = "local node should be absent in configuration"]
async fn the_local_node_is_never_contacted() {
    let local = Arc::new(Counters::default());
    let peer = Arc::new(Counters::default());
    let client = cluster_client(
        "node1",
        &[
            ("node1", local.clone(), Behavior::approve()),
            ("node2", peer.clone(), Behavior::approve()),
        ],
    );

    client.heartbeat(heartbeat("node1")).await.unwrap();

    assert_eq!(local.total(), 0);
    assert_eq!(peer.heartbeats.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn quorum_does_not_wait_for_a_hung_peer() {
    let healthy = Arc::new(Counters::default());
    let hung = Arc::new(Counters::default());
    let client = cluster_client(
        "node1",
        &[
            ("node2", healthy.clone(), Behavior::approve()),
            ("node3", hung.clone(), Behavior::hanging()),
        ],
    );

    let result = timeout(Duration::from_millis(100), client.push(write())).await;

    assert!(result.is_ok(), "a hung peer must not block an already-formed quorum");
}

#[tokio::test]
async fn five_node_quorum_needs_two_remote_votes() {
    let approval_a = Arc::new(Counters::default());
    let approval_b = Arc::new(Counters::default());
    let hung_a = Arc::new(Counters::default());
    let hung_b = Arc::new(Counters::default());
    let client = cluster_client(
        "node1",
        &[
            ("node2", approval_a.clone(), Behavior::approve()),
            ("node3", approval_b.clone(), Behavior::approve()),
            ("node4", hung_a.clone(), Behavior::hanging()),
            ("node5", hung_b.clone(), Behavior::hanging()),
        ],
    );

    let result = timeout(Duration::from_millis(100), client.push(write())).await;

    assert!(result.is_ok(), "two remote acknowledgements plus the leader form a majority of five");
    assert_eq!(approval_a.payloads.load(Ordering::SeqCst), 1);
    assert_eq!(approval_b.payloads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
// #[ignore = "known bug: broadcast_call has no timeout, so it waits forever when quorum is impossible"]
async fn broadcast_without_quorum_terminates() {
    let approvals: Vec<_> = (0..3).map(|_| Arc::new(Counters::default())).collect();
    let hung_a = Arc::new(Counters::default());
    let hung_b = Arc::new(Counters::default());
    let hung_c = Arc::new(Counters::default());
    let client = cluster_client(
        "node1",
        &[
            ("node2", approvals[0].clone(), Behavior::approve()),
            ("node3", hung_a, Behavior::hanging()),
            ("node4", hung_b, Behavior::hanging()),
            ("node5", hung_c, Behavior::hanging()),
        ],
    );

    let result = timeout(Duration::from_millis(100), client.push(write())).await;

    assert!(result.is_ok(), "an unreachable quorum must fail fast instead of hanging");
    assert!(result.unwrap().is_err());
}

#[tokio::test]
async fn ask_for_log_routes_to_the_node_named_in_the_params() {
    let leader = Arc::new(Counters::default());
    let client = cluster_client("follower", &[("leader", leader.clone(), Behavior::approve())]);

    let response = client
        .ask_for_log(node_id("leader"), ClusterMessage::new(
                    node_id("follower"),
                    1,
                    LogReplicationRequest {
                        cursor_from: Utc::now(),
                        chunk_size: 10,
                    },
                ),
        )
        .await
        .expect("the leader is a known peer");

    assert_eq!(leader.log_requests.load(Ordering::SeqCst), 1);
    assert!(response.commands.is_empty());
}

#[tokio::test]
async fn ask_for_log_fails_for_an_unknown_node() {
    let client = cluster_client("follower", &[]);

    let result = client
        .ask_for_log(node_id("leader"), ClusterMessage::new(
                    node_id("follower"),
                    1,
                    LogReplicationRequest {
                        cursor_from: Utc::now(),
                        chunk_size: 10,
                    },
                ),
        )
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn ask_for_log_targets_the_request_node_id_not_the_local_node() {
    let leader = Arc::new(Counters::default());
    let client = cluster_client("follower", &[("leader", leader.clone(), Behavior::approve())]);

    let result = client
        .ask_for_log(node_id("leader"), ClusterMessage::new(
                    node_id("follower"),
                    1,
                    LogReplicationRequest {
                        cursor_from: Utc::now(),
                        chunk_size: 10,
                    },
                ),
        )
        .await;

    assert!(result.is_ok(), "the caller must call the leader specified in the first param");
    assert_eq!(leader.log_requests.load(Ordering::SeqCst), 1);
}
