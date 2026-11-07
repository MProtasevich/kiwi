//! State machine, term and election behavior of [`rafty::node_state`].

mod support;

use rafty::node_state::{Node, State, states::Follower};
use support::*;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

fn plain_follower() -> State<Follower> {
    follower_state(
        "node1",
        FakeLog::empty(),
        Arc::new(FakeStorage::default()),
        cluster_client("node1", &[]),
        test_registry(),
    )
}

fn follower_with_peers(peers: &[(&str, Arc<Counters>, Behavior)]) -> State<Follower> {
    follower_state(
        "node1",
        FakeLog::empty(),
        Arc::new(FakeStorage::default()),
        cluster_client("node1", peers),
        test_registry(),
    )
}

/// A follower that knows `candidates` as peers, which `vote` requires before granting a vote.
fn follower_with_candidates(candidates: &[&str]) -> State<Follower> {
    let peers: Vec<(&str, Arc<Counters>, Behavior)> = candidates
        .iter()
        .map(|&id| (id, Arc::new(Counters::default()), Behavior::approve()))
        .collect();
    follower_with_peers(&peers)
}

#[tokio::test]
async fn follower_exposes_only_follower_operations() {
    let node = Node::Follower(plain_follower());

    assert!(node.try_follower(None).is_ok());
    assert!(node.try_candidate().is_err());
    assert!(node.try_leader().is_err());
}

#[tokio::test]
async fn candidate_exposes_only_candidate_operations() {
    let node = Node::Candidate(plain_follower().into_candidate());

    assert!(node.try_candidate().is_ok());
    assert!(node.try_follower(None).is_err());
    assert!(node.try_leader().is_err());
}

#[tokio::test]
async fn leader_exposes_only_leader_operations() {
    let node = Node::Leader(plain_follower().into_candidate().into_leader());

    assert!(node.try_leader().is_ok());
    assert!(node.try_follower(None).is_err());
    assert!(node.try_candidate().is_err());
}

#[tokio::test]
async fn transition_error_names_actual_and_expected_states() {
    let error = Node::Follower(plain_follower()).try_leader().unwrap_err();
    let message = format!("{error:#}");

    assert!(message.contains("Follower"), "unexpected message: {message}");
    assert!(message.contains("Leader"), "unexpected message: {message}");
}

#[tokio::test]
async fn follower_accepts_heartbeat_leader_it_already_knows() {
    let mut state = plain_follower();
    state.set_leader(node_id("leader-a"));

    let node = Node::Follower(state);
    assert!(node.try_follower(Some(node_id("leader-a"))).is_ok());
}

#[tokio::test]
async fn follower_rejects_heartbeat_leader_it_does_not_know() {
    let mut state = plain_follower();
    state.set_leader(node_id("leader-a"));

    let node = Node::Follower(state);
    assert!(node.try_follower(Some(node_id("leader-b"))).is_err());
}

#[tokio::test]
async fn try_follower_without_leader_skips_verification() {
    let mut state = plain_follower();
    state.set_leader(node_id("leader-a"));

    // `None` means "do not verify", so a caller can bypass the check entirely.
    // Hope, that's a temporary solution
    let node = Node::Follower(state);
    assert!(node.try_follower(None).is_ok());
}

#[tokio::test]
async fn transitions_preserve_node_id_and_share_the_term() {
    let follower = follower_with_candidates(&["peer"]);
    let candidate = follower.clone().into_candidate();

    assert_eq!(candidate.node_id(), follower.node_id());
    assert_eq!(candidate.term(), follower.term());

    // The term lives behind a shared pointer: bumping it through one handle is visible
    // through every other handle derived from the same node.
    let _ = follower.vote(node_id("peer"), 7);
    assert_eq!(candidate.term(), 7);

    let leader = candidate.into_leader();
    assert_eq!(leader.node_id(), follower.node_id());
    assert_eq!(leader.term(), 7);
}

#[tokio::test]
async fn candidate_and_leader_can_step_down_to_a_known_leader() {
    let candidate = plain_follower().into_candidate();
    let follower = candidate.clone().into_follower(node_id("leader-a"));
    assert!(Node::Follower(follower).try_follower(Some(node_id("leader-a"))).is_ok());

    let leader = candidate.into_leader().into_follower(node_id("leader-b"));
    assert!(Node::Follower(leader).try_follower(Some(node_id("leader-b"))).is_ok());
}

#[tokio::test]
async fn vote_from_higher_term_is_granted_and_advances_the_term() {
    let state = follower_with_candidates(&["peer"]);

    let response = state.vote(node_id("peer"), 5).unwrap();

    assert!(response.payload.voted);
    assert_eq!(state.term(), 5);
    assert_eq!(response.metadata.term, 5);
    assert_eq!(response.metadata.node_id, node_id("node1"));
}

#[tokio::test]
async fn second_vote_in_the_same_term_is_denied() {
    let state = follower_with_candidates(&["peer-a", "peer-b"]);

    assert!(state.vote(node_id("peer-a"), 5).unwrap().payload.voted);
    assert!(!state.vote(node_id("peer-b"), 5).unwrap().payload.voted);
    assert_eq!(state.term(), 5);
}

#[tokio::test]
async fn vote_from_lower_term_is_denied() {
    let state = follower_with_candidates(&["peer"]);
    let _ = state.vote(node_id("peer"), 10);

    let response = state.vote(node_id("peer"), 3).unwrap();

    assert!(!response.payload.voted);
    assert_eq!(state.term(), 10);
}

#[tokio::test]
async fn vote_response_reports_the_responders_term() {
    let state = follower_with_candidates(&["peer"]);
    let _ = state.vote(node_id("peer"), 10);

    let response = state.vote(node_id("peer"), 3).unwrap();

    assert!(!response.payload.voted);
    assert_eq!(response.metadata.term, 10, "a stale candidate must learn the current term");
}

#[tokio::test]
async fn vote_from_an_unknown_candidate_is_rejected() {
    let state = plain_follower();

    let error = state.vote(node_id("whoever"), 1).unwrap_err();
    let message = format!("{error:#}");

    assert!(message.contains("whoever"), "unexpected message: {message}");
    assert_eq!(state.term(), 0, "an unknown candidate must not advance the term");
}

#[tokio::test]
async fn candidate_vote_uses_the_same_term_bookkeeping() {
    let candidate = follower_with_candidates(&["candidate2"]).into_candidate();

    assert!(candidate.vote(node_id("candidate2"), 2).unwrap().payload.voted);
    assert!(!candidate.vote(node_id("candidate2"), 2).unwrap().payload.voted);
    assert_eq!(candidate.term(), 2);
}

#[tokio::test]
async fn term_starts_at_zero() {
    assert_eq!(plain_follower().term(), 0);
}

#[tokio::test]
async fn election_is_won_when_peers_approve() {
    let approvals = Arc::new(Counters::default());
    let state = follower_with_peers(&[("node2", approvals.clone(), Behavior::approve())]);
    let candidate = state.into_candidate();

    let leader = candidate.start_election().await.expect("election should succeed");

    assert_eq!(leader.term(), 1, "an election must bump the term exactly once");
    assert_eq!(approvals.votes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn election_is_lost_when_peers_reject() {
    let rejections = Arc::new(Counters::default());
    let state = follower_with_peers(&[("node2", rejections.clone(), Behavior::reject())]);
    let candidate = state.into_candidate();

    let result = candidate.start_election().await;

    assert!(result.is_err(), "a candidate with no yes-votes must not become leader");
}

#[tokio::test]
async fn election_is_lost_when_no_peer_can_be_reached() {
    let unreachable = Arc::new(Counters::default());
    let state = follower_with_peers(&[("node2", unreachable, Behavior::hanging())]);
    let candidate = state.into_candidate();

    let follower = candidate.start_election().await.expect_err("an unreachable peer cannot form a quorum");

    assert_eq!(follower.term(), 1);
    assert!(Node::Follower(follower).try_follower(Some(node_id("leader-a"))).is_err(),
        "after a failed election the node must not claim a leader");
}

#[tokio::test(start_paused = true)]
async fn election_times_out_when_peers_do_not_answer() {
    let unreachable = Arc::new(Counters::default());
    let state = follower_with_peers(&[("node2", unreachable, Behavior::hanging())]);
    let candidate = state.into_candidate();

    let election = tokio::spawn(async move { candidate.start_election().await });
    settle().await;
    tokio::time::advance(Duration::from_millis(501)).await;

    let result = election.await.expect("election task panicked");
    assert!(result.is_err(), "a hung peer must not keep the candidate waiting forever");
}

#[tokio::test]
// FIXME 'Term overflow #7'
#[ignore = "known bug: AtomicU64::fetch_add wraps, so a term at u64::MAX becomes 0"]
async fn term_does_not_wrap_around() {
    let state = follower_with_candidates(&["peer"]);
    let _ = state.vote(node_id("peer"), u64::MAX);
    let observer = state.clone();
    let candidate = state.into_candidate();

    let _ = candidate.start_election().await;

    assert_eq!(observer.term(), u64::MAX, "terms must saturate instead of wrapping to zero");
}
