//! Cluster membership: static configuration, runtime join and replicated
//! configuration entries.

mod support;

use rafty::cluster::{ClusterMember, ClusterMembership, ConfigChange, ConfigurationEntry};
use rafty::job::join::JoinJob;
use rafty::job::registry::{Control, Job};
use rafty::node_state::{Node, State, states::Leader};
use rafty::remote_node::{ClusterClient, RemoteNode, RemoteNodeFactory};
use rafty::transport::model::ClusterMessage;
use rafty::transport::model::membership::{ConfigurationBroadcast, JoinRequest, JoinResponse};
use rafty::transport::model::vote::VoteRequest;
use support::*;

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn join_request(cluster: &str, id: &str, addr: &str) -> ClusterMessage<JoinRequest> {
    ClusterMessage::new(
        node_id(id),
        0,
        JoinRequest::new(cluster_id(cluster), node_id(id), address(addr)),
    )
}

fn leadered(client: &Arc<ClusterClient>) -> State<Leader> {
    follower_state(
        "node1",
        FakeLog::empty(),
        Arc::new(FakeStorage::default()),
        Arc::clone(client),
        test_registry(),
    )
    .into_candidate()
    .into_leader()
}

#[tokio::test]
async fn leader_admits_a_learner_and_broadcasts_the_configuration() {
    let peer2 = Arc::new(Counters::default());
    let peer3 = Arc::new(Counters::default());
    let client = cluster_client(
        "node1",
        &[
            ("node2", peer2.clone(), Behavior::approve()),
            ("node3", peer3.clone(), Behavior::approve()),
        ],
    );
    let cluster = client.configuration().cluster_id.clone();
    let leader = leadered(&client);

    let response = leader
        .handle_join(join_request(cluster.as_str(), "node4", "node4:1004"))
        .await
        .unwrap();

    match response.payload {
        JoinResponse::Accepted {
            leader_id,
            configuration,
            voter,
        } => {
            assert_eq!(leader_id, node_id("node1"));
            assert!(!voter, "a new node must start as a learner");
            assert_eq!(configuration.index, 1);
            assert!(configuration.is_learner(&node_id("node4")));
            assert_eq!(configuration.voter_count(), 3);
        }
        other => panic!("expected acceptance, got {other:?}"),
    }

    assert_eq!(peer2.configurations.load(Ordering::SeqCst), 1);
    assert_eq!(peer3.configurations.load(Ordering::SeqCst), 1);
    assert_eq!(client.configuration().index, 1);
    let history = client.configuration_history().unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].index, 1);
}

#[tokio::test]
async fn joining_an_existing_member_is_idempotent() {
    let peer2 = Arc::new(Counters::default());
    let peer3 = Arc::new(Counters::default());
    let client = cluster_client(
        "node1",
        &[
            ("node2", peer2.clone(), Behavior::approve()),
            ("node3", peer3.clone(), Behavior::approve()),
        ],
    );
    let cluster = client.configuration().cluster_id.clone();
    let leader = leadered(&client);

    leader
        .handle_join(join_request(cluster.as_str(), "node4", "node4:1004"))
        .await
        .unwrap();
    let second = leader
        .handle_join(join_request(cluster.as_str(), "node4", "node4:1004"))
        .await
        .unwrap();

    match second.payload {
        JoinResponse::Accepted {
            configuration,
            voter,
            ..
        } => {
            assert!(!voter);
            assert_eq!(
                configuration.index, 1,
                "no new entry for an existing member"
            );
        }
        other => panic!("expected acceptance, got {other:?}"),
    }
    assert_eq!(peer2.configurations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_node_from_another_cluster_is_rejected() {
    let client = cluster_client("node1", &[]);
    let leader = leadered(&client);

    let response = leader
        .handle_join(join_request("other-cluster", "node4", "node4:1004"))
        .await
        .unwrap();

    assert!(matches!(response.payload, JoinResponse::Rejected { .. }));
    assert_eq!(client.configuration().index, 0);
}

#[tokio::test]
async fn a_follower_redirects_the_joiner_to_its_leader() {
    let configuration = configuration(
        "demo",
        0,
        &[("node1", "node1:1001", true), ("node2", "node2:1002", true)],
    );
    let client = cluster_client_with_configuration(
        "node1",
        configuration,
        &[("node2", Arc::new(Counters::default()), Behavior::approve())],
    );
    let mut follower = follower_state(
        "node1",
        FakeLog::empty(),
        Arc::new(FakeStorage::default()),
        client,
        test_registry(),
    );
    follower.set_leader(node_id("node2"));
    let node = Node::Follower(follower);

    let response = node
        .handle_join(join_request("demo", "node4", "node4:1004"))
        .await
        .unwrap();

    match response.payload {
        JoinResponse::Redirect { leader_id, address } => {
            assert_eq!(leader_id, node_id("node2"));
            assert_eq!(address.to_string(), "node2:1002");
        }
        other => panic!("expected redirect, got {other:?}"),
    }
}

#[tokio::test]
async fn configuration_change_replicates_and_applies_on_a_follower() {
    let configuration = configuration(
        "demo",
        0,
        &[
            ("node1", "node1:1001", true),
            ("node2", "node2:1002", true),
            ("node3", "node3:1003", true),
        ],
    );
    let client = cluster_client_with_configuration(
        "node1",
        configuration.clone(),
        &[("node2", Arc::new(Counters::default()), Behavior::approve())],
    );
    let node = Node::Follower(follower_state(
        "node1",
        FakeLog::empty(),
        Arc::new(FakeStorage::default()),
        Arc::clone(&client),
        test_registry(),
    ));

    let change = ConfigChange::AddLearner {
        node_id: node_id("node4"),
        address: address("node4:1004"),
    };
    let next = configuration.applied(&change).unwrap();
    let message = ClusterMessage::new(
        node_id("node3"),
        4,
        ConfigurationBroadcast {
            entry: ConfigurationEntry::new(next.index, 4, change),
            configuration: next,
        },
    );

    let ack = node.apply_configuration(&message);
    assert!(
        ack.payload.accepted,
        "unexpected rejection: {:?}",
        ack.payload.reason
    );
    assert_eq!(ack.metadata.term, 4);
    assert_eq!(client.configuration().index, 1);
    assert!(client.configuration().is_learner(&node_id("node4")));

    // Retries of the same entry are accepted and do not advance the index.
    let replay = node.apply_configuration(&message);
    assert!(replay.payload.accepted);
    assert_eq!(client.configuration().index, 1);

    // Skipping an entry is rejected: membership would otherwise miss a change.
    let gap = ClusterMessage::new(
        node_id("node3"),
        4,
        ConfigurationBroadcast {
            entry: ConfigurationEntry::new(
                3,
                4,
                ConfigChange::Promote {
                    node_id: node_id("node4"),
                },
            ),
            configuration: client.configuration().as_ref().clone(),
        },
    );
    assert!(!node.apply_configuration(&gap).payload.accepted);

    // A message from an older term is rejected without touching membership.
    let stale = ClusterMessage::new(node_id("node3"), 1, gap.payload.clone());
    assert!(!node.apply_configuration(&stale).payload.accepted);
}

#[tokio::test]
async fn a_configuration_change_without_a_quorum_is_not_applied() {
    let client = cluster_client(
        "node1",
        &[
            (
                "node2",
                Arc::new(Counters::default()),
                Behavior::rejecting_configuration(),
            ),
            (
                "node3",
                Arc::new(Counters::default()),
                Behavior::rejecting_configuration(),
            ),
        ],
    );
    let cluster = client.configuration().cluster_id.clone();
    let leader = leadered(&client);

    let result = leader
        .handle_join(join_request(cluster.as_str(), "node4", "node4:1004"))
        .await;

    assert!(
        result.is_err(),
        "the leader must not commit without a quorum"
    );
    assert_eq!(client.configuration().index, 0);
    assert!(!client.configuration().contains(&node_id("node4")));
}

#[tokio::test]
async fn a_learner_can_be_promoted_and_removed() {
    let client = cluster_client(
        "node1",
        &[
            ("node2", Arc::new(Counters::default()), Behavior::approve()),
            ("node3", Arc::new(Counters::default()), Behavior::approve()),
        ],
    );
    let cluster = client.configuration().cluster_id.clone();
    let leader = leadered(&client);

    leader
        .handle_join(join_request(cluster.as_str(), "node4", "node4:1004"))
        .await
        .unwrap();

    let promoted = leader.promote_member(node_id("node4")).await.unwrap();
    assert_eq!(promoted.voter_count(), 4);
    assert!(promoted.is_voter(&node_id("node4")));

    let removed = leader.remove_member(node_id("node4")).await.unwrap();
    assert_eq!(removed.voter_count(), 3);
    assert!(!removed.contains(&node_id("node4")));
}

/// Builds a transport for a member discovered at runtime and records it.
#[derive(Debug, Default)]
struct RecordingFactory {
    built: Mutex<Vec<(String, Arc<Counters>)>>,
}

impl RemoteNodeFactory for RecordingFactory {
    fn create(&self, member: &ClusterMember) -> anyhow::Result<RemoteNode> {
        let counters = Arc::new(Counters::default());
        self.built
            .lock()
            .unwrap()
            .push((member.id.to_string(), Arc::clone(&counters)));
        let recorder = Arc::new(Recorder {
            id: member.id.clone(),
            counters,
            behavior: Behavior::approve(),
        });
        Ok(RemoteNode::new(
            recorder.clone(),
            recorder.clone(),
            recorder,
        ))
    }
}

#[tokio::test]
async fn a_factory_is_used_to_reach_the_new_learner() {
    let mut nodes = HashMap::new();
    for id in ["node2", "node3"] {
        let counters = Arc::new(Counters::default());
        nodes.insert(node_id(id), remote_node(id, counters, Behavior::approve()));
    }
    let factory = Arc::new(RecordingFactory::default());
    let membership = Arc::new(ClusterMembership::new(configuration(
        "demo",
        0,
        &[
            ("node1", "node1:1001", true),
            ("node2", "node2:1002", true),
            ("node3", "node3:1003", true),
        ],
    )));
    let client = Arc::new(
        ClusterClient::with_membership(
            node_id("node1"),
            membership,
            Arc::new(nodes),
            Duration::from_millis(40),
        )
        .with_factory(factory.clone()),
    );
    let leader = leadered(&client);

    leader
        .handle_join(join_request("demo", "node4", "node4:1004"))
        .await
        .unwrap();

    let built = factory.built.lock().unwrap();
    let (id, counters) = built
        .iter()
        .find(|(id, _)| id == "node4")
        .expect("the new learner must get a transport");
    assert_eq!(id, "node4");
    assert_eq!(
        counters.configurations.load(Ordering::SeqCst),
        1,
        "the learner must receive the configuration that admits it"
    );
    assert_eq!(client.configuration().index, 1);
}

#[tokio::test]
async fn a_node_that_has_not_joined_cannot_form_a_quorum() {
    let client = Arc::new(ClusterClient::with_membership(
        node_id("node4"),
        Arc::new(ClusterMembership::new(configuration("demo", 0, &[]))),
        Arc::new(HashMap::new()),
        Duration::from_millis(40),
    ));

    let result = client
        .ask_for_vote(ClusterMessage::new(node_id("node4"), 1, VoteRequest))
        .await;

    assert!(
        result.is_err(),
        "an empty voter set must not count as an immediate quorum"
    );
}

#[tokio::test]
async fn join_job_installs_the_configuration_returned_by_the_leader() {
    let membership = configuration(
        "demo",
        0,
        &[
            ("node1", "node1:1001", true),
            ("node2", "node2:1002", true),
            ("node3", "node3:1003", true),
        ],
    );
    let target = membership
        .applied(&ConfigChange::AddLearner {
            node_id: node_id("node4"),
            address: address("node4:1004"),
        })
        .unwrap();
    let responder = JoinResponder::new(
        "node1",
        JoinResponse::Accepted {
            leader_id: node_id("node1"),
            configuration: target.clone(),
            voter: false,
        },
    );
    let remote = joinable_remote(
        "node1",
        Arc::new(Counters::default()),
        Behavior::approve(),
        Arc::clone(&responder),
    );
    let client = Arc::new(ClusterClient::with_membership(
        node_id("node4"),
        Arc::new(ClusterMembership::new(membership)),
        Arc::new(HashMap::from([(node_id("node1"), remote)])),
        Duration::from_millis(40),
    ));
    let job = JoinJob::new(
        Arc::clone(&client),
        node_id("node4"),
        JoinRequest::new(cluster_id("demo"), node_id("node4"), address("node4:1004")),
        vec![node_id("node1")],
        Duration::from_millis(10),
    );

    let control = job.execute().await;

    assert_eq!(control, Control::Done);
    assert_eq!(client.configuration().index, 1);
    assert!(client.configuration().is_learner(&node_id("node4")));
    assert_eq!(responder.calls(), 1);
}

#[tokio::test]
async fn join_job_follows_a_redirect_and_retries_otherwise() {
    let membership = configuration(
        "demo",
        0,
        &[("node1", "node1:1001", true), ("node2", "node2:1002", true)],
    );
    let accepted = membership
        .applied(&ConfigChange::AddLearner {
            node_id: node_id("node4"),
            address: address("node4:1004"),
        })
        .unwrap();
    let seed_responder = JoinResponder::new(
        "node1",
        JoinResponse::Redirect {
            leader_id: node_id("node2"),
            address: address("node2:1002"),
        },
    );
    let leader_responder = JoinResponder::new(
        "node2",
        JoinResponse::Accepted {
            leader_id: node_id("node2"),
            configuration: accepted.clone(),
            voter: false,
        },
    );
    let client = Arc::new(ClusterClient::with_membership(
        node_id("node4"),
        Arc::new(ClusterMembership::new(membership)),
        Arc::new(HashMap::from([
            (
                node_id("node1"),
                joinable_remote(
                    "node1",
                    Arc::new(Counters::default()),
                    Behavior::approve(),
                    Arc::clone(&seed_responder),
                ),
            ),
            (
                node_id("node2"),
                joinable_remote(
                    "node2",
                    Arc::new(Counters::default()),
                    Behavior::approve(),
                    Arc::clone(&leader_responder),
                ),
            ),
        ])),
        Duration::from_millis(40),
    ));
    let job = JoinJob::new(
        Arc::clone(&client),
        node_id("node4"),
        JoinRequest::new(cluster_id("demo"), node_id("node4"), address("node4:1004")),
        vec![node_id("node1")],
        Duration::from_millis(10),
    );

    assert_eq!(job.execute().await, Control::Done);
    assert_eq!(seed_responder.calls(), 1);
    assert_eq!(leader_responder.calls(), 1);
    assert_eq!(client.configuration().index, 1);
}

#[tokio::test]
async fn join_job_keeps_retrying_when_every_seed_rejects() {
    let membership = configuration("demo", 0, &[("node1", "node1:1001", true)]);
    let responder = JoinResponder::new(
        "node1",
        JoinResponse::Rejected {
            reason: "not now".to_owned(),
        },
    );
    let client = Arc::new(ClusterClient::with_membership(
        node_id("node4"),
        Arc::new(ClusterMembership::new(membership)),
        Arc::new(HashMap::from([(
            node_id("node1"),
            joinable_remote(
                "node1",
                Arc::new(Counters::default()),
                Behavior::approve(),
                Arc::clone(&responder),
            ),
        )])),
        Duration::from_millis(40),
    ));
    let job = JoinJob::new(
        Arc::clone(&client),
        node_id("node4"),
        JoinRequest::new(cluster_id("demo"), node_id("node4"), address("node4:1004")),
        vec![node_id("node1")],
        Duration::from_millis(10),
    );

    assert_eq!(job.execute().await, Control::Continue);
    assert_eq!(client.configuration().index, 0);
    assert_eq!(responder.calls(), 1);
}
