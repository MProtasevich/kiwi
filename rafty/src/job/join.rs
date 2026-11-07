//! Boot-time join: asks a seed to admit this node, then installs the cluster view.
//!
//! The job retries on a fixed period until a seed accepts or redirects to the leader.
//! It never changes Raft state itself: the leader owns the membership change, and the
//! joiner only installs the configuration it receives.

use crate::job::registry::{Control, Job, JobKey, Trigger};
use crate::node_state::NodeId;
use crate::remote_node::ClusterClient;
use crate::transport::model::ClusterMessage;
use crate::transport::model::membership::{JoinRequest, JoinResponse};
use async_trait::async_trait;
use log::{info, warn};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug)]
pub struct JoinJob {
    cluster_client: Arc<ClusterClient>,
    node_id: NodeId,
    request: JoinRequest,
    seeds: Vec<NodeId>,
    retry_period: Duration,
}

impl JoinJob {
    /// Creates a join job that tries `seeds` in order and retries every `retry_period`.
    pub fn new(
        cluster_client: Arc<ClusterClient>,
        node_id: NodeId,
        request: JoinRequest,
        seeds: Vec<NodeId>,
        retry_period: Duration,
    ) -> Self {
        Self {
            cluster_client,
            node_id,
            request,
            seeds,
            retry_period,
        }
    }
}

#[async_trait]
impl Job for JoinJob {
    async fn execute(&self) -> Control {
        let message = ClusterMessage::new(self.node_id.clone(), 0, self.request.clone());
        let mut queue = self.seeds.clone();
        let mut index = 0;

        while index < queue.len() {
            let seed = queue[index].clone();
            index += 1;

            match self.cluster_client.join(&seed, message.clone()).await {
                Ok(response) => match response.payload {
                    JoinResponse::Accepted {
                        configuration,
                        voter,
                        leader_id,
                    } => {
                        info!(
                            "[join] admitted by [{leader_id}] as {} at configuration index {}",
                            if voter { "voter" } else { "learner" },
                            configuration.index
                        );
                        if let Err(error) = self.cluster_client.install_snapshot(configuration) {
                            warn!("[join] unable to install the configuration: {error:#}");
                            return Control::Continue;
                        }
                        return Control::Done;
                    }
                    JoinResponse::Redirect { leader_id, .. } => {
                        if !queue.contains(&leader_id) {
                            queue.push(leader_id);
                        }
                    }
                    JoinResponse::Rejected { reason } => {
                        warn!("[join] seed [{seed}] rejected the request: {reason}");
                    }
                },
                Err(error) => {
                    warn!("[join] seed [{seed}] is not reachable: {error:#}");
                }
            }
        }

        Control::Continue
    }

    fn key(&self) -> JobKey {
        JobKey::Join
    }

    fn trigger(&self) -> Trigger {
        Trigger::Every(self.retry_period)
    }
}
