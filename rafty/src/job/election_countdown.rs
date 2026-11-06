//! Election timeout watchdog.
//!
//! The registry drives the deadline and restarts it on every heartbeat; this job only
//! knows what to do when the countdown fires.

use crate::job::registry::{Control, Job, JobKey, Trigger};
use crate::node_state::Node;
use anyhow::Result;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use log::{info, warn};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug)]
pub struct ElectionCountdownJob {
    period: Duration,
    node: Arc<ArcSwap<Node>>,
}

impl ElectionCountdownJob {

    pub fn new(period: Duration, node: Arc<ArcSwap<Node>>) -> Self {
        Self { period, node }
    }

}

#[async_trait]
impl Job for ElectionCountdownJob {

    async fn execute(&self) -> Control {
        match start_election(&self.node).await {
            // The node became a leader; there is nothing left to count down.
            Ok(()) => Control::Done,
            Err(error) => {
                warn!("[watchdog]: {error}");
                Control::Continue
            }
        }
    }

    fn key(&self) -> JobKey {
        JobKey::HeartbeatCountdown
    }

    fn trigger(&self) -> Trigger {
        Trigger::AfterQuiet(self.period)
    }

}

async fn start_election(node: &ArcSwap<Node>) -> Result<()> {
    info!("[watchdog] TIMEOUT! firing event...");
    let guard = node.load();
    let state = guard.try_follower(None)?;
    let candidate_state = Arc::new(Node::Candidate(state.into_candidate()));
    let cloned_state = Arc::clone(&candidate_state);
    node.compare_and_swap(guard, candidate_state);

    let candidate_state = Arc::clone(&cloned_state);
    let state = cloned_state.try_candidate()?;
    let election_result = state.start_election().await;
    let failed = election_result.is_err();
    let node_after_election = match election_result {
        Ok(state) => Node::Leader(state),
        Err(state) => Node::Follower(state),
    };
    // Have to use arc-swap instead of Arc<RwLock<T>> due to RwLockReadGuard is !Send
    // and can't be shared between threads
    node.compare_and_swap(&candidate_state, Arc::new(node_after_election));
    if failed {
        anyhow::bail!("election failed");
    }
    Ok(())
}
