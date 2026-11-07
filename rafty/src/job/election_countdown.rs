//! Election timeout watchdog.
//!
//! The registry drives the deadline and restarts it on every heartbeat; this job only
//! knows what to do when the countdown fires.

use crate::job::registry::{Control, Job, JobKey, Trigger};
use crate::node_state::Node;
use anyhow::Result;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use log::trace;
use std::range::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug)]
pub struct ElectionCountdownJob {
    range: RangeInclusive<Duration>,
    node: Arc<ArcSwap<Node>>,
}

impl ElectionCountdownJob {

    /// Creates a job with a fixed election timeout.
    pub fn new(period: Duration, node: Arc<ArcSwap<Node>>) -> Self {
        Self::with_timeout_range((period..=period).into(), node)
    }

    /// Creates a job whose election timeout is re-drawn from `range`
    /// on every reset, as Raft recommends to avoid split votes.
    pub fn with_timeout_range(range: RangeInclusive<Duration>, node: Arc<ArcSwap<Node>>) -> Self {
        Self {
            range,
            node,
        }
    }

}

#[async_trait]
impl Job for ElectionCountdownJob {

    async fn execute(&self) -> Control {
        trace!("Election initiated");
        if start_election(&self.node).await.is_ok() {
            trace!("Election won. Turn to [Leader]");
            Control::Done
        } else {
            trace!("Election lost. Turn to [Follower]");
            Control::Continue
        }
    }

    fn key(&self) -> JobKey {
        JobKey::HeartbeatCountdown
    }

    fn trigger(&self) -> Trigger {
        Trigger::AfterQuiet(self.range)
    }

}

async fn start_election(node: &ArcSwap<Node>) -> Result<()> {
    let guard = node.load();
    let state = guard.try_follower(None)?;
    let candidate_state = Arc::new(Node::Candidate(state.into_candidate()));
    let cloned_state = Arc::clone(&candidate_state);
    node.compare_and_swap(guard, candidate_state);

    let candidate_state = Arc::clone(&cloned_state);
    let state = cloned_state.try_candidate()?;
    let election_result = state.start_election().await;
    let election_won = election_result.is_ok();
    let node_after_election = match election_result {
        Ok(state) => Node::Leader(state),
        Err(state) => Node::Follower(state),
    };
    // Have to use arc-swap instead of Arc<RwLock<T>> due to RwLockReadGuard is !Send
    // and can't be shared between threads
    node.compare_and_swap(&candidate_state, Arc::new(node_after_election));
    if election_won { Ok(()) } else { Err(anyhow::anyhow!("election failed")) }
}
