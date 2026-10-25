use crate::job::registry::Job;
use crate::node_state::Node;
use anyhow::Result;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use log::{info, trace, warn};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::watch;
use tokio::sync::watch::{Receiver, Sender};
use tokio::task::AbortHandle;
use tokio::time::timeout;

#[derive(Debug)]
pub struct HeartbeatCountdown {
    runtime: Arc<Runtime>,
    period: Duration,
    node: Arc<ArcSwap<Node>>,
    tx: Option<Sender<u64>>,
    abort_handle: Option<AbortHandle>,
}

#[async_trait]
impl Job for HeartbeatCountdown {

    async fn start(&mut self) {
        let (tx, rx) = watch::channel(0u64);
        let node = Arc::clone(&self.node);
        let handle = self.runtime.spawn(countdown(rx, self.period, node));

        self.tx = Some(tx);
        self.abort_handle = Some(handle.abort_handle());
    }

    async fn reset(&self) {
        let Some(tx) = &self.tx else {
            warn!("[scheduler] job for [reset heartbeat countdown] hasn't been started");
            return;
        };
        // Increment value so `rx.changed()` detects a change
        let next = *tx.borrow() + 1;
        // send() only fails if all receivers are dropped
        let _ = tx.send(next);
    }

    async fn stop(&self) {
        match &self.abort_handle {
            Some(handle) => handle.abort(),
            None => warn!("No AbortHandle available"),
        }
    }
}

async fn countdown(mut rx: Receiver<u64>, period: Duration, node: Arc<ArcSwap<Node>>) {
    loop {
        // Wait for either: a reset signal OR the deadline to expire
        match timeout(period, rx.changed()).await {
            Ok(Ok(())) => trace!("[watchdog] heartbeat received, resetting countdown"),
            Ok(Err(_)) => {
                info!("[watchdog] sender dropped, stopping");
                break;
            }
            Err(_elapsed) => {
                let election_result = start_election(node.as_ref()).await;
                if let Err(error) = election_result {
                    warn!("[watchdog]: {error}");
                    continue;
                } else {
                    break;
                }
            }
        }
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
    let node_after_election = match state.start_election().await {
        Ok(state) => Node::Leader(state),
        Err(state) => Node::Follower(state),
    };
    // Have to use arc-swap instead of Arc<RwLock<T>> due to RwLockReadGuard is !Send
    // and can't be shared between threads
    node.compare_and_swap(&candidate_state, Arc::new(node_after_election));
    Ok(())
}
