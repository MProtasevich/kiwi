use crate::job::registry::Job;
use crate::log::CommandLog;
use crate::node_state::NodeId;
use crate::remote_node::ClusterClient;
use crate::transport::model::ClusterMessage;
use crate::transport::model::log::LogReplicationRequest;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::warn;
use std::ops::Sub;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Debug)]
pub struct LogReplication {
    cluster_client: Arc<ClusterClient>,
    log: Arc<dyn CommandLog>,
    since: DateTime<Utc>,
    term: Arc<AtomicU64>,
    node_id: NodeId,
    chunk_size: usize,
}

const INITIAL_SHIFT: Duration = Duration::from_nanos(1);

#[async_trait]
impl Job for LogReplication {

    async fn start(&mut self) {
        let mut prev_cursor = self.since.sub(INITIAL_SHIFT);
        let mut curr_cursor = self.since;
        while prev_cursor < curr_cursor {
            let request = LogReplicationRequest { cursor_from: curr_cursor, chunk_size: self.chunk_size };
            let message = ClusterMessage::new(self.node_id.clone(), self.term.load(Ordering::Relaxed), request);
            let Ok(response) = self.cluster_client.ask_for_log(message).await else {
                warn!("[log replication] request failed");
                return;
            };
            prev_cursor = curr_cursor;
            curr_cursor = response.cursor_id;
            let _ = self.log.push_all(response.commands);
        }
    }

}

impl LogReplication {

    pub fn new(
        cluster_client: Arc<ClusterClient>,
        log: Arc<dyn CommandLog>,
        since: DateTime<Utc>,
        term: Arc<AtomicU64>,
        node_id: NodeId,
        chunk_size: usize) -> Self {
        Self {
            cluster_client,
            log,
            since,
            term,
            node_id,
            chunk_size,
        }
    }

}
