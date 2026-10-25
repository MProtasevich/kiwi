use crate::job::registry::Job;
use crate::node_state::State;
use crate::node_state::states::Leader;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

#[derive(Debug)]
pub struct HeartbeatPolling {
    period: Duration,
    state: Arc<State<Leader>>,
}

#[async_trait]
impl Job for HeartbeatPolling {

    async fn start(&mut self) {
        let mut ticker = time::interval(self.period);
        loop {
            ticker.tick().await;
            let _ = self.state.send_heartbeat().await;
        }
    }

}
