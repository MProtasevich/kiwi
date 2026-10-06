use crate::job::registry::{Control, Job, JobKey, Trigger};
use crate::node_state::State;
use crate::node_state::states::Leader;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug)]
pub struct HeartbeatPollingJob {
    period: Duration,
    state: Arc<State<Leader>>,
}

impl HeartbeatPollingJob {

    pub fn new(period: Duration, state: Arc<State<Leader>>) -> Self {
        Self { period, state }
    }

}

#[async_trait]
impl Job for HeartbeatPollingJob {

    async fn execute(&self) -> Control {
        // A failed heartbeat must not stop the polling loop; the next tick retries.
        let _ = self.state.send_heartbeat().await;
        Control::Continue
    }

    fn key(&self) -> JobKey {
        JobKey::HeartbeatPush
    }

    fn trigger(&self) -> Trigger {
        Trigger::Every(self.period)
    }

}
