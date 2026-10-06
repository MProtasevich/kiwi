use crate::job::registry::{Control, Job, JobKey, Trigger};
use async_trait::async_trait;

#[derive(Debug)]
pub struct LogReplicationJob {}

#[async_trait]
impl Job for LogReplicationJob {
    async fn execute(&self) -> Control {
        todo!()
    }

    fn key(&self) -> JobKey {
        JobKey::LogReplication
    }

    fn trigger(&self) -> Trigger {
        Trigger::Once
    }

}

impl LogReplicationJob {

    pub fn new() -> Self {
        Self {}
    }

}
