use async_trait::async_trait;
use std::fmt::Debug;
use std::time::Duration;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub enum JobKey {
    HeartbeatPush,
    HeartbeatCountdown,
    LogReplication,
}

/// What the registry should do with a job after one `Job::execute` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Control {
    /// Keep the job scheduled.
    Continue,
    /// The job finished on its own; the driver stops.
    Done,
}

/// When the registry invokes [`Job::execute`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    /// Call once, immediately. For finite work (e.g. catching up a log).
    Once,
    /// Call every `period`; the first call happens immediately.
    Every(Duration),
    /// Call once after `period` without a reset; `reset` restarts the timer.
    AfterQuiet(Duration),
}

/// A unit of work driven by the registry.
///
/// A job is a listener of lifecycle events, specified by [`Trigger`]: it must
/// not loop forever waiting for events and must not spawn detached tasks.
/// The registry owns the task, the timers, and cancellation, so
/// [`JobRegistry::stop`] can always abort it.
#[async_trait]
pub trait Job: Debug + Send + Sync + 'static {
    /// Runs one unit of work. The registry decides when this is called.
    async fn execute(&self) -> Control;

    fn key(&self) -> JobKey;

    fn trigger(&self) -> Trigger;
}

#[derive(Debug)]
pub struct JobRegistry {
}

impl JobRegistry {

    pub fn is_registered(&self, job_key: JobKey) -> bool {
        todo!()
    }

    pub fn is_running(&self, job_key: JobKey) -> bool {
        todo!()
    }

    pub fn register(&self, job: impl Job) {
        todo!()
    }

    pub fn register_and_start(&self, job: impl Job) {
        todo!()
    }

    pub fn start(&self, job_key: JobKey) {
        todo!()
    }

    /// Restarts the `AfterQuiet` deadline of a running job. Ignored by other triggers.
    pub fn reset(&self, job_key: JobKey) {
        todo!()
    }

    pub async fn stop(&self, job_key: JobKey) {
        todo!()
    }

}
