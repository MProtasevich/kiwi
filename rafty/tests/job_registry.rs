//! Lifecycle of [`rafty::job::registry::JobRegistry`].
//!
//! The registry owns the driver loop; a job is only a listener called by that loop.

use async_trait::async_trait;
use rafty::job::registry::{Control, Job, JobKey, JobRegistry, Trigger};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

const PERIOD: Duration = Duration::from_millis(100);

#[derive(Debug)]
struct SpyJob {
    key: JobKey,
    trigger: Trigger,
    starts: Arc<AtomicUsize>,
    ticks: Arc<AtomicUsize>,
    forever: bool,
}

impl SpyJob {

    fn new(key: JobKey, trigger: Trigger) -> (Self, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let starts = Arc::new(AtomicUsize::new(0));
        let ticks = Arc::new(AtomicUsize::new(0));
        (
            Self {
                key,
                trigger,
                starts: starts.clone(),
                ticks: ticks.clone(),
                forever: false,
            },
            starts,
            ticks,
        )
    }

    fn looping(key: JobKey, trigger: Trigger) -> (Self, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let (job, starts, ticks) = Self::new(key, trigger);
        (Self { forever: true, ..job }, starts, ticks)
    }

}

#[async_trait]
impl Job for SpyJob {

    async fn execute(&self) -> Control {
        self.starts.fetch_add(1, Ordering::SeqCst);
        if self.forever {
            loop {
                self.ticks.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
            }
        }
        self.ticks.fetch_add(1, Ordering::SeqCst);
        Control::Continue
    }

    fn key(&self) -> JobKey {
        self.key
    }

    fn trigger(&self) -> Trigger {
        self.trigger
    }

}

async fn settle() {
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn start_runs_the_job_exactly_once() {
    let registry = JobRegistry::from_current();
    let (job, starts, _) = SpyJob::new(JobKey::HeartbeatPush, Trigger::Once);
    registry.register(job);

    registry.start(JobKey::HeartbeatPush);
    settle().await;

    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert!(!registry.is_running(JobKey::HeartbeatPush), "a Once job finishes after one tick");
}

#[tokio::test]
async fn register_and_start_is_a_single_call() {
    let registry = JobRegistry::from_current();
    let (job, starts, _) = SpyJob::new(JobKey::HeartbeatPush, Trigger::Once);

    registry.register_and_start(job);
    settle().await;

    assert_eq!(starts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn starting_a_running_job_is_ignored() {
    let registry = JobRegistry::from_current();
    let (job, starts, _) = SpyJob::looping(JobKey::HeartbeatPush, Trigger::Once);
    registry.register(job);

    registry.start(JobKey::HeartbeatPush);
    settle().await;
    registry.start(JobKey::HeartbeatPush);
    settle().await;

    assert_eq!(starts.load(Ordering::SeqCst), 1, "a running job must not be started twice");
    assert!(registry.is_running(JobKey::HeartbeatPush));

    registry.stop(JobKey::HeartbeatPush).await;
}

#[tokio::test]
async fn a_finished_job_can_be_started_again() {
    let registry = JobRegistry::from_current();
    let (job, starts, _) = SpyJob::new(JobKey::HeartbeatPush, Trigger::Once);
    registry.register(job);

    registry.start(JobKey::HeartbeatPush);
    settle().await;
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert!(!registry.is_running(JobKey::HeartbeatPush));

    registry.start(JobKey::HeartbeatPush);
    settle().await;

    assert_eq!(starts.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn every_trigger_ticks_repeatedly() {
    let registry = JobRegistry::from_current();
    let (job, starts, _) = SpyJob::new(JobKey::HeartbeatPush, Trigger::Every(PERIOD));
    registry.register_and_start(job);
    settle().await;

    assert_eq!(starts.load(Ordering::SeqCst), 1, "the first tick is immediate");

    for tick in 2..=5 {
        tokio::time::advance(PERIOD).await;
        settle().await;
        assert_eq!(starts.load(Ordering::SeqCst), tick);
    }

    registry.stop(JobKey::HeartbeatPush).await;
}

#[tokio::test(start_paused = true)]
async fn reset_restarts_the_after_quiet_timer() {
    let registry = JobRegistry::from_current();
    let (job, starts, _) = SpyJob::new(JobKey::HeartbeatCountdown, Trigger::AfterQuiet(PERIOD));
    registry.register_and_start(job);
    settle().await;

    tokio::time::advance(PERIOD - Duration::from_millis(1)).await;
    settle().await;
    assert_eq!(starts.load(Ordering::SeqCst), 0);

    registry.reset(JobKey::HeartbeatCountdown);
    settle().await;

    tokio::time::advance(PERIOD - Duration::from_millis(1)).await;
    settle().await;
    assert_eq!(starts.load(Ordering::SeqCst), 0, "a reset must restart the deadline");

    tokio::time::advance(Duration::from_millis(2)).await;
    settle().await;
    assert_eq!(starts.load(Ordering::SeqCst), 1);

    registry.stop(JobKey::HeartbeatCountdown).await;
}

#[tokio::test(start_paused = true)]
async fn stop_freezes_a_repeating_job() {
    let registry = JobRegistry::from_current();
    let (job, starts, _) = SpyJob::new(JobKey::HeartbeatPush, Trigger::Every(PERIOD));
    registry.register_and_start(job);
    settle().await;

    let before_stop = starts.load(Ordering::SeqCst);
    assert!(before_stop > 0);

    registry.stop(JobKey::HeartbeatPush).await;
    settle().await;
    tokio::time::advance(PERIOD * 5).await;
    settle().await;

    assert_eq!(starts.load(Ordering::SeqCst), before_stop);
}

#[tokio::test]
async fn stop_aborts_a_running_job() {
    let registry = JobRegistry::from_current();
    let (job, starts, ticks) = SpyJob::looping(JobKey::HeartbeatPush, Trigger::Once);
    registry.register_and_start(job);
    settle().await;

    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert!(ticks.load(Ordering::SeqCst) > 0);

    registry.stop(JobKey::HeartbeatPush).await;
    settle().await;
    let after_stop = ticks.load(Ordering::SeqCst);
    settle().await;

    assert_eq!(ticks.load(Ordering::SeqCst), after_stop, "the spawned task must be aborted");
    assert!(!registry.is_running(JobKey::HeartbeatPush));
    assert!(!registry.is_registered(JobKey::HeartbeatPush));
}

#[tokio::test]
async fn stop_is_idempotent() {
    let registry = JobRegistry::from_current();
    let (job, starts, _) = SpyJob::looping(JobKey::HeartbeatPush, Trigger::Once);
    registry.register_and_start(job);
    settle().await;

    registry.stop(JobKey::HeartbeatPush).await;
    registry.stop(JobKey::HeartbeatPush).await;

    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert!(!registry.is_registered(JobKey::HeartbeatPush));
    assert!(!registry.is_running(JobKey::HeartbeatPush));
}

#[tokio::test]
async fn stop_before_start_unregisters_the_job() {
    let registry = JobRegistry::from_current();
    let (job, starts, _) = SpyJob::new(JobKey::HeartbeatPush, Trigger::Once);
    registry.register(job);

    registry.stop(JobKey::HeartbeatPush).await;
    settle().await;

    assert_eq!(starts.load(Ordering::SeqCst), 0);
    assert!(!registry.is_registered(JobKey::HeartbeatPush));
    assert!(!registry.is_running(JobKey::HeartbeatPush));
}

#[tokio::test]
async fn reset_without_a_running_job_is_ignored() {
    let registry = JobRegistry::from_current();
    let (job, starts, _) = SpyJob::new(JobKey::HeartbeatCountdown, Trigger::AfterQuiet(PERIOD));
    registry.register(job);

    registry.reset(JobKey::HeartbeatCountdown);
    settle().await;

    assert_eq!(starts.load(Ordering::SeqCst), 0);
    assert!(registry.is_registered(JobKey::HeartbeatCountdown));
}

#[tokio::test]
async fn unknown_jobs_are_ignored_without_panicking() {
    let registry = JobRegistry::from_current();

    registry.start(JobKey::LogReplication);
    registry.reset(JobKey::LogReplication);
    registry.stop(JobKey::LogReplication).await;
    settle().await;

    assert!(!registry.is_registered(JobKey::LogReplication));
    assert!(!registry.is_running(JobKey::LogReplication));
}
