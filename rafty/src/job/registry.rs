use async_trait::async_trait;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use log::warn;
use std::fmt::Debug;
use std::range::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::watch;
use tokio::task::JoinHandle;

const AVAILABLE_JOBS_COUNT: usize = 3;
// const AVAILABLE_JOBS_COUNT: usize = std::mem::variant_count::<JobKey>();

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
    /// Call once after a random delay uniformly drawn from range;
    /// `reset` restarts the timer with a fresh delay.
    ///
    /// The bounds are swapped when `min > max`, so the range is always valid.
    AfterQuiet(RangeInclusive<Duration>),
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

/// Holds either an owned runtime or a handle to an externally managed one.
///
/// Tests need to spawn jobs on the *current* (possibly time-paused) runtime, while
/// the binary may hand the registry its own runtime.
#[derive(Debug)]
enum Executor {
    Owned(Runtime),
    Shared(Handle),
}

impl Executor {
    fn spawn<F>(&self, future: F) -> JoinHandle<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        match self {
            Executor::Owned(runtime) => runtime.spawn(future),
            Executor::Shared(handle) => handle.spawn(future),
        }
    }
}

/// Everything needed to cancel a running job. Owned by the registry, never by the job.
#[derive(Debug)]
struct Running {
    resets: Option<watch::Sender<u64>>,
    handle: JoinHandle<()>,
}

#[derive(Debug)]
pub struct JobRegistry {
    registered_jobs: DashMap<JobKey, Arc<dyn Job>>,
    running_jobs: DashMap<JobKey, Running>,
    executor: Executor,
}

impl JobRegistry {

    pub fn new(runtime: Runtime) -> Self {
        Self::with_executor(Executor::Owned(runtime))
    }

    /// Creates a registry that spawns jobs on the caller's current runtime.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime context.
    pub fn from_current() -> Self {
        Self::with_executor(Executor::Shared(Handle::current()))
    }

    fn with_executor(executor: Executor) -> Self {
        Self {
            registered_jobs: DashMap::with_capacity(AVAILABLE_JOBS_COUNT),
            running_jobs: DashMap::with_capacity(AVAILABLE_JOBS_COUNT),
            executor,
        }
    }

    #[inline]
    pub fn is_registered(&self, job_key: JobKey) -> bool {
        self.registered_jobs.contains_key(&job_key)
    }

    /// A job is running while its driver task has not finished or been aborted.
    #[inline]
    pub fn is_running(&self, job_key: JobKey) -> bool {
        self.running_jobs
            .get(&job_key)
            .is_some_and(|running| !running.handle.is_finished())
    }

    pub fn register(&self, job: Arc<dyn Job>) {
        self.registered_jobs.insert(job.key(), job);
    }

    pub fn register_and_start(&self, job: Arc<dyn Job>) {
        let job_key = job.key();
        self.register(job);
        self.start(job_key);
    }

    pub fn start(&self, job_key: JobKey) {
        let registration = self.registered_jobs
            .get(&job_key)
            .map(|job| Arc::clone(&*job));

        let Some(job) = registration else {
            warn!("No registered job found for {job_key:?}");
            return;
        };

        match self.running_jobs.entry(job_key) {
            // A job that returned or was aborted can be started again.
            Entry::Occupied(mut slot) if slot.get().handle.is_finished() => {
                slot.insert(self.spawn_driver(job));
            }
            Entry::Occupied(_) => warn!("Job already running for {job_key:?}"),
            Entry::Vacant(slot) => {
                slot.insert(self.spawn_driver(job));
            }
        }
    }

    /// Restarts the deadline of a running job. Works exclusively for `AfterQuiet`.
    /// Ignored by other triggers.
    pub fn reset(&self, job_key: JobKey) {
        let sender = self.running_jobs.get(&job_key)
            .and_then(|job_control| job_control.resets.clone());
        match sender {
            Some(tx) => {
                let (next, _) = (*tx.borrow()).overflowing_add(1);
                let _ = tx.send(next);
            }
            None => warn!("No running job found for {job_key:?}"),
        }
    }

    /// Stops a job: unregisters it and aborts its task.
    ///
    /// Returns once the task has been dropped, so no job code can run after this call.
    pub async fn stop(&self, job_key: JobKey) {
        self.registered_jobs.remove(&job_key);

        if let Some((_, Running { handle, .. })) = self.running_jobs.remove(&job_key) {
            handle.abort();
            let _ = handle.await;
        };
    }

    fn spawn_driver(&self, job: Arc<dyn Job>) -> Running {
        let (reset_tx, reset_rx) = match job.trigger() {
            Trigger::Once | Trigger::Every(_) => (None, None),
            Trigger::AfterQuiet(_) => {
                let (tx, rx) = watch::channel(0u64);
                (Some(tx), Some(rx))
            },
        };

        let handle = self.executor.spawn(launch(job, reset_rx));

        Running { resets: reset_tx, handle }
    }

}

async fn launch(job: Arc<dyn Job>, resets: Option<watch::Receiver<u64>>) {
    use tokio::time;

    match job.trigger() {
        Trigger::Once => {
            job.execute().await;
        }
        Trigger::Every(period) => {
            let mut ticker = time::interval(period);
            loop {
                ticker.tick().await;
                if job.execute().await == Control::Done {
                    break;
                }
            }
        }
        Trigger::AfterQuiet(range) => {
            let old_range: std::ops::RangeInclusive<_> = range.into();
            let mut resets = resets.expect("Receiver is missing for AfterQuiet job");
            let deadline = time::sleep(rand::random_range(old_range.clone()));
            tokio::pin!(deadline);
            loop {
                let old_range = old_range.clone();
                tokio::select! {
                    _ = resets.changed() => {
                        deadline.as_mut().reset(time::Instant::now() + rand::random_range(old_range));
                    }
                    _ = &mut deadline => {
                        if job.execute().await == Control::Done {
                            break;
                        }
                        deadline.as_mut().reset(time::Instant::now() + rand::random_range(old_range));
                    }
                }
            }
        }
    }
}
