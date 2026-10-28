use async_trait::async_trait;
use dashmap::DashMap;
use log::warn;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tokio::task::AbortHandle;
use tokio::time::timeout;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub enum JobKey {
    HeartbeatPush,
    HeartbeatCountdown,
    LogReplication,
}

#[async_trait]
pub trait Job: Debug + Send + Sync + 'static {
    async fn start(&mut self);
    async fn reset(&self) {}
    async fn stop(&self) {}
}

#[derive(Debug)]
pub struct JobRegistry {
    registered_jobs: DashMap<JobKey, Arc<Mutex<dyn Job>>>,
    launched_jobs: DashMap<JobKey, AbortHandle>,
    runtime: Runtime,
}

const MAX_TIME_OUT: Duration = Duration::from_secs(1);

impl JobRegistry {

    pub fn new(runtime: Runtime) -> Self {
        Self {
            registered_jobs: DashMap::new(),
            launched_jobs: DashMap::new(),
            runtime,
        }
    }

    pub fn register(&self, job_key: JobKey, job: impl Job) {
        self.registered_jobs.insert(job_key, Arc::new(Mutex::new(job)));
    }

    pub fn register_and_start(&self, job_key: JobKey, job: impl Job) {
        self.register(job_key, job);
        self.start(job_key);
    }

    #[inline]
    fn dispatch<F, Fut>(&self, job_key: JobKey, preserve: bool, op: F)
    where
        F: Fn(Arc<Mutex<dyn Job>>) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        match self.registered_jobs.get(&job_key) {
            Some(job) if !self.launched_jobs.contains_key(&job_key) => {
                let join_handle = self.runtime.spawn(op(Arc::clone(&job)));
                if preserve {
                    self.launched_jobs.insert(job_key, join_handle.abort_handle());
                }
            },
            Some(_) => warn!("Job already registered for {job_key:?}"),
            None => warn!("No registered job found for {job_key:?}"),
        }
    }

    pub fn start(&self, job_key: JobKey) {
        self.dispatch(job_key, true, async |job| job.lock().await.start().await);
    }

    pub fn reset(&self, job_key: JobKey) {
        self.dispatch(job_key, false, async |job| { let _ = timeout(MAX_TIME_OUT, job.lock().await.reset()).await; });
    }

    pub fn stop(&self, job_key: JobKey) {
        self.dispatch(job_key, false, async |job| { let _ = timeout(MAX_TIME_OUT, job.lock().await.stop()).await; });
        match self.launched_jobs.remove(&job_key) {
            Some((_, abort_handle)) => abort_handle.abort(),
            None => warn!("No Job has been launched for {job_key:?}"),
        }
        self.registered_jobs.remove(&job_key);
    }

}
