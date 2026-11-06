use crate::payload::Value;
use crate::transport::model::Key;
use anyhow::Result;
use dashmap::DashMap;
use std::fmt::Debug;

pub trait Storage: Debug + Send + Sync {

    fn push(&self, key: Key, payload: Value) -> Result<()>;

}

/// A reference implementation of [`Storage`] backed by a concurrent map.
#[derive(Debug, Default)]
pub struct InMemoryStorage {
    values: DashMap<Key, Value>,
}

impl InMemoryStorage {
    #[inline]
    pub fn get(&self, key: &Key) -> Option<Value> {
        self.values.get(key).map(|value| value.clone())
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl Storage for InMemoryStorage {
    fn push(&self, key: Key, payload: Value) -> Result<()> {
        self.values.insert(key, payload);
        Ok(())
    }
}
