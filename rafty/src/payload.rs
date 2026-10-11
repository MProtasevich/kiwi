use std::fmt::Debug;
use std::sync::Arc;

pub type ValueId = Arc<str>;

pub trait Key: Send + Sync {
    fn key(&self) -> ValueId;
}

pub trait Payload: Debug + Key {
}

#[derive(Debug, Default)]
pub(crate) struct Value(u64);

impl Key for Value {
    fn key(&self) -> ValueId {
        Arc::from(self.0.to_string())
    }
}

impl Payload for Value {
}

pub struct Uncommitted;
pub struct Committed;

#[derive(Debug)]
pub enum Command {
    Set(Box<dyn Payload>),
    Get(ValueId),
    Delete(ValueId),
}

#[derive(Debug)]
pub enum PersistenceState {
    Committed,
    Uncommitted,
}

#[derive(Debug)]
pub struct Log {
    pub command: Command,
    pub state: PersistenceState,
}

impl Log {
    pub fn new(value: Box<dyn Payload>) -> Log {
        Log { command: Command::Set(value), state: PersistenceState::Uncommitted }
    }

    pub fn commit(&mut self) {
        self.state = PersistenceState::Committed;
    }
}

// impl Container<Uncommitted> {
//     pub fn commit(self) -> Container<Committed> {
//         Container {
//             committed: self.uncommitted.expect("Uncommitted value is absent"),
//             uncommitted: None,
//             _phase: PhantomData
//         }
//     }
// }
//
// impl Container<Committed> {
//     pub fn accept_new(self, value: impl Payload + 'static) -> Container<Uncommitted> {
//         Container {
//             committed: self.committed,
//             uncommitted: Some(Box::new(value)),
//             _phase: PhantomData
//         }
//     }
// }
