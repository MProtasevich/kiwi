use std::fmt::Debug;

pub trait Payload: Debug {
}

#[derive(Debug, Default)]
pub(crate) struct Value(u64);
impl Payload for Value {}

pub struct Uncommitted;
pub struct Committed;

pub type ValueId = String;

#[derive(Debug)]
pub enum Command {
    Set(Box<dyn Payload>),
    Get(ValueId),
    Delete(ValueId),
}

#[derive(Debug)]
pub enum State {
    Committed,
    Uncommitted,
}

#[derive(Debug)]
pub struct Vault {
    pub command: Command,
    pub state: State,
}

impl Vault {
    pub fn new(value: Box<dyn Payload>) -> Vault {
        Vault { command: Command::Set(value), state: State::Uncommitted }
    }

    pub fn commit(&mut self) {
        self.state = State::Committed;
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
