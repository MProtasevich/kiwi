use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::node_state::InstanceId;
use crate::payload::Key;

#[derive(Debug, Serialize, Deserialize)]
pub enum CommandReq {
    Set(Payload), // fixme wrapping with Arc may have sense
    Get(Key),
    Update, // reserved for future implementation
    Delete(Key),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Payload {
    Integer(i64),
    Float(f64),
    String(String),
    DateTime(DateTime<Utc>),
    Object, // reserved for future implementation
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VoteResp {
    pub instance_id: InstanceId,
    pub term: u64,
    pub voted: bool,
}
