//! Cluster membership: identity, addresses and replicated configuration.
//!
//! Raft distinguishes three things that are easy to conflate:
//!
//! * **Identity** — [`NodeId`] is stable for the lifetime of a node.
//! * **Address** — [`NodeAddress`] is how other nodes dial it. In Docker this is a
//!   service name, not a container IP, because container IPs change on restart.
//! * **Membership** — which nodes are voters or learners. This is replicated as a
//!   [`ConfigurationEntry`] and applied to a [`ClusterMembership`].
//!
//! [`ClusterMembership::new`] is the state a node is born with (the static config from
//! `config.yaml`). From then on, the only ways it changes are a replicated
//! [`ConfigurationEntry`] or a join snapshot received from the leader.

use crate::node_state::NodeId;
use anyhow::{Result, anyhow, bail};
use arc_swap::ArcSwap;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

/// Identifier of a Raft cluster.
///
/// A node refuses to join, or to apply a configuration from, a different cluster.
/// This is what stops a restarted node with an empty data directory from silently
/// forming a second cluster that later merges with the first one.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClusterId(Arc<str>);

impl ClusterId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let value = value.trim();
        if value.is_empty() {
            bail!("cluster id must not be empty");
        }
        Ok(Self(Arc::from(value)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for ClusterId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for ClusterId {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        Self::new(value)
    }
}

impl Serialize for ClusterId {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ClusterId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// A dialable `host:port` pair.
///
/// Serialized as a string (`"node2:1002"`), but accepted from YAML as either that
/// string or a `{ host, port }` map. Prefer DNS/service names over IP addresses so that
/// a container can be recreated without invalidating the rest of the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeAddress {
    host: String,
    port: u16,
}

impl NodeAddress {
    pub fn new(host: impl Into<String>, port: u16) -> Result<Self> {
        let host = host.into();
        let host = host.trim();
        if host.is_empty() {
            bail!("node address host must not be empty");
        }
        Ok(Self {
            host: host.to_owned(),
            port,
        })
    }

    /// Address used by in-process clients that do not dial a socket.
    pub fn placeholder(id: &NodeId) -> Self {
        Self {
            host: id.as_str().to_owned(),
            port: 0,
        }
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// A wildcard listen address (`0.0.0.0:1001`) is not a valid advertised address.
    pub fn is_dialable(&self) -> bool {
        self.port != 0
    }
}

impl Display for NodeAddress {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        // Bracket IPv6 literals so that `host:port` stays unambiguous.
        if self.host.contains(':') && !self.host.starts_with('[') {
            write!(formatter, "[{}]:{}", self.host, self.port)
        } else {
            write!(formatter, "{}:{}", self.host, self.port)
        }
    }
}

impl FromStr for NodeAddress {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        let value = value.trim();
        let (host, port) = if let Some(rest) = value.strip_prefix('[') {
            let (host, rest) = rest
                .split_once(']')
                .ok_or_else(|| anyhow!("invalid IPv6 address [{value}]: missing ']'"))?;
            let port = rest
                .strip_prefix(':')
                .ok_or_else(|| anyhow!("invalid address [{value}]: missing port"))?;
            (host, port)
        } else {
            value
                .rsplit_once(':')
                .ok_or_else(|| anyhow!("invalid address [{value}]: expected host:port"))?
        };
        let port: u16 = port
            .parse()
            .map_err(|_| anyhow!("invalid port in address [{value}]"))?;
        if port == 0 {
            bail!("invalid address [{value}]: port must not be zero");
        }
        Self::new(host, port)
    }
}

impl Serialize for NodeAddress {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for NodeAddress {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Text(String),
            Map { host: String, port: u16 },
        }

        match Repr::deserialize(deserializer)? {
            Repr::Text(text) => text.parse().map_err(de::Error::custom),
            Repr::Map { host, port } => Self::new(host, port).map_err(de::Error::custom),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MemberRole {
    Voter,
    Learner,
}

impl MemberRole {
    pub fn is_voter(self) -> bool {
        matches!(self, Self::Voter)
    }

    pub fn is_learner(self) -> bool {
        matches!(self, Self::Learner)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterMember {
    pub id: NodeId,
    pub address: NodeAddress,
    pub role: MemberRole,
}

impl ClusterMember {
    pub fn voter(id: NodeId, address: NodeAddress) -> Self {
        Self {
            id,
            address,
            role: MemberRole::Voter,
        }
    }

    pub fn learner(id: NodeId, address: NodeAddress) -> Self {
        Self {
            id,
            address,
            role: MemberRole::Learner,
        }
    }

    pub fn is_voter(&self) -> bool {
        self.role.is_voter()
    }

    pub fn is_learner(&self) -> bool {
        self.role.is_learner()
    }
}

/// The full membership of a cluster at a given configuration index.
///
/// The index is the Raft log index of the entry that produced this configuration.
/// Bootstrap configuration has index `0`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterConfiguration {
    pub cluster_id: ClusterId,
    pub index: u64,
    pub members: BTreeMap<NodeId, ClusterMember>,
}

impl ClusterConfiguration {
    pub fn new(
        cluster_id: ClusterId,
        index: u64,
        members: impl IntoIterator<Item = ClusterMember>,
    ) -> Result<Self> {
        let mut by_id = BTreeMap::new();
        for member in members {
            if by_id.insert(member.id.clone(), member).is_some() {
                bail!("duplicate node id in cluster configuration");
            }
        }
        let configuration = Self {
            cluster_id,
            index,
            members: by_id,
        };
        configuration.validate()?;
        Ok(configuration)
    }

    pub fn validate(&self) -> Result<()> {
        let mut addresses = BTreeSet::new();
        for (id, member) in &self.members {
            if id != &member.id {
                bail!("member key [{id}] does not match member id [{}]", member.id);
            }
            // Placeholder addresses (used by in-process transports) are not dialable and
            // are allowed to repeat. Real addresses must be unique.
            if member.address.is_dialable() && !addresses.insert(member.address.clone()) {
                bail!("duplicate node address [{}]", member.address);
            }
        }
        Ok(())
    }

    pub fn members(&self) -> impl Iterator<Item = &ClusterMember> {
        self.members.values()
    }

    pub fn voters(&self) -> impl Iterator<Item = &ClusterMember> {
        self.members.values().filter(|member| member.is_voter())
    }

    pub fn learners(&self) -> impl Iterator<Item = &ClusterMember> {
        self.members.values().filter(|member| member.is_learner())
    }

    pub fn voter_count(&self) -> usize {
        self.voters().count()
    }

    /// Number of nodes that form a majority, including the local node when it is a voter.
    pub fn quorum_size(&self) -> usize {
        let voters = self.voter_count();
        if voters == 0 { 0 } else { voters / 2 + 1 }
    }

    pub fn get(&self, node_id: &NodeId) -> Option<&ClusterMember> {
        self.members.get(node_id)
    }

    pub fn contains(&self, node_id: &NodeId) -> bool {
        self.members.contains_key(node_id)
    }

    pub fn is_voter(&self, node_id: &NodeId) -> bool {
        self.get(node_id).is_some_and(ClusterMember::is_voter)
    }

    pub fn is_learner(&self, node_id: &NodeId) -> bool {
        self.get(node_id).is_some_and(ClusterMember::is_learner)
    }

    pub fn address_of(&self, node_id: &NodeId) -> Option<&NodeAddress> {
        self.get(node_id).map(|member| &member.address)
    }

    /// Computes the configuration produced by `change` without mutating `self`.
    ///
    /// This is the validation used by both the leader (before broadcasting) and the
    /// follower (before installing).
    pub fn applied(&self, change: &ConfigChange) -> Result<Self> {
        let mut next = self.clone();
        next.index = self
            .index
            .checked_add(1)
            .ok_or_else(|| anyhow!("configuration index overflow"))?;

        match change {
            ConfigChange::AddLearner { node_id, address } => {
                if next.members.contains_key(node_id) {
                    bail!("node [{node_id}] is already a member");
                }
                if !address.is_dialable() {
                    bail!("node [{node_id}] has a non-dialable address [{address}]");
                }
                if next
                    .members
                    .values()
                    .any(|member| &member.address == address)
                {
                    bail!("address [{address}] is already used by another member");
                }
                next.members.insert(
                    node_id.clone(),
                    ClusterMember::learner(node_id.clone(), address.clone()),
                );
            }
            ConfigChange::Promote { node_id } => {
                let member = next
                    .members
                    .get_mut(node_id)
                    .ok_or_else(|| anyhow!("node [{node_id}] is not a member"))?;
                if member.is_voter() {
                    bail!("node [{node_id}] is already a voter");
                }
                member.role = MemberRole::Voter;
            }
            ConfigChange::Remove { node_id } => {
                let member = next
                    .members
                    .get(node_id)
                    .ok_or_else(|| anyhow!("node [{node_id}] is not a member"))?;
                if member.is_voter() && next.voter_count() == 1 {
                    bail!("cannot remove the last voter [{node_id}]");
                }
                next.members.remove(node_id);
            }
        }

        next.validate()?;
        Ok(next)
    }
}

/// A single membership change. Raft allows only one uncommitted change at a time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ConfigChange {
    AddLearner {
        node_id: NodeId,
        address: NodeAddress,
    },
    Promote {
        node_id: NodeId,
    },
    Remove {
        node_id: NodeId,
    },
}

/// A Raft configuration entry.
///
/// It is replicated like any other log entry; once acknowledged by a quorum it is
/// applied to the [`ClusterMembership`] and the index becomes the new configuration
/// index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigurationEntry {
    pub index: u64,
    pub term: u64,
    pub change: ConfigChange,
}

impl ConfigurationEntry {
    pub fn new(index: u64, term: u64, change: ConfigChange) -> Self {
        Self {
            index,
            term,
            change,
        }
    }
}

/// Optional durable record of committed configuration entries.
///
/// The repository has no durable log yet, so the default journal is in-memory. The
/// trait exists so a persistent implementation can be plugged in without changing the
/// membership code.
pub trait ConfigurationJournal: Debug + Send + Sync {
    fn record(&self, entry: &ConfigurationEntry) -> Result<()>;
    fn entries(&self) -> Result<Vec<ConfigurationEntry>>;
}

#[derive(Debug, Default)]
pub struct InMemoryConfigurationJournal {
    entries: Mutex<Vec<ConfigurationEntry>>,
}

impl ConfigurationJournal for InMemoryConfigurationJournal {
    fn record(&self, entry: &ConfigurationEntry) -> Result<()> {
        self.entries
            .lock()
            .map_err(|_| anyhow!("configuration journal is poisoned"))?
            .push(entry.clone());
        Ok(())
    }

    fn entries(&self) -> Result<Vec<ConfigurationEntry>> {
        Ok(self
            .entries
            .lock()
            .map_err(|_| anyhow!("configuration journal is poisoned"))?
            .clone())
    }
}

/// Thread-safe holder for the currently applied [`ClusterConfiguration`].
///
/// Readers never block; writers replace the whole configuration with `ArcSwap`, which
/// matches how Raft applies a configuration entry atomically.
#[derive(Debug)]
pub struct ClusterMembership {
    configuration: ArcSwap<ClusterConfiguration>,
    journal: Option<Arc<dyn ConfigurationJournal>>,
}

impl ClusterMembership {
    pub fn new(configuration: ClusterConfiguration) -> Self {
        Self {
            configuration: ArcSwap::from_pointee(configuration),
            // Configuration changes are rare, so keeping them in memory by default
            // makes the applied entries inspectable without any extra wiring.
            journal: Some(Arc::new(InMemoryConfigurationJournal::default())),
        }
    }

    /// Creates a membership that does not keep a history of applied entries.
    pub fn without_journal(configuration: ClusterConfiguration) -> Self {
        Self {
            configuration: ArcSwap::from_pointee(configuration),
            journal: None,
        }
    }

    pub fn with_journal(
        configuration: ClusterConfiguration,
        journal: Arc<dyn ConfigurationJournal>,
    ) -> Self {
        Self {
            configuration: ArcSwap::from_pointee(configuration),
            journal: Some(journal),
        }
    }

    pub fn configuration(&self) -> Arc<ClusterConfiguration> {
        self.configuration.load_full()
    }

    /// Committed configuration entries applied to this membership, oldest first.
    pub fn entries(&self) -> Result<Vec<ConfigurationEntry>> {
        match &self.journal {
            Some(journal) => journal.entries(),
            None => Ok(Vec::new()),
        }
    }

    /// Replaces the configuration with an authoritative snapshot (e.g. from a join
    /// response). Older snapshots are rejected so a delayed response cannot roll
    /// membership backwards.
    pub fn install_snapshot(&self, snapshot: ClusterConfiguration) -> Result<()> {
        let current = self.configuration();
        if snapshot.cluster_id != current.cluster_id {
            bail!(
                "refusing configuration for cluster [{}] on cluster [{}]",
                snapshot.cluster_id,
                current.cluster_id
            );
        }
        if snapshot.index < current.index {
            bail!(
                "refusing stale configuration snapshot: index {} < {}",
                snapshot.index,
                current.index
            );
        }
        snapshot.validate()?;
        self.configuration.store(Arc::new(snapshot));
        Ok(())
    }

    /// Records an entry that has been committed by a quorum.
    ///
    /// [`ClusterMembership::apply`] records automatically; this is for the leader path,
    /// where the configuration is installed as a snapshot after the quorum ack.
    pub fn record_committed(&self, entry: &ConfigurationEntry) -> Result<()> {
        if let Some(journal) = &self.journal {
            journal.record(entry)?;
        }
        Ok(())
    }

    /// Applies a replicated configuration entry.
    ///
    /// Returns the current configuration unchanged when the entry was already applied
    /// (at-least-once delivery) and fails when entries were skipped, because applying a
    /// later change without its predecessor could corrupt membership.
    pub fn apply(&self, entry: &ConfigurationEntry) -> Result<Arc<ClusterConfiguration>> {
        let current = self.configuration();
        if entry.index <= current.index {
            return Ok(current);
        }
        if entry.index != current.index + 1 {
            bail!(
                "configuration entry gap: got index {}, expected {}",
                entry.index,
                current.index + 1
            );
        }
        let next = current.applied(&entry.change)?;
        if let Some(journal) = &self.journal {
            journal.record(entry)?;
        }
        self.configuration.store(Arc::new(next.clone()));
        Ok(Arc::new(next))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: &str) -> NodeId {
        NodeId::new(value)
    }

    fn address(value: &str) -> NodeAddress {
        value.parse().unwrap()
    }

    fn cluster_id() -> ClusterId {
        ClusterId::new("test").unwrap()
    }

    fn three_voters() -> ClusterConfiguration {
        ClusterConfiguration::new(
            cluster_id(),
            0,
            [
                ClusterMember::voter(id("node1"), address("node1:1001")),
                ClusterMember::voter(id("node2"), address("node2:1002")),
                ClusterMember::voter(id("node3"), address("node3:1003")),
            ],
        )
        .unwrap()
    }

    #[test]
    fn adding_a_learner_increments_the_index_and_keeps_voters() {
        let next = three_voters()
            .applied(&ConfigChange::AddLearner {
                node_id: id("node4"),
                address: address("node4:1004"),
            })
            .unwrap();

        assert_eq!(next.index, 1);
        assert_eq!(next.voter_count(), 3);
        assert!(next.is_learner(&id("node4")));
    }

    #[test]
    fn promoting_a_learner_makes_it_a_voter() {
        let with_learner = three_voters()
            .applied(&ConfigChange::AddLearner {
                node_id: id("node4"),
                address: address("node4:1004"),
            })
            .unwrap();
        let promoted = with_learner
            .applied(&ConfigChange::Promote {
                node_id: id("node4"),
            })
            .unwrap();

        assert_eq!(promoted.voter_count(), 4);
        assert!(promoted.is_voter(&id("node4")));
    }

    #[test]
    fn removing_the_last_voter_is_rejected() {
        let single = ClusterConfiguration::new(
            cluster_id(),
            0,
            [ClusterMember::voter(id("node1"), address("node1:1001"))],
        )
        .unwrap();

        let error = single
            .applied(&ConfigChange::Remove {
                node_id: id("node1"),
            })
            .unwrap_err();

        assert!(error.to_string().contains("last voter"));
    }

    #[test]
    fn duplicate_addresses_are_rejected() {
        let error = three_voters()
            .applied(&ConfigChange::AddLearner {
                node_id: id("node4"),
                address: address("node2:1002"),
            })
            .unwrap_err();

        assert!(error.to_string().contains("already used"));
    }

    #[test]
    fn membership_ignores_replayed_entries_and_rejects_gaps() {
        let membership = ClusterMembership::new(three_voters());
        let entry = ConfigurationEntry::new(
            1,
            3,
            ConfigChange::AddLearner {
                node_id: id("node4"),
                address: address("node4:1004"),
            },
        );

        membership.apply(&entry).unwrap();
        membership.apply(&entry).unwrap();
        assert_eq!(membership.configuration().index, 1);

        let gap = ConfigurationEntry::new(
            3,
            3,
            ConfigChange::Promote {
                node_id: id("node4"),
            },
        );
        assert!(membership.apply(&gap).is_err());
    }

    #[test]
    fn configuration_round_trips_through_json() {
        let configuration = three_voters();
        let json = serde_json::to_string(&configuration).unwrap();
        let decoded: ClusterConfiguration = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded, configuration);
        assert!(json.contains("node2:1002"));
    }

    #[test]
    fn journal_records_applied_entries() {
        let journal = Arc::new(InMemoryConfigurationJournal::default());
        let membership = ClusterMembership::with_journal(three_voters(), journal.clone());
        let entry = ConfigurationEntry::new(
            1,
            2,
            ConfigChange::AddLearner {
                node_id: id("node4"),
                address: address("node4:1004"),
            },
        );

        membership.apply(&entry).unwrap();

        assert_eq!(journal.entries().unwrap(), vec![entry]);
    }

    #[test]
    fn snapshot_from_another_cluster_is_rejected() {
        let membership = ClusterMembership::new(three_voters());
        let mut snapshot = three_voters();
        snapshot.cluster_id = ClusterId::new("other").unwrap();

        assert!(membership.install_snapshot(snapshot).is_err());
    }

    #[test]
    fn node_address_accepts_strings_and_maps() {
        #[derive(Deserialize)]
        struct Holder {
            address: NodeAddress,
        }

        let from_text: Holder = serde_saphyr::from_str("address: node2:1002").unwrap();
        let from_map: Holder =
            serde_saphyr::from_str("address:\n  host: node2\n  port: 1002").unwrap();

        assert_eq!(from_text.address, address("node2:1002"));
        assert_eq!(from_map.address, address("node2:1002"));
        assert_eq!(from_text.address.to_string(), "node2:1002");
    }
}
