//! Static bootstrap configuration.
//!
//! Every node starts from a [`Config`] loaded from YAML. The file is deliberately the
//! *initial* state only: once the cluster is running, membership is owned by the Raft
//! log (see [`crate::cluster`]) and the file is not consulted again.
//!
//! The same file can be mounted into every container; `RAFTY_NODE_ID`,
//! `RAFTY_LISTEN`, `RAFTY_ADVERTISE`, `RAFTY_BOOTSTRAP` and `RAFTY_CLUSTER_ID`
//! override the per-node fields when running under Docker Compose.

use crate::cluster::{
    ClusterConfiguration, ClusterId, ClusterMember, ClusterMembership, NodeAddress,
};
use crate::node_state::NodeId;
use crate::remote_node::{ClusterClient, RemoteNode, RemoteNodeFactory};
use crate::transport::model::membership::JoinRequest;
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Deserializer, de};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::env;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_CLUSTER_ID: &str = "rafty";
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(150);
const DEFAULT_ELECTION_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub listen: NodeAddress,
    pub advertise: NodeAddress,
    pub request_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub election_timeout: Duration,
    /// `true` only for the nodes that form the cluster for the first time.
    pub bootstrap: bool,
    /// Initial members (or, for a joining node, the seed nodes).
    pub nodes: BTreeMap<NodeId, NodeAddress>,
}

impl Config {
    /// Parses and validates a YAML document.
    pub fn from_yaml(yaml: &str) -> Result<Self> {
        let raw: RawConfig = serde_saphyr::from_str(yaml)
            .map_err(|error| anyhow!("invalid configuration YAML: {error}"))?;
        Self::try_from(raw)
    }

    /// Loads a configuration file without applying environment overrides.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("unable to read configuration [{}]", path.display()))?;
        Self::from_yaml(&contents)
            .with_context(|| format!("invalid configuration [{}]", path.display()))
    }

    /// Loads a configuration file and applies `RAFTY_*` overrides used by Docker.
    pub fn load_with_env(path: impl AsRef<Path>) -> Result<Self> {
        let mut config = Self::load(path)?;
        config.apply_env_overrides()?;
        Ok(config)
    }

    /// Applies environment overrides, then re-validates the whole configuration.
    pub fn apply_env_overrides(&mut self) -> Result<()> {
        if let Some(value) = env::var_os("RAFTY_NODE_ID") {
            self.node_id = NodeId::new(value.to_string_lossy().trim());
        }
        if let Some(value) = env::var_os("RAFTY_LISTEN") {
            self.listen = value
                .to_string_lossy()
                .parse()
                .context("invalid RAFTY_LISTEN")?;
        }
        if let Some(value) = env::var_os("RAFTY_ADVERTISE") {
            self.advertise = value
                .to_string_lossy()
                .parse()
                .context("invalid RAFTY_ADVERTISE")?;
        }
        if let Some(value) = env::var_os("RAFTY_BOOTSTRAP") {
            self.bootstrap = parse_bool(&value.to_string_lossy())
                .context("invalid RAFTY_BOOTSTRAP: expected true or false")?;
        }
        if let Some(value) = env::var_os("RAFTY_CLUSTER_ID") {
            self.cluster_id = ClusterId::new(value.to_string_lossy().to_string())?;
        }
        self.validate()
    }

    /// The configuration every node is born with.
    ///
    /// A bootstrapping node starts with every listed node as a voter. A joining node
    /// starts with an *empty* voter set: it is not a member until the leader admits it,
    /// and an empty configuration can never form a quorum by accident. The seed
    /// addresses still live in [`Config::nodes`] and are used to build the transports
    /// the join job dials.
    pub fn initial_configuration(&self) -> Result<ClusterConfiguration> {
        let members = if self.bootstrap {
            self.nodes
                .iter()
                .map(|(id, address)| ClusterMember::voter(id.clone(), address.clone()))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        ClusterConfiguration::new(self.cluster_id.clone(), 0, members)
    }

    /// The membership this node starts with. See [`Config::initial_configuration`].
    pub fn initial_membership(&self) -> Result<Arc<ClusterMembership>> {
        Ok(Arc::new(ClusterMembership::new(
            self.initial_configuration()?,
        )))
    }

    /// Nodes to contact when joining, excluding this node.
    pub fn seeds(&self) -> Vec<(NodeId, NodeAddress)> {
        self.nodes
            .iter()
            .filter(|(id, _)| **id != self.node_id)
            .map(|(id, address)| (id.clone(), address.clone()))
            .collect()
    }

    /// Builds the node's [`ClusterClient`] from this static configuration.
    ///
    /// `remote_nodes` must contain a transport for every peer this node can dial
    /// (the seeds, for a joining node). When `factory` is supplied, transports for
    /// members added at runtime are built on demand.
    pub fn cluster_client(
        &self,
        remote_nodes: HashMap<NodeId, RemoteNode>,
        factory: Option<Arc<dyn RemoteNodeFactory>>,
    ) -> Result<Arc<ClusterClient>> {
        let mut client = ClusterClient::with_membership(
            self.node_id.clone(),
            self.initial_membership()?,
            Arc::new(remote_nodes),
            self.request_timeout,
        );
        if let Some(factory) = factory {
            client = client.with_factory(factory);
        }
        Ok(Arc::new(client))
    }

    /// Seed identities, ready to hand to [`crate::job::join::JoinJob`].
    pub fn seed_ids(&self) -> Vec<NodeId> {
        self.seeds().into_iter().map(|(id, _)| id).collect()
    }

    /// The join request this node sends to a seed at startup.
    pub fn join_request(&self) -> JoinRequest {
        JoinRequest::new(
            self.cluster_id.clone(),
            self.node_id.clone(),
            self.advertise.clone(),
        )
    }

    pub fn validate(&self) -> Result<()> {
        if self.node_id.as_str().trim().is_empty() {
            bail!("current-node must not be empty");
        }
        if !self.listen.is_dialable() {
            bail!(
                "listen address [{}] must include a non-zero port",
                self.listen
            );
        }
        if !self.advertise.is_dialable() {
            bail!(
                "advertise address [{}] must include a non-zero port",
                self.advertise
            );
        }
        if self.advertise.host() == "0.0.0.0" || self.advertise.host() == "::" {
            bail!(
                "advertise address [{}] is a wildcard; other nodes cannot dial it",
                self.advertise
            );
        }
        if self.nodes.is_empty() {
            bail!("at least one node must be configured");
        }

        let mut addresses = BTreeSet::new();
        for (id, address) in &self.nodes {
            if id.as_str().trim().is_empty() {
                bail!("node id must not be empty");
            }
            if !address.is_dialable() {
                bail!("node [{id}] has a non-dialable address [{address}]");
            }
            if !addresses.insert(address.clone()) {
                bail!("duplicate node address [{address}]");
            }
        }

        match self.nodes.get(&self.node_id) {
            Some(address) if address != &self.advertise => {
                bail!(
                    "current node [{}] is configured at [{}] but advertises [{}]",
                    self.node_id,
                    address,
                    self.advertise
                );
            }
            None if self.bootstrap => {
                bail!(
                    "bootstrap node [{}] must be present in the nodes list",
                    self.node_id
                );
            }
            _ => {}
        }

        if self.heartbeat_interval.is_zero() {
            bail!("heartbeat-interval must be greater than zero");
        }
        if self.election_timeout <= self.heartbeat_interval {
            bail!(
                "election-timeout [{:?}] must be greater than heartbeat-interval [{:?}]",
                self.election_timeout,
                self.heartbeat_interval
            );
        }
        if self.request_timeout.is_zero() {
            bail!("request-timeout must be greater than zero");
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    config: RawNodeConfig,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct RawNodeConfig {
    #[serde(default = "default_cluster_id")]
    cluster_id: String,
    current_node: String,
    listen: NodeAddress,
    advertise: NodeAddress,
    #[serde(
        default = "default_request_timeout",
        deserialize_with = "deserialize_duration"
    )]
    request_timeout: Duration,
    #[serde(
        default = "default_heartbeat_interval",
        deserialize_with = "deserialize_duration"
    )]
    heartbeat_interval: Duration,
    #[serde(
        default = "default_election_timeout",
        deserialize_with = "deserialize_duration"
    )]
    election_timeout: Duration,
    #[serde(default)]
    bootstrap: bool,
    nodes: Vec<BTreeMap<String, NodeAddress>>,
}

impl TryFrom<RawConfig> for Config {
    type Error = anyhow::Error;

    fn try_from(raw: RawConfig) -> Result<Self> {
        let config = raw.config;
        let cluster_id = ClusterId::new(config.cluster_id)?;

        let mut nodes = BTreeMap::new();
        for entry in config.nodes {
            if entry.len() != 1 {
                bail!("each nodes entry must contain exactly one node id");
            }
            for (id, address) in entry {
                if nodes.insert(NodeId::new(id.trim()), address).is_some() {
                    bail!("duplicate node id [{id}] in nodes list");
                }
            }
        }

        let config = Self {
            cluster_id,
            node_id: NodeId::new(config.current_node.trim()),
            listen: config.listen,
            advertise: config.advertise,
            request_timeout: config.request_timeout,
            heartbeat_interval: config.heartbeat_interval,
            election_timeout: config.election_timeout,
            bootstrap: config.bootstrap,
            nodes,
        };
        config.validate()?;
        Ok(config)
    }
}

fn default_cluster_id() -> String {
    DEFAULT_CLUSTER_ID.to_owned()
}

fn default_request_timeout() -> Duration {
    DEFAULT_REQUEST_TIMEOUT
}

fn default_heartbeat_interval() -> Duration {
    DEFAULT_HEARTBEAT_INTERVAL
}

fn default_election_timeout() -> Duration {
    DEFAULT_ELECTION_TIMEOUT
}

fn parse_bool(value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => bail!("invalid boolean [{other}]"),
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum DurationRepr {
    Text(String),
    Millis(u64),
}

fn deserialize_duration<'de, D>(deserializer: D) -> std::result::Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    match DurationRepr::deserialize(deserializer)? {
        DurationRepr::Text(value) => parse_duration(&value).map_err(de::Error::custom),
        DurationRepr::Millis(millis) => Ok(Duration::from_millis(millis)),
    }
}

/// Parses `150ms`, `2s`, `1m`, `1h` and bare numbers interpreted as milliseconds.
pub fn parse_duration(value: &str) -> Result<Duration> {
    let value = value.trim();
    if value.is_empty() {
        bail!("duration must not be empty");
    }
    let split = value
        .find(|character: char| !character.is_ascii_digit() && character != '.')
        .unwrap_or(value.len());
    let (number, unit) = value.split_at(split);
    let number: f64 = number
        .parse()
        .map_err(|_| anyhow!("invalid duration [{value}]: expected a number"))?;
    if !number.is_finite() || number <= 0.0 {
        bail!("invalid duration [{value}]: must be positive");
    }
    let millis = match unit.trim() {
        "" | "ms" => number,
        "us" => number / 1_000.0,
        "s" => number * 1_000.0,
        "m" => number * 60_000.0,
        "h" => number * 3_600_000.0,
        other => bail!("invalid duration [{value}]: unknown unit [{other}]"),
    };
    Ok(Duration::from_secs_f64(millis / 1_000.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    const REFERENCE: &str = include_str!("../config.yaml");
    const JOINER: &str = include_str!("../config.joiner.yaml");

    #[test]
    fn parses_the_reference_configuration() {
        let config = Config::from_yaml(REFERENCE).unwrap();

        assert_eq!(config.node_id, NodeId::new("node1"));
        assert_eq!(config.cluster_id.as_str(), "rafty-dev");
        assert_eq!(config.listen.to_string(), "0.0.0.0:1001");
        assert_eq!(config.advertise.to_string(), "node1:1001");
        assert_eq!(config.nodes.len(), 3);
        assert!(config.bootstrap);
        assert_eq!(config.heartbeat_interval, Duration::from_millis(150));
        assert_eq!(config.election_timeout, Duration::from_secs(1));

        let configuration = config.initial_configuration().unwrap();
        assert_eq!(configuration.voter_count(), 3);
        assert!(configuration.is_voter(&NodeId::new("node2")));
    }

    #[test]
    fn parses_the_joiner_configuration() {
        let config = Config::from_yaml(JOINER).unwrap();

        assert_eq!(config.node_id, NodeId::new("node4"));
        assert!(!config.bootstrap);
        assert_eq!(config.seed_ids().len(), 3);
        assert_eq!(config.initial_configuration().unwrap().voter_count(), 0);

        let client = config.cluster_client(Default::default(), None).unwrap();
        assert_eq!(client.configuration().index, 0);
        assert_eq!(client.configuration().voter_count(), 0);
    }

    #[test]
    fn bootstrap_configuration_builds_a_client_with_all_voters() {
        let config = Config::from_yaml(REFERENCE).unwrap();
        let client = config.cluster_client(Default::default(), None).unwrap();

        assert_eq!(client.configuration().voter_count(), 3);
        assert_eq!(client.configuration().quorum_size(), 2);
        assert!(client.configuration().is_voter(&NodeId::new("node2")));
    }

    #[test]
    fn parses_short_addresses_and_scale_suffixes() {
        let yaml = r#"
config:
  cluster-id: demo
  current-node: node1
  listen: 0.0.0.0:1001
  advertise: node1:1001
  request-timeout: 750ms
  heartbeat-interval: 100ms
  election-timeout: 1s
  bootstrap: true
  nodes:
    - node1: node1:1001
    - node2: "node2:1002"
"#;
        let config = Config::from_yaml(yaml).unwrap();

        assert_eq!(config.request_timeout, Duration::from_millis(750));
        assert_eq!(
            config.nodes[&NodeId::new("node2")].to_string(),
            "node2:1002"
        );
    }

    #[test]
    fn rejects_duplicate_addresses() {
        let yaml = r#"
config:
  cluster-id: demo
  current-node: node1
  listen: 0.0.0.0:1001
  advertise: node1:1001
  bootstrap: true
  nodes:
    - node1: node1:1001
    - node2: node1:1001
"#;
        let error = Config::from_yaml(yaml).unwrap_err();
        assert!(error.to_string().contains("duplicate node address"));
    }

    #[test]
    fn rejects_a_bootstrap_node_missing_from_the_nodes_list() {
        let yaml = r#"
config:
  cluster-id: demo
  current-node: node9
  listen: 0.0.0.0:1009
  advertise: node9:1009
  bootstrap: true
  nodes:
    - node1: node1:1001
"#;
        assert!(Config::from_yaml(yaml).is_err());
    }

    #[test]
    fn a_joining_node_may_be_absent_from_the_seed_list() {
        let yaml = r#"
config:
  cluster-id: demo
  current-node: node4
  listen: 0.0.0.0:1004
  advertise: node4:1004
  bootstrap: false
  nodes:
    - node1: node1:1001
    - node2: node2:1002
"#;
        let config = Config::from_yaml(yaml).unwrap();

        assert!(!config.bootstrap);
        assert_eq!(config.seeds().len(), 2);
        assert_eq!(config.seed_ids().len(), 2);
        assert_eq!(config.join_request().node_id, NodeId::new("node4"));
        assert_eq!(
            config.initial_configuration().unwrap().voter_count(),
            0,
            "a node that has not joined yet must not be able to form a quorum"
        );
    }

    #[test]
    fn rejects_a_wildcard_advertised_address() {
        let yaml = r#"
config:
  cluster-id: demo
  current-node: node1
  listen: 0.0.0.0:1001
  advertise: 0.0.0.0:1001
  bootstrap: true
  nodes:
    - node1: 0.0.0.0:1001
"#;
        let error = Config::from_yaml(yaml).unwrap_err();
        assert!(error.to_string().contains("wildcard"));
    }

    #[test]
    fn election_timeout_must_exceed_the_heartbeat_interval() {
        let yaml = r#"
config:
  cluster-id: demo
  current-node: node1
  listen: 0.0.0.0:1001
  advertise: node1:1001
  heartbeat-interval: 500ms
  election-timeout: 500ms
  bootstrap: true
  nodes:
    - node1: node1:1001
"#;
        assert!(Config::from_yaml(yaml).is_err());
    }

    #[test]
    fn parses_duration_units() {
        assert_eq!(parse_duration("150ms").unwrap(), Duration::from_millis(150));
        assert_eq!(parse_duration("2s").unwrap(), Duration::from_secs(2));
        assert_eq!(parse_duration("1m").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_duration("250").unwrap(), Duration::from_millis(250));
        assert!(parse_duration("2fortnights").is_err());
        assert!(parse_duration("0s").is_err());
    }
}
