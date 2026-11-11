//! Static bootstrap configuration.
//!
//! Every node starts from a [`Config`] loaded from YAML. The file is deliberately the
//! *initial* state only: once the cluster is running, membership is owned by the Raft
//! log (see [`crate::cluster`]) and the file is not consulted again.
//!
//! The struct derives [`serde::Deserialize`] directly, so the YAML shape and the Rust
//! type cannot drift apart. Durations use `humantime` syntax (`150ms`, `2s`, `1m`) and
//! addresses are [`NodeAddress`] values.
//!
//! Command-line and environment overrides live in the binary (`clap`), not here; see
//! [`Config::apply_overrides`].

use crate::cluster::{
    ClusterConfiguration, ClusterId, ClusterMember, ClusterMembership, NodeAddress,
};
use crate::node_state::NodeId;
use crate::remote_node::{ClusterClient, RemoteNode, RemoteNodeFactory};
use crate::transport::model::membership::JoinRequest;
use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_CLUSTER_ID: &str = "rafty";
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(150);
const DEFAULT_ELECTION_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_cluster_id")]
    pub cluster_id: ClusterId,
    #[serde(rename = "current-node")]
    pub node_id: NodeId,
    pub listen: NodeAddress,
    pub advertise: NodeAddress,
    #[serde(
        default = "default_request_timeout",
        deserialize_with = "humantime_serde::deserialize"
    )]
    pub request_timeout: Duration,
    #[serde(
        default = "default_heartbeat_interval",
        deserialize_with = "humantime_serde::deserialize"
    )]
    pub heartbeat_interval: Duration,
    #[serde(
        default = "default_election_timeout",
        deserialize_with = "humantime_serde::deserialize"
    )]
    pub election_timeout: Duration,
    /// `true` only for the nodes that form the cluster for the first time.
    #[serde(default)]
    pub bootstrap: bool,
    /// Initial members (or, for a joining node, the seed nodes).
    pub nodes: BTreeMap<NodeId, NodeAddress>,
}

/// Values that a CLI or environment can override on top of the YAML file.
#[derive(Debug, Default, Clone)]
pub struct ConfigOverrides {
    pub cluster_id: Option<ClusterId>,
    pub node_id: Option<NodeId>,
    pub listen: Option<NodeAddress>,
    pub advertise: Option<NodeAddress>,
    pub bootstrap: Option<bool>,
}

impl Config {
    /// Parses and validates a YAML document.
    pub fn from_yaml(yaml: &str) -> Result<Self> {
        let Root { config } = serde_saphyr::from_str(yaml)
            .map_err(|error| anyhow::anyhow!("invalid configuration YAML: {error}"))?;
        config.validate()?;
        Ok(config)
    }

    /// Loads and validates a configuration file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("unable to read configuration [{}]", path.display()))?;
        Self::from_yaml(&contents)
            .with_context(|| format!("invalid configuration [{}]", path.display()))
    }

    /// Applies CLI/environment overrides, then re-validates the whole configuration.
    pub fn apply_overrides(&mut self, overrides: ConfigOverrides) -> Result<()> {
        if let Some(cluster_id) = overrides.cluster_id {
            self.cluster_id = cluster_id;
        }
        if let Some(node_id) = overrides.node_id {
            self.node_id = node_id;
        }
        if let Some(listen) = overrides.listen {
            self.listen = listen;
        }
        if let Some(advertise) = overrides.advertise {
            self.advertise = advertise;
        }
        if let Some(bootstrap) = overrides.bootstrap {
            self.bootstrap = bootstrap;
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
        ensure!(
            !self.node_id.as_str().trim().is_empty(),
            "current-node must not be empty"
        );
        ensure!(
            self.listen.is_dialable(),
            "listen address [{}] must include a non-zero port",
            self.listen
        );
        ensure!(
            self.advertise.is_dialable(),
            "advertise address [{}] must include a non-zero port",
            self.advertise
        );
        ensure!(
            !matches!(self.advertise.host(), "0.0.0.0" | "::"),
            "advertise address [{}] is a wildcard; other nodes cannot dial it",
            self.advertise
        );
        ensure!(
            !self.nodes.is_empty(),
            "at least one node must be configured"
        );

        let mut addresses = HashSet::new();
        for (id, address) in &self.nodes {
            ensure!(!id.as_str().trim().is_empty(), "node id must not be empty");
            ensure!(
                address.is_dialable(),
                "node [{id}] has a non-dialable address [{address}]"
            );
            ensure!(
                addresses.insert(address.clone()),
                "duplicate node address [{address}]"
            );
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

        ensure!(
            !self.heartbeat_interval.is_zero(),
            "heartbeat-interval must be greater than zero"
        );
        ensure!(
            self.election_timeout > self.heartbeat_interval,
            "election-timeout [{:?}] must be greater than heartbeat-interval [{:?}]",
            self.election_timeout,
            self.heartbeat_interval
        );
        ensure!(
            !self.request_timeout.is_zero(),
            "request-timeout must be greater than zero"
        );
        Ok(())
    }
}

/// The YAML document has a single top-level `config` key.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Root {
    config: Config,
}

fn default_cluster_id() -> ClusterId {
    ClusterId::new(DEFAULT_CLUSTER_ID).expect("static cluster id is valid")
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

        let client = config.cluster_client(HashMap::new(), None).unwrap();
        assert_eq!(client.configuration().index, 0);
        assert_eq!(client.configuration().voter_count(), 0);
    }

    #[test]
    fn bootstrap_configuration_builds_a_client_with_all_voters() {
        let config = Config::from_yaml(REFERENCE).unwrap();
        let client = config.cluster_client(HashMap::new(), None).unwrap();

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
    node1: node1:1001
    node2: "node2:1002"
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
    node1: node1:1001
    node2: node1:1001
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
    node1: node1:1001
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
    node1: node1:1001
    node2: node2:1002
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
    node1: 0.0.0.0:1001
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
    node1: node1:1001
"#;
        assert!(Config::from_yaml(yaml).is_err());
    }

    #[test]
    fn an_invalid_duration_is_reported() {
        let yaml = r#"
config:
  cluster-id: demo
  current-node: node1
  listen: 0.0.0.0:1001
  advertise: node1:1001
  request-timeout: two seconds
  nodes:
    node1: node1:1001
"#;
        assert!(Config::from_yaml(yaml).is_err());
    }

    #[test]
    fn overrides_are_applied_before_validation() {
        let mut config = Config::from_yaml(REFERENCE).unwrap();
        config
            .apply_overrides(ConfigOverrides {
                node_id: Some(NodeId::new("node2")),
                advertise: Some("node2:1002".parse().unwrap()),
                listen: Some("0.0.0.0:1002".parse().unwrap()),
                ..ConfigOverrides::default()
            })
            .unwrap();

        assert_eq!(config.node_id, NodeId::new("node2"));
        assert_eq!(config.advertise.to_string(), "node2:1002");
    }
}
