use crate::cluster::node::persist::Persist;
use crate::cluster::node::Node;
use crate::cluster::roles::{Role, RoleSet};
use clap::Id;
use serde::{Deserialize, Serialize};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;
use whirlpool_common::util::asserts::assert_that;
use crate::cluster::Cluster;

/// The configuration of a node
#[derive(Debug, Deserialize, Serialize)]
pub struct NodeConfig {
    persist: PathBuf,
    roles: RoleSet,
    bind_address: IpAddr,
    timeout: Option<Duration>,
    cluster_addrs: Vec<SocketAddr>,
    comms_port: u16,
    client_port: u16,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            persist: PathBuf::from("node-info.json"),
            roles: RoleSet::new(),
            bind_address: IpAddr::V4(Ipv4Addr::LOCALHOST),
            timeout: None,
            cluster_addrs: vec![],
            comms_port: 16700,
            client_port: 16800,
        }
    }
}

impl NodeConfig {
    /// Creates a new, default, node config
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets where persisted info should be stored
    pub fn persist_location(mut self, path: impl AsRef<Path>) -> Self {
        self.persist = path.as_ref().to_path_buf();
        self
    }

    pub fn cluster_address<A : ToSocketAddrs>(mut self, addr: A) -> io::Result<Self> {
        self.cluster_addrs = addr.to_socket_addrs()?.collect();
        Ok(self)
    }

    /// Modifies the roles to the given list
    pub fn roles(mut self, roles: &[Role]) -> Self {
        self.roles = RoleSet::from_iter(roles.iter().copied());
        self
    }

    /// Sets the default timeout for requests
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn bind_address(mut self, bind_address: IpAddr) -> Self {
        self.bind_address = bind_address;
        self
    }
    pub fn comms_port(mut self, comms_port: u16) -> Self {
        self.comms_port = comms_port;
        self
    }
    pub fn client_port(mut self, client_port: u16) -> Self {
        self.client_port = client_port;
        self
    }

    /// Tries to a build a node from a config
    pub fn build_node(self) -> Result<Node, BuildNodeError> {
        let mut persist = Persist::open(self.persist)?;
        if persist.node_id().is_none() {
            persist.set_node_id(Uuid::new_v4());
        }


        assert_that(|| !self.roles.is_empty(), BuildNodeError::NoRolesSet)?;
        assert_that(
            || self.comms_port != self.client_port,
            BuildNodeError::CommsAndClientOnSamePort(self.client_port),
        )?;

        Ok(Node {
            id: *persist.node_id().unwrap(),
            roles: self.roles,
            bind_address: self.bind_address,
            comms_port: self.comms_port,
            client_port: self.client_port,
            persist,
            timeout: self.timeout,
            comms_socket: None,
        })
    }
}

#[derive(Debug, Error)]
pub enum BuildNodeError {
    #[error("Tried to create a node without any roles!")]
    NoRolesSet,
    #[error("comms and client port must not be same")]
    CommsAndClientOnSamePort(u16),
    #[error(transparent)]
    IoError(#[from] io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_buildable() {
        let default = NodeConfig::default();
        assert!(default.build_node().is_ok());
    }

    #[test]
    fn node_must_have_roles() {
        let roles = NodeConfig::new().roles(&[]);
        assert!(roles.build_node().is_err())
    }
}
