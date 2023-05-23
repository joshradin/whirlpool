use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::net::{SocketAddr, ToSocketAddrs};
use tokio::io;
use uuid::Uuid;
use crate::cluster::roles::{Role, RoleSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cluster {
    id: Uuid,
    manager: Uuid,
    members: HashMap<Uuid, SocketAddr>,
}

impl Cluster {
    /// Creates a new cluster, owned by this node
    pub fn new(manager: Uuid) -> Self {
        Self {
            id: Uuid::new_v4(),
            manager,
            members: HashMap::new()
        }
    }

    /// Gets the id of the cluster
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Gets the current manager of the cluster
    pub fn manager(&self) -> Uuid {
        self.manager
    }

    /// The members of a cluster. should never contain the owning node.
    pub fn members(&self) -> impl IntoIterator<Item=&Uuid> {
        self.members.keys()
    }

    pub fn get_address(&self, node: &Uuid) -> Option<&SocketAddr> {
        self.members.get(node)
    }

    pub fn join_into(&mut self, cluster: &Cluster) {
        todo!()
    }

    pub fn absorb(&mut self, cluster: &Cluster) {
        todo!()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MemberInfo {
    addr: SocketAddr,
    roles: RoleSet
}

