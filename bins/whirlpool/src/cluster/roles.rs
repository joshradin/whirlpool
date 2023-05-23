//! Use to differentiate between different things a node in a cluster can do.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// A role is a set of actions a node within a cluster can perform. However, the node is not
/// required to fulfill the responsibilities of that role. For example, only one node should
/// act as a [cluster manager](Role::ClusterManager).
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum Role {
    /// Main driver of the cluster. Does not, by default, have access to CRUD operations, and must
    /// interact with a node that does.
    ClusterManager,
    /// A client has no ability to store data, and just send data to a node. Essentially it's job is
    /// to expose the API.
    Client,
    /// Can perform CRUD operations on data.
    Data,
}

/// Defines the set of roles a node performs
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct RoleSet {
    set: HashSet<Role>,
}

impl Default for RoleSet {
    /// Create a role-set with all roles active.
    fn default() -> Self {
        Self::from_iter([Role::Data, Role::ClusterManager, Role::Client])
    }
}

impl RoleSet {
    /// Create a role-set with all roles active.
    pub fn new() -> Self {
        Self::default()
    }
    /// Check if the role set contains a role
    pub fn has_role(&self, role: Role) -> bool {
        self.set.contains(&role)
    }

    /// Check if this role-set contains all roles listed
    pub fn has_roles(&self, roles: &[Role]) -> bool {
        roles.iter().all(|role| self.has_role(*role))
    }

    /// Gets the number of roles within this set
    pub fn len(&self) -> usize {
        self.set.len()
    }

    /// Gets whether this role set has no roles
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    /// Gets an iterator of all the roles within this set.
    pub fn roles(&self) -> impl IntoIterator<Item = &Role> {
        self.set.iter()
    }
}

impl FromIterator<Role> for RoleSet {
    fn from_iter<T: IntoIterator<Item = Role>>(iter: T) -> Self {
        Self {
            set: HashSet::from_iter(iter),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster::roles::{Role, RoleSet};

    #[test]
    fn default_has_all_roles() {
        let def = RoleSet::default();
        assert!(def.has_roles(&[Role::ClusterManager, Role::Client, Role::Data]));
    }
}
