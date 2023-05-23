use crate::cluster::Cluster;
use log::{debug, trace};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Persisted data
#[derive(Debug)]
pub struct Persist {
    path: PathBuf,
    inner: PersistInner,
}

impl Persist {
    /// Opens a persist from a file location. if the location does not exist,
    /// then the default persist is used.
    pub fn open<P>(path: P) -> Result<Self, io::Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().to_path_buf();

        if path.exists() {
            let file = File::open(&path)?;
            let inner: PersistInner = serde_json::from_reader(file)?;
            Ok(Self { path, inner })
        } else {
            Ok(Self {
                path,
                inner: Default::default(),
            })
        }
    }

    pub fn node_id(&self) -> Option<&Uuid> {
        self.inner.node_id.as_ref()
    }

    pub fn set_node_id(&mut self, id: Uuid) {
        self.inner.node_id = Some(id);
        self.flush().expect("could not change node id");
    }

    pub fn cluster(&self) -> Option<&Cluster> {
        self.inner.cluster.as_ref()
    }

    /// Modifies a cluster, if already present.
    pub fn modify_cluster<F: FnOnce(&mut Cluster)>(&mut self, callback: F) {
        if let Some(cluster) = self.inner.cluster.as_mut() {
            callback(cluster);
            self.flush().expect("could not change cluster id");
        }
    }

    pub fn set_cluster(&mut self, cluster: Cluster) {
        self.inner.cluster = Some(cluster);
        self.flush().expect("could not change cluster id");
    }

    /// Flushes the persist to the storage file. Automatically run at drop.
    pub fn flush(&mut self) -> Result<(), io::Error> {
        trace!("flushing contents of persist to {:?}", self.path);
        let mut file = File::create(&self.path)?;
        serde_json::to_writer_pretty(&mut file, &self.inner)?;
        Ok(())
    }
}

impl Drop for Persist {
    fn drop(&mut self) {
        drop(self.flush());
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistInner {
    node_id: Option<Uuid>,
    cluster: Option<Cluster>,
}
