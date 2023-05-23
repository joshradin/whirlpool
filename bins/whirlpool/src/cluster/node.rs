//! Defines the mechanisms that control a node

use crate::cluster::node::config::NodeConfig;
use crate::cluster::node::frame::{AsyncFrameReader, AsyncFrameWriter, Frame};
use crate::cluster::node::message::{BuildRequestError, BuildResponseError, Request, RequestBody, RequestBuilder, Response, ResponseBody};
use crate::cluster::node::persist::Persist;
use crate::cluster::roles::{Role, RoleSet};
use crate::cluster::Cluster;
use log::{debug, info, trace, warn};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::io::ErrorKind;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc};
use std::time::Duration;
use parking_lot::{Mutex, RwLock};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::task::JoinHandle;
use tokio::time::error::Elapsed;
use tokio::time::timeout;
use tokio::{io, signal, task, time, try_join};
use uuid::Uuid;
use whirlpool_common::util::asserts::assert_that;

pub mod config;
pub mod frame;
pub mod message;
pub mod persist;

/// An actual node, which acts like an actor.
#[derive(Debug)]
pub struct Node {
    id: Uuid,
    roles: RoleSet,
    bind_address: IpAddr,
    comms_port: u16,
    client_port: u16,
    persist: Persist,
    timeout: Option<Duration>,
    comms_socket: Option<SocketAddr>,
}

impl Default for Node {
    fn default() -> Self {
        NodeConfig::default()
            .build_node()
            .expect("default node config should always create a good node")
    }
}

impl Node {
    /// Tells this node to start running. After the communications port is bound, the callback is ran if present.
    pub async fn run<'a>(
        mut self,
    ) -> Result<(SocketAddr, impl Future<Output=Result<(), NodeError>> + 'a), NodeError> {
        let cluster: Cluster = if let Some(cluster) = self.persist.cluster() {
            self.join_cluster(cluster.clone()).await?
        } else if self.roles.has_role(Role::ClusterManager) {
            self.start_cluster().await?
        } else {
            return Err(NodeError::NoCluster);
        };

        self.persist.set_cluster(cluster);

        let socket_addr = (self.bind_address, self.comms_port);
        let listener = TcpListener::bind(socket_addr).await?;
        let local_addr = listener.local_addr()?;
        self.comms_socket = Some(local_addr);
        Ok((local_addr, async move {
            self.main_loop(listener, local_addr).await?;
            Ok(())
        }))
    }

    pub async fn main_loop(mut self, listener: TcpListener, local_addr: SocketAddr) -> Result<(), NodeError> {
        info!("starting tcp listener at address = {:?}", local_addr);
        info!("{} is now waiting for connections to made.", self.id);
        while let (cnxn, addr) = listener.accept().await? {
            info!("{} made connection to node {:#?}", self.id, addr);
            self.handle_connect(cnxn).await?;
        }

        Ok(())
    }

    pub fn is_manager(&self) -> bool {
        self.persist.node_id()
            .zip(self.persist.cluster())
            .map(|(&node_id, cluster)| {
                node_id == cluster.manager()
            })
            .unwrap_or(false)
    }

    pub fn request_builder(&self) -> RequestBuilder {
        RequestBuilder::new()
            .node(self)
    }

    async fn handle_connect(&mut self, mut connection: TcpStream) -> Result<(), NodeError> {



        let frame: Frame<Request> = {
            let mut frame_reader = AsyncFrameReader::new(&mut connection).await;
            debug!("{} waiting for frame", self.id);
            frame_reader.read_frame().await?
        };
        debug!("received frame: {:#?}", frame);
        let request = frame.unwrap();

        if self.is_manager() || request.node() == self.persist.cluster().map(|c| c.manager()){
            // if this is the manager of the cluster, or the request is from the manager
            let response = self.get_response(request, &mut connection).await?;
            let resp_frame = Frame::new(response);


            let mut writer = AsyncFrameWriter::new(&mut connection).await;
            writer.write_frame(resp_frame).await?;
        } else {
            todo!("forwarding to cluster manager")
        }


        Ok(())
    }



    async fn get_response(&mut self, ref req: Request, cnxn: &mut TcpStream) -> Result<Response, NodeError> {
        Ok(match req.body() {
            RequestBody::Ping => Response::builder().body(ResponseBody::Pong).finish()?,
            RequestBody::GetInfo => {
                Response::builder()
                    .body(ResponseBody::NodeInfo {
                        node: self.id,
                        cluster: self.persist.cluster().cloned(),
                    }).finish()?
            }
            RequestBody::ConnectToCluster { socket_addr } => {
                info!("attempting to connect to cluster at address: {:?}", socket_addr);
                debug!("testing if node is active...");
                self.send_request(self.request_builder()
                                      .body(RequestBody::Ping).finish()?, socket_addr).await?;
                debug!("node is active");

                let ResponseBody::NodeInfo {
                    node, cluster
                } = self.send_request(self.request_builder()
                                          .body(RequestBody::GetInfo).finish()?, socket_addr).await?.body().clone() else {
                    panic!("could not get node info")
                };
                let Some(cluster) = cluster else {
                    panic!("node is not part of any cluster")
                };

                info!("cluster: {:#?}", cluster);

                self.persist.modify_cluster(|my_cluster| {
                    my_cluster.join_into(&cluster);
                });


                Response::ok()
            }
        })
    }

    async fn send_request(&mut self, request: Request, addr: &SocketAddr) -> io::Result<Response> {
        send_to_node(request, *addr, self.timeout).await
    }

    async fn join_cluster(&mut self, cluster: Cluster) -> Result<Cluster, NodeError> {
        if cluster.manager() == self.id
            && self.roles.has_role(Role::ClusterManager)
        {
            info!("re-joining single-node cluster as manager");
            return Ok(cluster);
        } else {
            todo!("joining existing clusters not yet implemented")
        }
    }

    async fn start_cluster(&mut self) -> Result<Cluster, NodeError> {
        let cluster = Cluster::new(self.id);
        self.persist.set_cluster(cluster.clone());
        Ok(cluster)
    }
    pub fn comms_socket(&self) -> Option<SocketAddr> {
        self.comms_socket
    }

    pub fn id(&self) -> Uuid {
        *self.persist.node_id().unwrap()
    }
}

pub const TIMEOUT: Duration = Duration::from_secs(10);

pub async fn send_to_node<A: ToSocketAddrs>(
    request: Request,
    addr: A,
    timeout: impl Into<Option<Duration>>,
) -> io::Result<Response> {
    let timeout = timeout.into().unwrap_or(TIMEOUT);
    match tokio::time::timeout(timeout, async {
        debug!("sending request {request:#?}");
        let mut socket = TcpStream::connect(&addr).await?;
        {
            let mut frame_writer = AsyncFrameWriter::new(&mut socket).await;
            let i = frame_writer.write_frame(Frame::new(request)).await?;
            trace!("wrote {} bytes to socket", i);
            frame_writer.flush().await?;
        }
        {
            let mut frame_reader = AsyncFrameReader::new(&mut socket).await;
            let frame: Frame<Response> = frame_reader.read_frame().await?;
            let response = frame.unwrap();
            info!("received response {:#?}", response);
            Ok(response)
        }
    })
        .await
    {
        Err(elapsed) => {
            warn!(
                "timeout occurred after {:?} when attempting to communicate with node",
                timeout
            );
            Err(io::Error::new(ErrorKind::ConnectionReset, elapsed))
        }
        Ok(inner) => inner,
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeSocket {}

/// Used to represent an error occurred within a node
#[derive(Debug, Error)]
pub enum NodeError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("no cluster to join and not a cluster manager")]
    NoCluster,
    #[error(transparent)]
    BuildRequestError(#[from] BuildRequestError),
    #[error(transparent)]
    BuildResponseError(#[from] BuildResponseError),
}
