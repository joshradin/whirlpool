//! Messages are sent between nodes

use crate::cluster::node::{Node, NodeError};
use crate::cluster::Cluster;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use thiserror::Error;
use uuid::Uuid;
use whirlpool_common::util::asserts::assert_that;

/// A request message is sent from one node to another
#[derive(Debug, Deserialize, Serialize)]
pub struct Request {
    node: Option<Uuid>,
    headers: HashMap<String, String>,
    body: RequestBody,
}

impl Request {
    pub fn node(&self) -> Option<Uuid> {
        self.node
    }
    pub fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }
    pub fn body(&self) -> &RequestBody {
        &self.body
    }

    pub fn builder() -> RequestBuilder {
        RequestBuilder::new()
    }
}

/// The body of the request
#[derive(Debug, Deserialize, Serialize)]
pub enum RequestBody {
    Ping,
    GetInfo,
    /// Connect to a cluster at a socket address
    ConnectToCluster {
        socket_addr: SocketAddr,
    },
}

/// Used to build requests
#[derive(Debug, Default)]
pub struct RequestBuilder {
    node: Option<Uuid>,
    headers: HashMap<String, String>,
    body: Option<RequestBody>,
}

impl RequestBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn header(mut self, key: impl AsRef<str>, value: impl ToString) -> Self {
        self.headers
            .insert(key.as_ref().to_string(), value.to_string());
        self
    }

    pub fn node(mut self, node: &Node) -> Self {
        self.node = Some(node.id);
        self
    }

    pub fn body(mut self, body: RequestBody) -> Self {
        self.body = Some(body);
        self
    }

    pub fn finish(self) -> Result<Request, BuildRequestError> {
        assert_that(|| self.body.is_some(), BuildRequestError::NoBodySet)?;

        Ok(Request {
            node: self.node,
            headers: self.headers,
            body: self.body.unwrap(),
        })
    }
}

#[derive(Debug, Error)]
pub enum BuildRequestError {
    #[error("no error was set")]
    NoNodeSet,
    #[error("no body was set")]
    NoBodySet,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Response {
    headers: HashMap<String, String>,
    body: ResponseBody,
}

impl Response {


    pub fn builder() -> ResponseBuilder {
        ResponseBuilder::new()
    }

    pub fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }
    pub fn body(&self) -> &ResponseBody {
        &self.body
    }

    pub fn ok() -> Self {
        Self::builder()
            .body(ResponseBody::Ok)
            .finish()
            .unwrap()
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum ResponseBody {
    Err(String),
    Ok,
    Pong,
    NodeInfo { node: Uuid, cluster: Option<Cluster> },
}

/// Used to build responses
#[derive(Debug, Default)]
pub struct ResponseBuilder {
    headers: HashMap<String, String>,
    body: Option<ResponseBody>,
}

impl ResponseBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn header(mut self, key: impl AsRef<str>, value: impl ToString) -> Self {
        self.headers
            .insert(key.as_ref().to_string(), value.to_string());
        self
    }

    pub fn body(mut self, body: ResponseBody) -> Self {
        self.body = Some(body);
        self
    }

    pub fn finish(self) -> Result<Response, BuildResponseError> {
        assert_that(|| self.body.is_some(), BuildResponseError::NoBodySet)?;

        Ok(Response {
            headers: self.headers,
            body: self.body.unwrap(),
        })
    }
}

#[derive(Debug, Error)]
pub enum BuildResponseError {
    #[error("no body was set")]
    NoBodySet,
}
