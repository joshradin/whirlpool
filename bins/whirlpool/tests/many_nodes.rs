use log::{info, Level};
use std::net::{Ipv4Addr, ToSocketAddrs};
use std::time::Duration;
use tokio::task::JoinSet;
use whirlpool::cluster::node::config::NodeConfig;
use whirlpool::cluster::node::message::{Request, RequestBody};
use whirlpool::cluster::node::{send_to_node, Node};
use whirlpool::cluster::node::message::ResponseBody::Pong;

#[tokio::test]
async fn connect_3() {
    simple_logger::init_with_level(Level::Debug).unwrap();
    run_many_nodes(3).await
}

async fn run_many_nodes(node_count: usize) {

    let mut join_set = JoinSet::new();
    let base_port = 10000;
    let mut ports = vec![];

    let temp_dir = tempfile::tempdir().unwrap();


    for i in 0..node_count {
        let port = base_port + i as u16;
        ports.push(port);

        let config = NodeConfig::new()
            .persist_location(temp_dir.path().join(format!("node{}.info", i)))
            .comms_port(port)
            .timeout(Duration::from_millis(5000));
        let mut node = config
            .build_node()
            .expect("node could not be built");
        let node_id = node.id();

        let ports = ports.clone();
        join_set.spawn(async move {
            node.run().await
        });
        // info!("node started on port {:?}", node.comms_socket());
        if i > 0 {
            let first = ports[0];
            info!("sending command to node to connect to node at {}", first);
            let resp = send_to_node(
                Request::builder()
                    .body(RequestBody::ConnectToCluster {
                        socket_addr: (Ipv4Addr::LOCALHOST, first)
                            .to_socket_addrs()
                            .unwrap()
                            .next()
                            .unwrap(),
                    })
                    .finish()
                    .unwrap(),
                (Ipv4Addr::LOCALHOST, first),
                Duration::from_millis(500)
            )
                .await;
        }
    }
    while let Some(join) = join_set.join_next().await {
        join.unwrap().unwrap();
    }
}
