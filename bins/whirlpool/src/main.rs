use std::env::set_current_dir;
use log::{debug, info, Level, warn};
use std::error::Error;
use std::fs;
use std::fs::File;
use std::net::{Ipv4Addr, ToSocketAddrs};
use std::time::Duration;
use tokio::signal;

use whirlpool::cli::Args;
use whirlpool::cluster::node::config::NodeConfig;
use whirlpool::cluster::node::{Node, send_to_node};
use whirlpool::cluster::node::message::{Request, RequestBody};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::init_with_level(Level::Debug).expect("could not init logger");
    let args = Args::new();
    debug!("args: {:#?}", args);
    if let Some(dir) = &args.dir {
        fs::create_dir_all(dir)?;
        set_current_dir(dir)?;
    }


    let mut node_config: NodeConfig = if let Some(config_file) = &args.config_file {
        let ext = config_file.extension().and_then(|os| os.to_str());
        match ext {
            #[cfg(feature = "yaml")]
            Some("yaml") => serde_yaml::from_reader(File::open(config_file)?)?,
            #[cfg(feature = "toml")]
            Some("toml") => {
                let toml_str = fs::read_to_string(config_file)?;
                toml::from_str(&toml_str)?
            }
            Some("json") | None => serde_json::from_reader(File::open(config_file)?)?,
            Some(other) => {
                panic!("can not process extension for config file: {other:?}")
            }
        }
    } else {
        NodeConfig::default()
    };


    debug!("node config: {:#?}", node_config);

    if let Some(comms_port_override) = args.comms_port {
        node_config = node_config.comms_port(comms_port_override);
    }


    let mut node = node_config.build_node()?;

    info!("running node {:#?}...", node);
    let (socket, run) = node.run().await?;
    info!("node running on socket {}", socket);


    let output = tokio::spawn(async move { run.await });

    if let Some(connect_to) = args.connect {
        info!("attempting to connect to cluster at {:?}", connect_to);
        send_to_node(
            Request::builder()
                .body(RequestBody::ConnectToCluster {
                    socket_addr: connect_to
                        .to_socket_addrs()
                        .unwrap()
                        .next()
                        .unwrap(),
                })
                .finish()
                .unwrap(),
            socket,
            Duration::from_millis(500)
        ).await?;
    }

    let abort_handle = output.abort_handle();

    info!("will terminate when receiving ctrl-c event");
    signal::ctrl_c().await?;
    warn!("ctrl-c event received, terminating node...");
    abort_handle.abort();
    return match output.await {
        Ok(result) => { Ok(result?)}
        Err(e) => {
            if e.is_cancelled() {
                Ok(())
            } else {
                panic!("{}", e);
            }
        }
    }
}