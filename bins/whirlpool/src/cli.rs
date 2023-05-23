//! Defines the command line interface for working with whirlpool

use clap::Parser;
use std::path::PathBuf;

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(short = 'f')]
    #[clap(long = "file")]
    #[clap(env = "WHIRLPOOL_CONFIG")]
    pub config_file: Option<PathBuf>,
    /// Overrides the comms port setting in the config
    #[clap(short = 'p', long = "port")]
    #[clap(env = "WHIRLPOOL_COMMS_PORT")]
    pub comms_port: Option<u16>,
    /// Sets a host for the node to try to connect to
    #[clap(long = "connect")]
    #[clap(env = "WHIRLPOOL_CONNECT")]
    pub connect: Option<String>,
    /// The directory to execute from
    #[clap(long = "dir")]
    pub dir: Option<PathBuf>
}

impl Args {
    pub fn new() -> Self {
        Parser::parse()
    }
}
