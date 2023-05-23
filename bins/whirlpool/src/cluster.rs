//! You should be able to connect many whirlpool instances together

mod cluster_impl;
pub use cluster_impl::*;
pub mod node;
pub mod roles;
