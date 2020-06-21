use std::net::SocketAddr;

use crate::config::Config;

pub struct Server {
    address: SocketAddr,
    key: u128,
}

impl Server {
    pub fn new(config: Config) -> Server {
        todo!();
    }

    pub fn start(self) {
        todo!();
    }
}
