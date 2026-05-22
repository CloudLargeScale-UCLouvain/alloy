extern crate routing;

use log::info;
use std::collections::HashMap;
use getrandom::getrandom;
mod root_filter;

use proxy_wasm::{traits::RootContext, types::LogLevel};
use root_filter::*;

mod tcp_filter;

mod data;

use routing::RoutingConfiguration;

fn get_random_string() -> String {
    // We'll store 8 random bytes in this buffer.
    let mut buf = [0u8; 8];
    getrandom(&mut buf).expect("Failed to get random bytes");

    // Define the character set we want to sample from:
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                             abcdefghijklmnopqrstuvwxyz\
                             0123456789";

    // Map each random byte to an index in CHARSET via modulo, then collect into a String.
    buf
        .iter()
        .map(|&b| CHARSET[(b as usize) % CHARSET.len()] as char)
        .collect()

}

proxy_wasm::main! {{
    proxy_wasm::set_log_level(LogLevel::Trace);
    proxy_wasm::set_root_context(|context_id| -> Box<dyn RootContext> {
        let identifier = get_random_string();
        info!("_start AlloyRootFilter {} for context {}", identifier, context_id);
        Box::new(AlloyRootFilter {
            context_id: context_id,
            config_bytes: Vec::new(),
            configuration: RoutingConfiguration::new(),
            requests_queue_id: 0,
            responses_queue_id: 0,
            map_requests: HashMap::new(),
            map_responses: HashMap::new(),
            identifier: identifier,
            fetch_max_wait_ms_virtual_partition: 1000,
            fetch_min_bytes_virtual_partition: 1000000,
            map_queue: HashMap::new(),
        })
    });
    
}}
