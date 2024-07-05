#![allow(dead_code)]
use rand::{self, Rng};
use std::{ops::Add, time::Duration};

pub const REQUEST_TIMEOUT: u64 = 1000;
pub const HEARTBEAT_INTERVAL: i64 = 150;
pub const REQUEST_VOTE_INTERVAL: i64 = 150;
pub const ELECTION_TIMEOUT: i64 = 150;
pub const ELECTION_TIMEOUT_DELTA: i64 = 75;
pub const HEARTBEAT_TIMEOUT: i64 = 150;
pub const HEARTBEAT_TIMEOUT_DELTA: i64 = 75;

pub fn with_random_backoff(base: i64, delta: i64) -> Box<dyn Fn() -> Duration + Send + Sync> {
    Box::new(move || {
        let mut rng = rand::thread_rng();
        Duration::from_millis(base.add(rng.gen_range(-delta..delta)) as u64)
    })
}
