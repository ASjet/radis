#![allow(dead_code)]
use rand::{self, Rng};
use std::{ops::Add, time::Duration};

pub const REQUEST_TIMEOUT: u64 = 1000;
pub const HEARTBEAT_INTERVAL: i64 = 100;
pub const REQUEST_VOTE_INTERVAL: i64 = 150;
pub const ELECTION_TIMEOUT: i64 = 150;
pub const ELECTION_TIMEOUT_DELTA: i64 = 75;
pub const HEARTBEAT_TIMEOUT: i64 = 150;
pub const HEARTBEAT_TIMEOUT_DELTA: i64 = 75;

pub fn random_backoff(base: i64, delta: i64) -> Duration {
    let mut rng = rand::thread_rng();
    Duration::from_millis(base.add(rng.gen_range(-delta / 2..delta / 2)) as u64)
}

pub fn follower_timeout() -> Duration {
    random_backoff(ELECTION_TIMEOUT, ELECTION_TIMEOUT_DELTA)
}

pub fn candidate_timeout() -> Duration {
    random_backoff(ELECTION_TIMEOUT, ELECTION_TIMEOUT_DELTA)
}
