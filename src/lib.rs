pub mod conf;
pub mod db;

#[cfg(feature = "async_log")]
pub fn init_logger(level: &str) {
    use structured_logger::{async_json::new_writer, Builder};
    use tokio::io;

    Builder::with_level(level)
        .with_target_writer("*", new_writer(io::stdout()))
        .init()
}

#[cfg(not(feature = "async_log"))]
pub fn init_logger(level: &str) {
    use std::io;
    use structured_logger::{json::new_writer, Builder};

    Builder::with_level(level)
        .with_target_writer("*", new_writer(io::stdout()))
        .init()
}
