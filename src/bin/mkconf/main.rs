use clap::Parser;
use radis::conf::Config;
use raft::config::Config as RaftConfig;
use std::fs;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Root directory for generated configs
    #[arg(short, long, default_value_t = String::from("."))]
    dir: String,

    /// Peers amount
    #[arg(short, long, default_value_t = 3)]
    peers: i32,

    /// Node ID prefix
    #[arg(long, default_value_t = String::from("node"))]
    prefix: String,

    /// Listen host
    #[arg(long, default_value_t = String::from("0.0.0.0"))]
    host: String,

    /// Peer host
    #[arg(long, default_value_t = String::from("http://localhost"))]
    peer_host: String,

    /// Raft rpc listening start port
    #[arg(long, default_value_t = 50000)]
    raft_port: u16,

    /// Redis API listening start port
    #[arg(long, default_value_t = 63790)]
    redis_port: u16,

    /// Enable persistent storage
    #[arg(long, default_value_t = true)]
    persist: bool,
}

fn main() {
    let args = Args::parse();

    RaftConfig::builder()
        .listen_host(&args.host)
        .peer_host(&args.peer_host)
        .base_port(args.raft_port)
        .name_prefix(&args.prefix)
        .peers(args.peers)
        .build()
        .into_iter()
        .enumerate()
        .map(|(i, rc)| Config {
            listen_addr: format!("{}:{}", &args.host, args.redis_port + i as u16),
            raft_data: if args.persist {
                Some(format!("{}/{}/data", &args.dir, &rc.id))
            } else {
                None
            },
            raft: rc,
        })
        .for_each(|cfg| {
            let path = format!("{}/{}", &args.dir, &cfg.raft.id);
            let filename: String = "conf.toml".into();
            let cfg = toml::to_string_pretty(&cfg).unwrap();
            fs::create_dir_all(&path).unwrap();
            fs::write(format!("{}/{}", &path, &filename), cfg.as_bytes()).unwrap();
        });
}
