use clap::Parser;
use radis::conf::Config;
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

    /// Create sub directory for every node
    #[arg(long, default_value_t = false)]
    sub_dir: bool,

    /// Listen host
    #[arg(long, default_value_t = String::from("0.0.0.0"))]
    host: String,

    /// Peer host
    #[arg(long, default_value_t = String::from("http://localhost"))]
    peer_host: String,

    /// Listen port start
    #[arg(long, default_value_t = 50000)]
    port: u16,
}

fn main() {
    let args = Args::parse();

    for id in 0..args.peers {
        let name = make_node_name(&args.prefix, id);
        let (path, filename) = if args.sub_dir {
            (format!("{}/{}", &args.dir, &name), "conf.toml".into())
        } else {
            (args.dir.clone(), format!("{}.toml", &name))
        };

        let cfg = Config {
            id: name,
            listen_addr: join_host_port(&args.host, args.port + id as u16),
            peer_addrs: make_peer_addrs(&args.peer_host, args.port, args.peers, id),
        };
        let cfg = toml::to_string_pretty(&cfg).unwrap();

        fs::create_dir_all(&path).unwrap();
        fs::write(format!("{}/{}", &path, &filename), cfg.as_bytes()).unwrap();
    }
}

fn join_host_port(host: &str, port: u16) -> String {
    format!("{}:{}", host, port)
}

fn make_node_name(prefix: &str, id: i32) -> String {
    format!("{}{}", prefix, id)
}

fn make_peer_addrs(host: &str, base_port: u16, n_peer: i32, id: i32) -> Vec<String> {
    (0..n_peer)
        .filter(|peer| *peer != id)
        .map(|peer| join_host_port(host, base_port + peer as u16))
        .collect()
}
