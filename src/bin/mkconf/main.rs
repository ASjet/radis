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

    Config::builder()
        .listen_host(&args.host)
        .peer_host(&args.peer_host)
        .base_port(args.port)
        .name_prefix(&args.prefix)
        .peers(args.peers)
        .build()
        .iter()
        .for_each(|cfg| {
            let (path, filename) = if args.sub_dir {
                (format!("{}/{}", &args.dir, &cfg.id), "conf.toml".into())
            } else {
                (args.dir.clone(), format!("{}.toml", &cfg.id))
            };
            let cfg = toml::to_string_pretty(&cfg).unwrap();
            fs::create_dir_all(&path).unwrap();
            fs::write(format!("{}/{}", &path, &filename), cfg.as_bytes()).unwrap();
        });
}
