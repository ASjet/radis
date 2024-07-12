use clap::Parser;
use radis::conf::Config;
use radis::raft::RaftService;
use tokio;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Config file path
    #[arg(short, long)]
    conf: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    radis::init_logger("info");

    let args = Args::parse();
    let cfg = Config::from_path(&args.conf)?;

    RaftService::new(cfg).serve().await?;

    Ok(())
}
