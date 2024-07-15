use clap::Parser;
use radis::conf::Config;
use radis::raft::RaftService;
use tokio;
use tokio::sync::mpsc;

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

    let (commit_tx, _) = mpsc::channel(1);
    RaftService::new(cfg, commit_tx).serve().await?;

    Ok(())
}
