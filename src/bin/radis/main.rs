use clap::Parser;
use radis::conf::Config;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Config file path
    #[arg(short, long)]
    conf: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let cfg = Config::from_path(&args.conf)?;
    dbg!(cfg);

    Ok(())
}
