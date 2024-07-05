use clap::Parser;
use radis::conf::Config;
use radis::raft::{RaftServer, RaftService, RequestVoteArgs};
use tokio;
use tonic::transport::Server;

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
    let addr = cfg.listen_addr.parse()?;
    println!("radis node <{}> listening on {}", cfg.id, addr);

    let srv = RaftService::new(cfg);
    let context = srv.context();
    let (peers, me) = {
        let ctx = context.read().await;
        (ctx.peers(), ctx.me().to_string())
    };
    for fd in 0..peers {
        let candidate_id = me.clone();
        let ctx = context.clone();
        tokio::spawn(async move {
            let peer = ctx.read().await.get_peer(fd).clone();
            let resp = peer
                .lock()
                .await
                .request_vote(RequestVoteArgs {
                    term: 0,
                    candidate_id: candidate_id,
                    last_log_index: 0,
                    last_log_term: 0,
                })
                .await;
            println!("response: {:?}", resp);
        });
    }

    Server::builder()
        .add_service(RaftServer::new(srv))
        .serve(addr)
        .await?;

    Ok(())
}
