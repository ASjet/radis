use radis::conf::Config;
use radis::raft::state;
use radis::raft::RaftService;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;

struct ServiceHandler {
    service: RaftService,
    close_ch: mpsc::Sender<()>,
}

impl ServiceHandler {
    #[allow(dead_code)]
    async fn serve(&self) {
        self.service.serve().await.unwrap();
    }

    async fn close(&self) {
        self.close_ch.send(()).await.unwrap();
    }

    async fn role(&self) -> state::Role {
        self.service.state().lock().await.role()
    }
}

async fn start(peers: i32) -> Vec<ServiceHandler> {
    let services: Vec<RaftService> = Config::builder()
        .peers(peers)
        .build()
        .iter()
        .map(|cfg| RaftService::new(cfg.clone()))
        .collect();

    let mut handlers = Vec::new();
    for service in services {
        let (close_tx, mut close_rx) = mpsc::channel(1);
        let srv = service.clone();
        tokio::spawn(async move {
            srv.serve_with_shutdown(async move {
                close_rx.recv().await;
            })
            .await
            .unwrap();
        });
        handlers.push(ServiceHandler {
            service,
            close_ch: close_tx,
        })
    }

    handlers
}

async fn count_roles(services: &Vec<ServiceHandler>) -> (i32, i32, i32) {
    let mut leader_cnt = 0;
    let mut followers = 0;
    let mut candidates = 0;
    for service in services {
        match service.role().await {
            state::Role::Leader => leader_cnt += 1,
            state::Role::Follower => followers += 1,
            state::Role::Candidate => candidates += 1,
        }
    }
    (leader_cnt, followers, candidates)
}

////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn leader_election() {
    let handlers = start(3).await;

    // Wait for establishing agreement on one leader
    sleep(Duration::from_secs(1)).await;

    let (leader_cnt, followers, candidates) = count_roles(&handlers).await;

    assert_eq!(leader_cnt, 1);
    assert_eq!(followers, 2);
    assert_eq!(candidates, 0);

    for handler in handlers {
        handler.close().await;
    }

    // Wait for all connections to finish
    sleep(Duration::from_secs(1)).await;
}
