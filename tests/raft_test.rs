use radis::conf::Config;
use radis::raft::state;
use radis::raft::RaftService;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn leader_election() {
    let mut ctl = Controller::new(3, 50000);
    ctl.serve_all().await;

    // Wait for establishing agreement on one leader
    sleep(Duration::from_millis(1000)).await;

    let (followers, candidates, leader_cnt) = ctl.count_roles().await;
    // There should be only one leader, and all others are followers
    assert_eq!(followers, 2);
    assert_eq!(candidates, 0);
    assert_eq!(leader_cnt, 1);

    ctl.close_all().await;
    // Wait for all connections to finish
    sleep(Duration::from_millis(500)).await;
}

#[tokio::test]
async fn fail_over() {
    let mut ctl = Controller::new(3, 50003);
    ctl.serve_all().await;

    // Wait for establishing agreement on one leader
    sleep(Duration::from_millis(1000)).await;

    let (followers, candidates, leader_cnt) = ctl.count_roles().await;
    // There should be only one leader, and all others are followers
    assert_eq!(followers, 2);
    assert_eq!(candidates, 0);
    assert_eq!(leader_cnt, 1);

    // Leader offline
    let old_leader = ctl.leader().await.unwrap();
    let old_term = ctl.term(old_leader).await;
    ctl.close(old_leader).await;

    // Wait for one follower timeout and start new election
    sleep(Duration::from_millis(1000)).await;

    let (followers, candidates, leader_cnt) = ctl.count_roles().await;
    // There should be a new candidate that got a vote
    assert_eq!(followers, 1);
    assert_eq!(candidates, 1);
    assert_eq!(leader_cnt, 1); // Old leader

    // Old leader back online
    ctl.serve(old_leader).await;
    ctl.setup_timer(old_leader).await;

    // Wait for re-establishing agreement on new leader
    sleep(Duration::from_millis(1000)).await;

    let new_leader = ctl.leader().await.unwrap();
    let new_term = ctl.term(new_leader).await;
    // The old leader should be replaced by the new leader with a higher term
    assert_ne!(old_leader, new_leader);
    assert!(new_term > old_term);

    ctl.close_all().await;
    // Wait for all connections to finish
    sleep(Duration::from_millis(500)).await;
}

////////////////////////////////////////////////////////////////////////////////

struct Controller {
    services: Vec<RaftService>,
}

impl Controller {
    fn new(peers: i32, port_base: u16) -> Self {
        Controller {
            services: Config::builder()
                .peers(peers)
                .base_port(port_base)
                .build()
                .iter()
                .map(|cfg| RaftService::new(cfg.clone()))
                .collect(),
        }
    }

    async fn serve(&mut self, idx: usize) {
        let srv = self.services[idx].clone();
        tokio::spawn(async move {
            srv.serve().await.unwrap();
        });
    }

    async fn serve_all(&mut self) {
        for i in 0..self.services.len() {
            self.serve(i).await;
        }
    }

    async fn close(&self, idx: usize) {
        self.services[idx].close().await;
    }

    async fn close_all(&self) {
        for i in 0..self.services.len() {
            self.close(i).await;
        }
    }

    async fn setup_timer(&self, idx: usize) {
        let srv = self.services[idx].clone();
        let state = srv.state();
        let ctx = srv.context();
        let state = state.lock().await;
        state.on_timeout(ctx.clone()).await;
        state.on_tick(ctx.clone()).await;
    }

    async fn term(&self, idx: usize) -> u64 {
        self.services[idx].state().lock().await.term()
    }

    async fn role(&self, idx: usize) -> state::Role {
        self.services[idx].state().lock().await.role()
    }

    async fn count_roles(&self) -> (i32, i32, i32) {
        let mut followers = 0;
        let mut candidates = 0;
        let mut leader_cnt = 0;
        for i in 0..self.services.len() {
            match self.role(i).await {
                state::Role::Follower => followers += 1,
                state::Role::Candidate => candidates += 1,
                state::Role::Leader => leader_cnt += 1,
            }
        }
        (followers, candidates, leader_cnt)
    }

    async fn leader(&self) -> Option<usize> {
        for i in 0..self.services.len() {
            if self.role(i).await == state::Role::Leader {
                return Some(i);
            }
        }
        None
    }
}
