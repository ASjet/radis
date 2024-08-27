use core::panic;
use raft::config::Config;
use raft::state;
use raft::RaftService;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;

#[tokio::test]
async fn leader_election() {
    let mut ctl = Controller::new(3, 50000);
    ctl.serve_all().await;

    // Wait for establishing agreement on one leader
    ctl.leader().await;

    let (followers, candidates, leader_cnt) = ctl.count_roles().await;
    // There should be only one leader, and all others are followers
    assert_eq!(followers, 2);
    assert_eq!(candidates, 0);
    assert_eq!(leader_cnt, 1);

    ctl.close_all().await;
}

#[tokio::test]
async fn fail_over() {
    let mut ctl = Controller::new(3, 50003);
    ctl.serve_all().await;

    // Wait for establishing agreement on one leader
    let old_leader = ctl.leader().await;

    // Leader offline
    let old_term = ctl.term(old_leader).await;
    ctl.close(old_leader).await;

    // Wait for one follower timeout and start new election
    sleep(Duration::from_millis(1000)).await;

    // There should be a new leader got elected
    ctl.leader().await;

    let (followers, candidates, leader_cnt) = ctl.count_roles().await;
    assert_eq!(followers, 1);
    assert_eq!(candidates, 0);
    assert_eq!(leader_cnt, 2); // New leader plus old leader

    // Old leader back online
    ctl.serve(old_leader).await;
    ctl.setup_timer(old_leader).await;

    // Wait for re-establishing agreement on new leader
    sleep(Duration::from_millis(500)).await;
    let new_leader = ctl.leader().await;
    let new_term = ctl.term(new_leader).await;
    // The old leader should be replaced by the new leader with a higher term
    assert_ne!(old_leader, new_leader);
    assert!(new_term > old_term);

    ctl.close_all().await;
}

#[tokio::test]
async fn basic_commit() {
    let peers = 3;
    let mut ctl = Controller::new(peers, 50006);
    ctl.serve_all().await;

    // Wait for establishing agreement on one leader
    let leader = ctl.leader().await;

    // Append command to leader
    let data = b"hello, raft!".to_vec();
    ctl.agree_one(leader, data).await;

    ctl.close_all().await;
}

#[tokio::test]
async fn command_forward() {
    let peers = 3;
    let mut ctl = Controller::new(peers, 50009);
    ctl.serve_all().await;

    // Wait for establishing agreement on one leader
    ctl.leader().await;

    // Append command to follower
    let data = b"hello, raft!".to_vec();
    let follower = ctl.follower().await.unwrap();
    ctl.agree_one(follower, data).await;

    ctl.close_all().await;
}

////////////////////////////////////////////////////////////////////////////////

struct ControllerConfig {
    election_wait: Duration,
    election_retry: i32,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        ControllerConfig {
            election_wait: Duration::from_millis(200),
            election_retry: 5,
        }
    }
}

struct Controller {
    cfg: ControllerConfig,
    services: Vec<RaftService>,
    commit_rxs: Vec<mpsc::Receiver<Arc<Vec<u8>>>>,
}

impl Controller {
    fn new(peers: i32, port_base: u16) -> Self {
        let mut services = Vec::with_capacity(peers as usize);
        let mut commit_rxs = Vec::with_capacity(peers as usize);
        for cfg in Config::builder()
            .peers(peers)
            .base_port(port_base)
            .build()
            .into_iter()
        {
            let (commit_tx, commit_rx) = mpsc::channel(1);
            services.push(RaftService::new(cfg, commit_tx));
            commit_rxs.push(commit_rx);
        }
        Controller {
            cfg: ControllerConfig::default(),
            services,
            commit_rxs,
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
        // Wait for all connections to finish
        sleep(Duration::from_millis(500)).await;
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

    async fn leader(&self) -> usize {
        for _ in 0..self.cfg.election_retry {
            for i in 0..self.services.len() {
                if self.role(i).await == state::Role::Leader {
                    return i;
                }
            }
            sleep(self.cfg.election_wait).await;
        }
        panic!("No leader elected");
    }

    async fn follower(&self) -> Option<usize> {
        for i in 0..self.services.len() {
            if self.role(i).await == state::Role::Follower {
                return Some(i);
            }
        }
        None
    }

    async fn append_command(&self, idx: usize, cmd: Vec<u8>) {
        self.services[idx].append_command(cmd).await.unwrap();
    }

    async fn read_commit(&mut self, idx: usize) -> Option<Arc<Vec<u8>>> {
        self.commit_rxs[idx].recv().await
    }

    async fn agree_one(&mut self, srv: usize, cmd: Vec<u8>) {
        // Send command to specified service
        self.append_command(srv, cmd.clone()).await;

        // Expect the command to be committed on leader
        let leader = self.leader().await;
        let recv_data = self.read_commit(leader).await.unwrap();
        assert!(recv_data.as_slice() == cmd.as_slice());

        // Expect the command to be committed on all followers
        for idx in (0..self.services.len()).filter(|idx| *idx != leader) {
            let recv_data = self.read_commit(idx as usize).await.unwrap();
            assert!(recv_data.as_slice() == cmd.as_slice());
        }
    }
}
