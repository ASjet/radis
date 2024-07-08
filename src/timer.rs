use std::time::Duration;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::Mutex;

pub struct OneshotTimer {
    on_fire: Sender<()>,
    timer: Mutex<async_timers::OneshotTimer>,

    reset_tx: mpsc::Sender<Duration>,
    reset_rx: Mutex<mpsc::Receiver<Duration>>,

    cancel_tx: mpsc::Sender<()>,
    cancel_rx: Mutex<mpsc::Receiver<()>>,
}

impl OneshotTimer {
    pub fn new(fire_tx: Sender<()>) -> Self {
        let (reset_tx, reset_rx) = mpsc::channel(1);
        let (cancel_tx, cancel_rx) = mpsc::channel(1);
        let timer = async_timers::OneshotTimer::expired();

        OneshotTimer {
            on_fire: fire_tx,
            timer: Mutex::new(timer),
            reset_tx,
            reset_rx: Mutex::new(reset_rx),
            cancel_tx: cancel_tx,
            cancel_rx: Mutex::new(cancel_rx),
        }
    }

    pub async fn start(&self) {
        loop {
            let mut timer = self.timer.lock().await;
            let mut reset_rx = self.reset_rx.lock().await;
            let mut cancel_rx = self.cancel_rx.lock().await;
            tokio::select! {
                _ = timer.tick() => {
                    self.on_fire.send(()).await.unwrap();
                }
                dura = reset_rx.recv() => {
                    if let Some(dura) = dura {
                        timer.schedule(dura);
                    }
                }
                _ = cancel_rx.recv() => {
                    timer.cancel();
                }
            }
        }
    }

    pub async fn reset(&self, dura: Duration) {
        let _ = self.reset_tx.send(dura).await.unwrap();
    }

    pub async fn cancel(&self) {
        let _ = self.cancel_tx.send(()).await.unwrap();
    }
}

pub struct PeriodicTimer {
    on_fire: Sender<()>,
    timer: Mutex<async_timers::PeriodicTimer>,

    reset_tx: mpsc::Sender<Duration>,
    reset_rx: Mutex<mpsc::Receiver<Duration>>,

    cancel_tx: mpsc::Sender<()>,
    cancel_rx: Mutex<mpsc::Receiver<()>>,
}

impl PeriodicTimer {
    pub fn new(fire_tx: Sender<()>) -> Self {
        let (reset_tx, reset_rx) = mpsc::channel(1);
        let (cancel_tx, cancel_rx) = mpsc::channel(1);
        let timer = async_timers::PeriodicTimer::stopped();

        PeriodicTimer {
            on_fire: fire_tx,
            timer: Mutex::new(timer),
            reset_tx,
            reset_rx: Mutex::new(reset_rx),
            cancel_tx: cancel_tx,
            cancel_rx: Mutex::new(cancel_rx),
        }
    }

    /// This will cause the timer to tick immediately
    pub async fn start(&self) {
        loop {
            let mut timer = self.timer.lock().await;
            let mut reset_rx = self.reset_rx.lock().await;
            let mut cancel_rx = self.cancel_rx.lock().await;
            tokio::select! {
                _ = timer.tick() => {
                    self.on_fire.send(()).await.unwrap();
                }
                interval = reset_rx.recv() => {
                    if let Some(interval) = interval {
                        timer.start(interval);
                    }
                }
                _ = cancel_rx.recv() => {
                    timer.stop();
                }
            }
        }
    }

    pub async fn reset(&self, dura: Duration) {
        let _ = self.reset_tx.send(dura).await.unwrap();
    }

    pub async fn stop(&self) {
        let _ = self.cancel_tx.send(()).await.unwrap();
    }
}
