use std::sync::Arc;
use std::time::Duration;
use timer::{OneshotTimer, PeriodicTimer};
use tokio::sync::mpsc;
use tokio::time::sleep;

#[tokio::test]
async fn oneshot_start() {
    let (fire_tx, mut fire_rx) = mpsc::channel(1);

    let timer = Arc::new(OneshotTimer::new(fire_tx));

    let t = timer.clone();
    tokio::spawn(async move {
        t.start().await;
    });

    assert!(fire_rx.try_recv().is_err());
    timer.reset(Duration::from_millis(10)).await;
    sleep(Duration::from_millis(11)).await;
    assert_eq!(Some(()), fire_rx.try_recv().ok());
}

#[tokio::test]
async fn oneshot_reset() {
    let (fire_tx, mut fire_rx) = mpsc::channel(1);

    let timer = Arc::new(OneshotTimer::new(fire_tx));

    let t = timer.clone();
    tokio::spawn(async move {
        t.start().await;
    });

    timer.reset(Duration::from_millis(20)).await;
    sleep(Duration::from_millis(10)).await;
    assert!(fire_rx.try_recv().is_err());
    timer.reset(Duration::from_millis(20)).await;
    assert!(fire_rx.try_recv().is_err());
    timer.reset(Duration::from_millis(20)).await;
    assert!(fire_rx.try_recv().is_err());
    timer.reset(Duration::from_millis(20)).await;
    assert!(fire_rx.try_recv().is_err());
    timer.reset(Duration::from_millis(20)).await;
    assert!(fire_rx.try_recv().is_err());
    timer.reset(Duration::from_millis(20)).await;
    assert!(fire_rx.try_recv().is_err());
    sleep(Duration::from_millis(21)).await;
    assert_eq!(Some(()), fire_rx.try_recv().ok());
}

#[tokio::test]
async fn oneshot_cancel() {
    let (fire_tx, mut fire_rx) = mpsc::channel(1);

    let timer = Arc::new(OneshotTimer::new(fire_tx));

    let t = timer.clone();
    tokio::spawn(async move {
        t.start().await;
    });

    timer.reset(Duration::from_millis(20)).await;
    sleep(Duration::from_millis(10)).await;
    timer.cancel().await;
    assert!(fire_rx.try_recv().is_err());
    sleep(Duration::from_millis(20)).await;
    assert!(fire_rx.try_recv().is_err());
}

#[tokio::test]
async fn periodict_start() {
    let (fire_tx, mut fire_rx) = mpsc::channel(1);

    let timer = Arc::new(PeriodicTimer::new(fire_tx));

    let t = timer.clone();
    tokio::spawn(async move {
        t.start().await;
    });

    assert!(fire_rx.try_recv().is_err());

    timer.reset(Duration::from_millis(10)).await;
    sleep(Duration::from_millis(2)).await;
    assert_eq!(Some(()), fire_rx.try_recv().ok());

    sleep(Duration::from_millis(12)).await;
    assert_eq!(Some(()), fire_rx.try_recv().ok());

    sleep(Duration::from_millis(12)).await;
    assert_eq!(Some(()), fire_rx.try_recv().ok());
}

#[tokio::test]
async fn periodict_reset() {
    let (fire_tx, mut fire_rx) = mpsc::channel(1);

    let timer = Arc::new(PeriodicTimer::new(fire_tx));

    let t = timer.clone();
    tokio::spawn(async move {
        t.start().await;
    });

    timer.reset(Duration::from_millis(20)).await;
    sleep(Duration::from_millis(2)).await;
    assert_eq!(Some(()), fire_rx.try_recv().ok());

    sleep(Duration::from_millis(10)).await;
    assert!(fire_rx.try_recv().is_err());
    sleep(Duration::from_millis(10)).await;
    assert_eq!(Some(()), fire_rx.try_recv().ok());

    sleep(Duration::from_millis(10)).await;
    assert!(fire_rx.try_recv().is_err());
    sleep(Duration::from_millis(10)).await;
    assert_eq!(Some(()), fire_rx.try_recv().ok());

    timer.reset(Duration::from_millis(5)).await;
    sleep(Duration::from_millis(2)).await;
    assert_eq!(Some(()), fire_rx.try_recv().ok());

    sleep(Duration::from_millis(5)).await;
    assert_eq!(Some(()), fire_rx.try_recv().ok());

    sleep(Duration::from_millis(5)).await;
    assert_eq!(Some(()), fire_rx.try_recv().ok());
}

#[tokio::test]
async fn periodict_stop() {
    let (fire_tx, mut fire_rx) = mpsc::channel(1);

    let timer = Arc::new(PeriodicTimer::new(fire_tx));

    let t = timer.clone();
    tokio::spawn(async move {
        t.start().await;
    });

    timer.reset(Duration::from_millis(10)).await;
    sleep(Duration::from_millis(2)).await;
    assert_eq!(Some(()), fire_rx.try_recv().ok());

    sleep(Duration::from_millis(10)).await;
    assert_eq!(Some(()), fire_rx.try_recv().ok());

    sleep(Duration::from_millis(10)).await;
    assert_eq!(Some(()), fire_rx.try_recv().ok());

    timer.stop().await;
    sleep(Duration::from_millis(10)).await;
    assert!(fire_rx.try_recv().is_err());

    sleep(Duration::from_millis(10)).await;
    assert!(fire_rx.try_recv().is_err());
}
