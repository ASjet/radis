use persister::FilePersister;
use persister::Persister;
use std::fs;

#[tokio::test]
async fn file_persister() {
    let path = "/tmp/radis/test/persister/fp";
    let fp = FilePersister::new(path);

    test_persister(fp).await;

    fs::remove_dir_all(path).expect("remove file error");
}

////////////////////////////////////////////////////////////////////////////////

async fn test_persister(mut p: impl Persister) {
    // Read none at the beginning
    assert_eq!(
        None,
        p.read_wal().await.unwrap(),
        "should return None at the beginning"
    );

    let write_wal = vec![
        (1, "wal_test".as_bytes()),
        (2, "wal_test2".as_bytes()),
        (3, "wal_test3".as_bytes()),
    ];
    for (term, data) in write_wal.iter() {
        // Write wal without error
        p.write_wal(*term, *data).await.expect("write wal error");
    }

    for (term, data) in write_wal.iter() {
        // Read wal without error and data is consistent
        let (t, d) = p.read_wal().await.unwrap().expect("read wal error");
        assert_eq!(t, *term, "term should be consistent");
        assert_eq!(d.as_slice(), *data, "data should be consistent");
    }

    // Read none at EOF
    assert_eq!(None, p.read_wal().await.unwrap(), "return None at EOF");

    // When meet EOF, next `read_wal` should start from the beginning
    for (term, data) in write_wal.iter() {
        let (t, d) = p.read_wal().await.unwrap().unwrap();
        assert_eq!(t, *term, "term should be consistent");
        assert_eq!(d.as_slice(), *data, "data should be consistent");
    }

    assert_eq!(None, p.read_wal().await.unwrap(), "return None at EOF");
}
