use persister::FilePersister;
use persister::Persister;

#[tokio::test]
async fn rw_wal() {
    let mut fp = fp_at("file");

    let write_wal = vec![
        (1, "wal_test".as_bytes()),
        (2, "wal_test2".as_bytes()),
        (3, "wal_test3".as_bytes()),
    ];
    for (term, data) in write_wal.iter() {
        fp.write_wal(*term, *data).await.unwrap();
    }

    for (term, data) in write_wal.iter() {
        let (t, d) = fp.read_wal().await.unwrap().unwrap();
        assert_eq!(t, *term);
        assert_eq!(d.as_slice(), *data);
    }

    assert_eq!(None, fp.read_wal().await.unwrap());

    // When meet EOF, next `read_wal` should start from the beginning
    for (term, data) in write_wal.iter() {
        let (t, d) = fp.read_wal().await.unwrap().unwrap();
        assert_eq!(t, *term);
        assert_eq!(d.as_slice(), *data);
    }

    assert_eq!(None, fp.read_wal().await.unwrap());
}

////////////////////////////////////////////////////////////////////////////////

fn fp_at(dir: &str) -> FilePersister {
    FilePersister::new(format!("/tmp/radis/test/persister/{}", dir).as_str())
}
