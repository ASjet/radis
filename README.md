# Radis

A raft based distributed K/V database with redis-compatible API, implemented in async rust.

## Getting Started

```shell
# Start a 3-node cluster, will listen on port 63790, 63791, 63792 respectively
./run.sh 3
```

Use `redis-cli` to connect to one of the node:

```shell
redis-cli -p 63790
127.0.0.1:63790> get foo
(nil)
```

Use `redis-cli` to connect to another node and make a write request:

```shell
redis-cli -p 63791
127.0.0.1:63791> get foo
(nil)
127.0.0.1:63791> set foo 123
OK
127.0.0.1:63791> get foo
"123"
```

Then this update will be visible to all nodes:

```shell
127.0.0.1:63790> get foo
"123"
```

## Features

- [x] Automatic failover
- [x] Read/Write splitting
- [x] Write forwarding
- [ ] Log compaction(snapshot)
- [ ] Dynamic membership

## License

Radis is licensed under the [MIT license](LICENSE).
