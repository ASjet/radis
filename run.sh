#!/bin/bash
set -e

NODES=${1:-3}
NODE_PREFIX=${2:-node}
LOG_DIR=${3:-./logs}

mkdir -p $LOG_DIR

echo "Running $NODES nodes with prefix $NODE_PREFIX"

cargo run --bin mkconf -- -p $NODES
cargo build
for node in $(seq 1 $NODES); do
    node_name="${NODE_PREFIX}$(($node - 1))"
    ./target/debug/radis -c ${node_name}.toml > ${LOG_DIR}/${node_name}.log &
done

function cleanup {
    pkill -9 radis
    rm -f ${NODE_PREFIX}*.toml
    exit 0
}

trap 'cleanup 2>/dev/null' INT
wait < <(jobs -p)
