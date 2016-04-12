#!/bin/bash
# This is a simple wrapper script around the cockroach binary.
# It must be in the same directory as the cockroach binary, and invoked in one of two ways:
#
# To start a node:
# ./launch.sh
set -ex

source config.sh

DATA_DIR="cockroach-data"
LOG_DIR="cockroach-data/logs"
COMMON_FLAGS="--log-dir=${LOG_DIR} --logtostderr=false"
START_FLAGS="--insecure --host=${LOCAL_ADDRESS} --port=${SQL_PORT} --http-port=${HTTP_PORT} --join=${JOIN_ADDRESS}"

# Setup iptables rules.
sudo iptables -t nat -A PREROUTING -p tcp --dport ${SQL_PORT} -j DNAT --to-destination ${LOCAL_ADDRESS}
sudo iptables -t nat -A PREROUTING -p tcp --dport ${HTTP_PORT} -j DNAT --to-destination ${LOCAL_ADDRESS}

chmod 755 cockroach
mkdir -p ${DATA_DIR} ${LOG_DIR}

cmd="./cockroach start ${COMMON_FLAGS} ${START_FLAGS}"
nohup ${cmd} > ${LOG_DIR}/cockroach.STDOUT 2> ${LOG_DIR}/cockroach.STDERR < /dev/null &
pid=$!
echo "Launched ${cmd}: pid=${pid}"
# Sleep a bit to let the process start before we terminate the ssh connection.
sleep 5
