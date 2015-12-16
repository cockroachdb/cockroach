#!/bin/bash
# This is a simple wrapper script around the cockroach binary.
# It must be in the same directory as the cockroach binary, and invoked in one of two ways:
#
# To start the first node and initialize the cluster:
# ./launch init [load balancer address:port]
# To start a node:
# ./launch start [load balancer address:port]
set -ex

source config.sh

DATA_DIR="data"
LOG_DIR="logs"
STORES="ssd=${DATA_DIR}"
COMMON_FLAGS="--log-dir=${LOG_DIR} --logtostderr=false --stores=${STORES}"
START_FLAGS="--insecure --addr=${LOCAL_ADDRESS}:${PORT}"

action=$1
if [ "${action}" != "init" -a "${action}" != "start" ]; then
  echo "Usage: ${0} [init|start] [load balancer address:port]"
  exit 1
fi

# Setup iptables rules.
sudo iptables -t nat -A PREROUTING -p tcp --dport ${PORT} -j DNAT --to-destination ${LOCAL_ADDRESS}
sudo iptables -t nat -A POSTROUTING -p tcp -d ${LB_ADDRESS} --dport ${PORT} -j SNAT --to-source ${LOCAL_ADDRESS}

chmod 755 cockroach
mkdir -p ${DATA_DIR} ${LOG_DIR}

if [ "${action}" == "init" ]; then
  ./cockroach init ${COMMON_FLAGS}
fi

cmd="./cockroach start ${COMMON_FLAGS} ${START_FLAGS} --gossip=${GOSSIP}"
nohup ${cmd} > ${LOG_DIR}/cockroach.STDOUT 2> ${LOG_DIR}/cockroach.STDERR < /dev/null &
pid=$!
echo "Launched ${cmd}: pid=${pid}"
# Sleep a bit to let the process start before we terminate the ssh connection.
sleep 5
