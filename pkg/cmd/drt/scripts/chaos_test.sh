#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -u
set -o pipefail

# Assumptions : roachprod and gcloud are available on machine executing this script

# Constants
RP="${HOME}/roachprod"
CRDB_CUSTOM="" # custom cockroach binary local path to stage, CRDB_DEFAULT is used if empty
CRDB_DEFAULT='v23.2.0'  # If CRDB_CUSTOM is empty, version of cockroach binary to be staged
WAIT_TIME_LB=10    # lower bound in secs, applicable only with -r
WAIT_TIME_UB=600  # upper bound in secs, applicable only with -r
WAIT_TIME_BETWEEN=600   # amount of time to wait between successive operations
POLL_INTERVAL=1    # polling interval in secs to check if node is alive
KILL_TIMEOUT=5    # max time(secs) a node should take to stop after kill is issued
DRAIN_TIMEOUT=600  # max time(secs) a node should take to drain and stop after drain cmd is issued

# Defaults
CLUSTER_NAME='cct-232'
WAIT_TIME=60    # seconds to wait before restoring chaos condition, ignored for -r
CREATE_CLUSTER=false
KILL_DB_NODE=false
DRAIN_DB_NODE=false
DISK_STALL=false
NW_PARTITION=false
RUN_CHAOS=false

while getopts 'c:dhi:kn:prsw:y' flag
do
    case "${flag}" in
  c) CLUSTER_NAME=${OPTARG};;
  d) DRAIN_DB_NODE=true;;
  i) NODE_ID=${OPTARG};;
  k) KILL_DB_NODE=true;;
  n) NODE_COUNT=${OPTARG};;
  p) NW_PARTITION=true;;
  r) RUN_CHAOS=true;;
  s) DISK_STALL=true;;
  w) WAIT_TIME=${OPTARG};;
  y) CREATE_CLUSTER=true;;
  ?|h) echo "Usage: $(basename $0) [-c <cluster-name>] [-d] [-i <node_id>] [-k] [-n <node_count>] [-p] [-s] [-w <seconds>] [-y]"
    echo "-c : optional, cluster name for existing or new cluster to be created"
    echo "-d : drain a node, if -i is provided, that specific node will be drained, else last node."
    echo "-h : print script usage help"
    echo "-i : optional, provide node id of cluster on which operation is performed, default is last node."
    echo "-k : kill a node, if -i is provided, that specific node will be killed, else last node."
    echo "-n : optional, node count of cluster to be created, default is 4, used with -y"
    echo "-p : network paritition a node, if -i is provided, that specific node will be isolated, else last node."
    echo "-r : run different chaos tests infinitely one at a time picked randomly."
    echo "-s : create disk stall on a node, if -i is provided, that specific node will have disk stall, else last node."
    echo "-w : optional, wait time in seconds used with -p, -s, -k, -d, not used with -r, wait times are randomly picked between pre-configured range."
    echo "-y : create a new cluster, only created if cluster with same name does not exist."
    exit 1;;
    esac
done


logmsg() {
  echo "$( date -u +"%F %T" ) $@"
}

validate_status() {
  status=$1
  if [[ $status -ne 0 ]]; then
    if [[ -n $2 ]]; then
      logmsg "$2"
    else
                  logmsg "roachprod command exited with non-zero status $status"
    fi
    exit $status
        fi
}

get_cluster_json() {
  CLUSTER_FILE="$(dirname $0)/${CLUSTER_NAME}.json"
  $RP list --pattern "^${CLUSTER_NAME}$" --json > $CLUSTER_FILE 2> /dev/null
  if [[ $(cat $CLUSTER_FILE | wc -c) -eq 0 ]]; then
    logmsg "Error! Failed to generate cluster json file"
    exit 1
  fi
}

is_cluster_exist() {
  if [[ $(cat $CLUSTER_FILE | grep -v skipping | jq '.clusters | length') -eq 1 ]];then
    echo true  
  else
    echo false
  fi
}

get_cluster_node_count() {
  echo $(cat $CLUSTER_FILE | grep -v skipping | jq '.clusters[].vms | length')
}

create_cluster() {
  if $( is_cluster_exist ); then
    logmsg "Error! Cluster with name $CLUSTER_NAME already exist. Can't create"
    exit 1
  fi
  logmsg "Creating cluster $CLUSTER_NAME with $NODE_COUNT nodes"
  $RP create $CLUSTER_NAME \
    --clouds gce \
    --nodes ${NODE_COUNT} \
    --local-ssd=false \
    --label "usage=$CLUSTER_NAME"
  validate_status $?
  if [[ -n "$CRDB_CUSTOM" ]]; then
    logmsg "CRDB_CUSTOM used"
    $RP put $CLUSTER_NAME $CRDB_CUSTOM
  else
    logmsg "CRDB_DEFAULT used"
    $RP stage $CLUSTER_NAME release $CRDB_DEFAULT
  fi
  validate_status $?
  $RP start ${CLUSTER_NAME} --secure --binary ./cockroach --args=--log="file-defaults: {dir: 'logs', max-group-size: 1GiB}"
  validate_status $?
  get_cluster_json
}

get_node_run_cmd() {
  echo "$RP run ${CLUSTER_NAME}:${NODE_ID} "
}

setup_env() {
  get_cluster_json
  if $CREATE_CLUSTER ;then
    NODE_COUNT=${NODE_COUNT:-4}
          create_cluster
    validate_status $? "Error! Failed to create cluster $CLUSTER_NAME "
    logmsg "Created cluster $CLUSTER_NAME with $NODE_COUNT nodes"
  else
    if ! $( is_cluster_exist ); then
      logmsg "Error! Cluster with name $CLUSTER_NAME does not exist."
      exit 1
    fi
    NODE_COUNT=$( get_cluster_node_count )
    logmsg "Found cluster $CLUSTER_NAME with $NODE_COUNT nodes"
  fi
  NODE_ID=${NODE_ID:-${NODE_COUNT}}
  if [[ $NODE_ID -gt ${NODE_COUNT} ]]; then
    logmsg "Error! invalid node selected '$NODE_ID', cluster contains ${NODE_COUNT} nodes"
    exit 1
  fi
  SSHNODE="$( get_node_run_cmd )"
  GCLOUD_ACCOUNT="$(gcloud auth  list --filter active --format default | grep 'account' | awk '{print $2}')"
}

# Generate bounded random numbers
get_random() {
  ub=$1 # inclusive upper bound
  lb=${2:-1} # optional, inclusive lower bound, default 1
  echo $(( ( RANDOM % ($ub - $lb + 1) ) + $lb ))
}

wait_random_time () {
  WAIT_TIME=$( get_random $WAIT_TIME_UB $WAIT_TIME_LB )
  logmsg "Random wait for $WAIT_TIME secs"
  sleep $WAIT_TIME
}

wait_between_ops () {
  logmsg "Waiting for $WAIT_TIME_BETWEEN seconds between chaos operations"
  sleep $WAIT_TIME_BETWEEN
}

add_grafana_annotation() {
  TAG=$1
  MESSAGE=$2
  DASHBOARD="tQOYKAKIk"
  logmsg "Annotation Tag: ${TAG}, Msg: ${MESSAGE}"
  gcloud config set account grafanabot@cockroach-testeng-infra.iam.gserviceaccount.com 2> /dev/null
  TOKEN=$(gcloud auth print-identity-token --audiences="1063333028845-p47csl1ukrgnpnnjc7lrtrto6uqs9t37.apps.googleusercontent.com")
  gcloud config set account $GCLOUD_ACCOUNT 2> /dev/null
  GRAFANA_API="https://grafana.testeng.crdb.io/api/annotations"
  EPOCH=$(($(date +%s%N)/1000000))
  curl --silent --output /dev/null --show-error --fail \
       -H "Authorization: Bearer ${TOKEN}" \
       -H "Content-Type: application/json" \
       -X POST ${GRAFANA_API} \
       --data "{\"time\":${EPOCH},\"isRegion\":true,\"tags\":[\"${TAG}\"],\"text\":\"${MESSAGE}\",\"timeEnd\":${EPOCH},\"dashboardUID\":\"${DASHBOARD}\"}"
  if [[ $? -ne 0 ]] ;then
    logmsg "Annotation failed! Tag: ${TAG}, Msg: ${MESSAGE}"
  fi
}

is_node_alive() {
  alive_status=$( $RP status ${CLUSTER_NAME}:${NODE_ID} ) 
  if echo "$alive_status" | grep -q 'not running'; then
    logmsg "${CLUSTER_NAME}:${NODE_ID} not running!"
    logmsg "$alive_status"
    return 1
  fi
  return 0
}

is_node_connected () {
  conn_status=$( $RP sql ${CLUSTER_NAME}:${NODE_ID} --secure -- -e "select 1;" )
  if echo "$conn_status" | grep -q 'ERROR'; then
    logmsg "Error! Node $NODE_ID is disconnected!"
    logmsg "$conn_status"
    return 1
  fi
  return 0
}

start_node() {
  logmsg "Starting cockroach node ${CLUSTER_NAME}:${NODE_ID}"
  $RP start ${CLUSTER_NAME}:${NODE_ID} --secure --binary ./cockroach --args=--log="file-defaults: {dir: 'logs', max-group-size: 1GiB}"
  validate_status $?
  add_grafana_annotation "ChaosNodeStart" "Node ${NODE_ID}"
}

wait_till_node_stop() {
  exit_timeout=$1
  logmsg "Waiting for $CLUSTER_NAME:$NODE_ID to stop(max wait $exit_timeout secs)"
  exit_epoch=$(( $(date +%s)+${exit_timeout} ))
  while true
  do
    is_node_alive
    if [[ $? -eq 0 ]] ; then
      sleep $POLL_INTERVAL
    else
      break  
    fi
    cur_epoch=$(date +%s)
    if [[ $cur_epoch -gt $exit_epoch ]]; then
      logmsg "Error! ${CLUSTER_NAME}:${NODE_ID} node did not stop in given timeout: $exit_timeout secs"
      exit 1
    fi 
  done
}

# Kill a node (SIGKILL)
kill_node() {
  logmsg "Killing cockroach node ${CLUSTER_NAME}:${NODE_ID}"
  add_grafana_annotation "ChaosNodeKill" "Node ${NODE_ID}"
  $RP signal ${CLUSTER_NAME}:${NODE_ID} 9
  validate_status $?
  wait_till_node_stop $KILL_TIMEOUT
}

# Drain a node (SIGTERM)
drain_node() {
  logmsg "Draining cockroach node ${CLUSTER_NAME}:${NODE_ID}"
  add_grafana_annotation "ChaosNodeDrain" "Node ${NODE_ID}"
  $RP signal ${CLUSTER_NAME}:${NODE_ID} 15
  validate_status $?
  wait_till_node_stop $DRAIN_TIMEOUT
}

# Start disk stall on a node
start_disk_stall() {
  logmsg "Starting disk stall on cockroach node $CLUSTER_NAME:$NODE_ID"
  device_number="$( $SSHNODE -- 'lsblk' | grep data1 | awk '{print $2}' )"
  cmd="$device_number rbps=max wbps=1024 riops=max wiops=10"
  add_grafana_annotation "ChaosDiskStallStart" "Node ${NODE_ID}"
  $SSHNODE <<-EOF
  sudo su -
  echo $cmd > /sys/fs/cgroup/system.slice/io.max
  cat /sys/fs/cgroup/system.slice/io.max
  EOF
}

# Stop disk stall on a node
stop_disk_stall() {
  logmsg "Stopping disk stall on cockroach node ${CLUSTER_NAME}:${NODE_ID}"
  set -x
  # Loop is to handle ssh failures and add retry
  while true
  do
    blk_dev_info="$( $SSHNODE -- 'lsblk')"
    if [[ -n $blk_dev_info ]]; then
      break
    fi
    sleep 1
  done
  device_number=$( echo "$blk_dev_info" | grep data1 | awk '{print $2}' )
  cmd="$device_number rbps=max wbps=max riops=max wiops=max"
  $SSHNODE <<-EOF
  sudo su -
  echo $cmd > /sys/fs/cgroup/system.slice/io.max
  cat /sys/fs/cgroup/system.slice/io.max
  EOF
  set +x
  is_node_alive
  if [[ $? -ne 0 ]]; then
    start_node
  fi
  add_grafana_annotation "ChaosDiskStallStop" "Node ${NODE_ID}"
}

# Start iptables based network partitioning  of a node blocking join port
start_ipt_nw_partition() {
  logmsg "Isolating ${CLUSTER_NAME}:${NODE_ID} node using iptables network partitioning"
  port=$( $RP pgurl ${CLUSTER_NAME}:${NODE_ID} | cut -d: -f3 | cut -d? -f1  )
  add_grafana_annotation "ChaosNWPtStart" "Node ${NODE_ID}"
  $SSHNODE -- "sudo iptables -I INPUT 1 -p tcp --dport $port -j DROP"
  $SSHNODE -- "sudo iptables -I OUTPUT 1 -p tcp --dport $port -j DROP"
}

# Stop iptables based network partitioning of a node unblocking join port
stop_ipt_nw_partition() {
  logmsg "Restoring ${CLUSTER_NAME}:${NODE_ID} node network using iptables"
  port=$( $RP pgurl ${CLUSTER_NAME}:${NODE_ID} | cut -d: -f3 | cut -d? -f1  )
  #$SSHNODE -- "sudo iptables -D INPUT -p tcp --dport $port -j DROP"
  #$SSHNODE -- "sudo iptables -D OUTPUT -p tcp --dport $port -j DROP"
  $SSHNODE -- "sudo iptables -F"
  add_grafana_annotation "ChaosNWPtStop" "Node ${NODE_ID}"
}

fix_if_disk_stall() {
  if [[ $($SSHNODE -- "cat /sys/fs/cgroup/system.slice/io.max" | wc -c) -ne 0 ]]; then
    stop_disk_stall
  fi
}

fix_if_nw_partition() {
  if $SSHNODE -- "sudo iptables -S" | grep -q DROP; then
    stop_ipt_nw_partition
  fi
}

# Fix cluster nodes if broken
fix_cluster_health() {
  temp=${NODE_ID}
  for i in `seq 1 $NODE_COUNT`
  do
    NODE_ID=$i
    SSHNODE="$( get_node_run_cmd )"
    fix_if_nw_partition
    fix_if_disk_stall
    ! is_node_alive && start_node
    is_node_connected
    if [[ $? -ne 0 ]]; then
      kill_node
      start_node
      is_node_alive
      validate_status $? "Error! Not able to start node ${CLUSTER_NAME}:${NODE_ID}. Exiting"
      is_node_connected
      validate_status $? "Error! Not able to fix node ${CLUSTER_NAME}:${NODE_ID} connection issues. Exiting"
    fi
  done
  NODE_ID=${temp}
  SSHNODE="$( get_node_run_cmd )"
}

is_any_node_down() {
  alive_status=$( $RP status ${CLUSTER_NAME} ) 
  if echo "$alive_status" | grep -q 'not running'; then
    logmsg "Error! Some nodes are down!"
    logmsg "$alive_status"
    return 0
  fi
  return 1
}

is_any_node_disconnected() {
  conn_status=$( $RP sql ${CLUSTER_NAME} --secure -- -e "select 1;" ) 
  if echo "$conn_status" | grep -q 'ERROR'; then
    logmsg "Error! Some nodes are disconnected!"
    logmsg "$conn_status"
    return 0
  fi
  return 1
}

check_cluster_health() {
  logmsg "Checking cluster health"
  ( is_any_node_down || is_any_node_disconnected ) && fix_cluster_health
  logmsg "Done checking cluster health"
}

# run chaos operations randomly one at time in loop
run_chaos_tests_nonstop() {
  while true
  do
    NODE_ID=$( get_random $NODE_COUNT  )
    SSHNODE="$( get_node_run_cmd )"
    ops=$( get_random 4 )
    case "$ops" in
      1) kill_node; wait_random_time; start_node ;;
      2) drain_node; wait_random_time; start_node ;;
      3) start_disk_stall; wait_random_time; stop_disk_stall ;;
      4) start_ipt_nw_partition; wait_random_time; stop_ipt_nw_partition ;;
      *) logmsg "Error! Bug in random operation generation logic. Exiting..."; exit 1 ;;
    esac
    check_cluster_health
    wait_between_ops
  done
}

cleanup() {
  check_cluster_health
  gcloud config set account $GCLOUD_ACCOUNT 2> /dev/null
}

# ===========Execution starts here ==============
logmsg "Chaos Script Started"

# Setup Environment
setup_env
check_cluster_health
trap cleanup EXIT

# kill a cockroach node
if $KILL_DB_NODE ;then
  kill_node
  sleep $WAIT_TIME
  start_node
fi

# Drain a node
if $DRAIN_DB_NODE ;then
  drain_node
  sleep $WAIT_TIME
  start_node
fi

# Disk stall
if $DISK_STALL ;then
  start_disk_stall
  sleep $WAIT_TIME
  stop_disk_stall
fi

# Network partition
if $NW_PARTITION ; then
  start_ipt_nw_partition
  sleep $WAIT_TIME
  stop_ipt_nw_partition
fi

# Run chaos test in loop infinitely
if $RUN_CHAOS ; then
  run_chaos_tests_nonstop
fi

logmsg "Chaos script Ended"
