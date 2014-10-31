#!/bin/bash

source ./verify-docker.sh

# Image names.
COCKROACH_IMAGE="cockroachdb/cockroach"

# Container names.
COCKROACH_NAME="${HOSTNAME:-local}-roachnode"

# Determine running containers.
CONTAINERS=$(docker ps -a | egrep -e '-roachnode' -e "$COCKROACH_IMAGE" | awk '{print $1}')

# Parse [start|stop] directive.
if [[ $1 == "start" ]]; then
  if [[ ! $CONTAINERS == "" ]]; then
    echo "Local cluster already running; stop cluster using \"$0 stop\""
    exit 1
  fi
elif [[ $1 == "stop" ]]; then
  if [[ $CONTAINERS == "" ]]; then
    echo "Local cluster not running"
    exit 0
  fi
  echo "Stopping containers..."
  docker kill $CONTAINERS > /dev/null
  docker rm $CONTAINERS > /dev/null
  exit 0
else
  echo "Usage: $0 [start|stop]"
  exit 1
fi

# Default number of nodes.
NODES=${NODES:-3}

# Determine docker host for communicating with cockroach nodes.
DOCKERHOST=$(echo ${DOCKER_HOST:-"tcp://127.0.0.1:0"} | sed -E 's/tcp:\/\/(.*):.*/\1/')

# Start the cluster by initializing the first node and then starting
# all nodes in order using the first node as the gossip bootstrap host.
echo "Starting Cockroach cluster with $NODES nodes..."

# Standard arguments for running containers.
STD_ARGS="-P -d"

# Local rpc and http ports.
RPC_PORT=9000
HTTP_PORT=8080

# Start all nodes.
for i in $(seq 1 $NODES); do
  HOSTS[$i]="$COCKROACH_NAME$i"

  # If this is the first node, command is init; otherwise start.
  CMD="start"
  if [[ $i == 1 ]]; then
    CMD="init"
  fi

  # Command args specify two data directories per instance to simulate two physical devices.
  CMD_ARGS="-gossip=${HOSTS[1]}:$RPC_PORT -stores=hdd=/tmp/disk1,hdd=/tmp/disk2 -rpc=${HOSTS[$i]}:$RPC_PORT -http=${HOSTS[$i]}:$HTTP_PORT"

  # Node-specific arguments for node container.
  NODE_ARGS="--hostname=${HOSTS[$i]} --name=${HOSTS[$i]}"
  if [[ $i != 1 ]]; then
      # If this is not the first node then link to the first node for
      # gossip bootstrapping.
      NODE_ARGS="${NODE_ARGS} --link=${HOSTS[1]}:${HOSTS[1]}"
  fi

  # Start Cockroach docker container and corral HTTP port for later
  # verification of cluster health.
  CIDS[$i]=$(docker run $STD_ARGS $NODE_ARGS $COCKROACH_IMAGE $CMD $CMD_ARGS)
  HTTP_PORTS[$i]=$(echo $(docker port ${CIDS[$i]} 8080) | sed 's/.*://')
done

# Get gossip network contents from each node in turn.
MAX_WAIT=20 # seconds
for ATTEMPT in $(seq 1 $MAX_WAIT); do
  ALL_FOUND=1
  for i in $(seq 1 $NODES); do
    GOSSIP=$(curl -s $DOCKERHOST:${HTTP_PORTS[$i]}/_status/gossip)
    for j in $(seq 1 $NODES); do
      if [[ $(echo $GOSSIP | grep "node-$j") == "" ]]; then
        ALL_FOUND=0
        break
      fi
    done
    if [[ $ALL_FOUND == 0 ]]; then
      sleep 1
      break
    fi
  done
  # This will only be true if ALL hosts get ALL gossip.
  if [[ $ALL_FOUND == 1 ]]; then
    echo "All nodes verified in the cluster"
    exit 0
  fi
done

# Print all node logs for debugging.
echo "Failed to verify nodes in cluster after $MAX_WAIT seconds"
for i in $(seq 1 $NODES); do
  echo ""
  echo "Output for ${HOSTS[$i]}..."
  echo "=========================="
  docker logs ${CIDS[$i]}
done

docker kill ${CIDS[*]} > /dev/null
docker rm ${CIDS[*]} > /dev/null
exit 1
