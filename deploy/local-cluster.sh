#!/bin/bash

source ./verify-docker.sh

# Determine running containers.
CONTAINERS=$(docker ps | grep 'node' | awk '{print $1}')

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
  docker stop $CONTAINERS > /dev/null
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
  HOST="node$i"

  # If this is the first node, command is init; otherwise start.
  CMD="start"
  LINKS=""
  if [[ $i == 1 ]]; then
    CMD="init"
  else
    # If not the first node, set up a link which inserts node1's address
    # into the /etc/hosts file, so the -gossip bootstrap host works.
    LINKS="--link=node1:node1"
  fi

  # Command args specify two data directories per instance to simulate two physical devices.
  CMD_ARGS="-gossip=node1:$RPC_PORT -stores=hdd=/tmp/disk1,hdd=/tmp/disk2 -rpc=$HOST:$RPC_PORT -http=$HOST:$HTTP_PORT"

  # Node-specific arguments for node container.
  NODE_ARGS="--hostname=$HOST --name=$HOST $LINKS"

  CIDS[$i]=$(docker run $STD_ARGS $NODE_ARGS cockroachdb/cockroach $CMD $CMD_ARGS)
  HTTP_PORTS[$i]=$(echo $(docker port ${CIDS[$i]} 8080) | sed 's/.*://')
done

# Get gossip network contents from each node in turn.
MAX_WAIT=10 # in seconds
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
  if [[ $ALL_FOUND == 1 ]]; then
    echo "All nodes verified in the cluster"
    exit 0
  fi
done

# Print all node logs for debugging.
echo "Failed to verify nodes in cluster after $MAX_WAIT seconds"
for i in $(seq 1 $NODES); do
  echo ""
  echo "Output for node$i..."
  echo "===================="
  docker logs ${CIDS[$i]}
done

docker stop ${CIDS[*]} > /dev/null
docker rm ${CIDS[*]} > /dev/null
exit 1
