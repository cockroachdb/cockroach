#!/bin/bash
# Start a local cluster and verify the gossip network connects.
# You can override the Cockroach image by supplying COCKROACH_IMAGE.
# The default choice is cockroachdb/cockroach-dev; to run deployment
# image acceptance tests, use cockroachdb/cockroachdb instead.
#
# Author: Spencer Kimball (spencerkimball@gmail.com)

cd "$(dirname $0)"

source ../build/verify-docker.sh

# Image names.
DNSMASQ_IMAGE="cockroachdb/dnsmasq"
COCKROACH_IMAGE="${COCKROACH_IMAGE:-cockroachdb/cockroach-dev}"

# Container names.
DNSMASQ_NAME="${HOSTNAME:-local}-cockroach-dns"
COCKROACH_NAME="${HOSTNAME:-local}-roachnode"

# Determine running containers.
CONTAINERS_RUN=$(docker ps | egrep -e '-roachnode|-cockroach-dns' | awk '{print $1}')
CONTAINERS=$(docker ps -a | egrep -e '-roachnode|-cockroach-dns' | awk '{print $1}')

# Parse [start|stop] directive.
if [[ $1 == "start" ]]; then
  if [[ $CONTAINERS_RUN != "" ]]; then
    echo "Local cluster already running; stop cluster using \"$0 stop\":"
    echo "${CONTAINERS}"
    exit 1
  fi
elif [[ $1 == "stop" ]]; then
  if [[ $CONTAINERS == "" ]]; then
    exit 0
  fi
  echo "Stopping containers..."
  docker kill $CONTAINERS > /dev/null
  exit 0
else
  echo "Usage: $0 [start|stop]"
  exit 1
fi

# Make sure to clean up any remaining containers
$0 stop
# Doing this here only so that after a cluster has been started and stopped,
# the containers are still available for debugging.
echo "Removing any old containers..."
docker rm $CONTAINERS > /dev/null

# Default number of nodes.
NODES=${NODES:-3}

# Determine docker host for communicating with cockroach nodes.
DOCKERHOST=$(echo ${DOCKER_HOST:-"tcp://127.0.0.1:0"} | sed -E 's/tcp:\/\/(.*):.*/\1/')

# Start the cluster by initializing the first node and then starting
# all nodes in order using the first node as the gossip bootstrap host.
echo "Starting Cockroach cluster with $NODES nodes:"

# Standard arguments for running containers.
STD_ARGS="-P -d"

# Shell script cleanup for DNS.
function finish {
  rm -rf $DNS_DIR
  # NOTE: this is a hack due to boot2docker's incomplete support
  # for sharing volumes from the host OS down through to the VM.
  if [[ $DOCKERHOST != "127.0.0.1" ]]; then
    boot2docker ssh "sudo -u root rm -rf $DNS_DIR"
  fi
}
trap finish EXIT

# Create temporary file for DNS hosts.
DNS_DIR=$(mktemp -d "/tmp/dnsmasq.hosts.XXXXXXXX" || exit 1)
DNS_FILE="$DNS_DIR/addn-hosts"

# Start dnsmasq container. We wait in a loop until the DNS additional
# hosts file appears before starting the dnsmasq process.
DNS_CID=$(docker run -d -v "$DNS_DIR:/dnsmasq.hosts" --name=$DNSMASQ_NAME $DNSMASQ_IMAGE /bin/sh -c "while true; do if [ -f /dnsmasq.hosts/addn-hosts ]; then break; else echo 'waiting 1s for DNS address info...'; sleep 1; fi; done; cat /dnsmasq.hosts/addn-hosts; cat /etc/resolv.dnsmasq.conf; /usr/sbin/dnsmasq -d")
DNS_IP=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' $DNS_CID)
echo "* ${DNSMASQ_NAME}"

# Local rpc and http ports.
RPC_PORT=9000
HTTP_PORT=8080

# Start all nodes.
for i in $(seq 1 $NODES); do
  HOSTS[$i]="$COCKROACH_NAME$i.local"

  # If this is the first node, command is init; otherwise start.
  CMD="start"
  if [[ $i == 1 ]]; then
    CMD="init"
  fi
  # Command args specify two data directories per instance to simulate two physical devices.
  CMD_ARGS="-gossip=${HOSTS[1]}:$RPC_PORT -stores=hdd=/tmp/disk1,hdd=/tmp/disk2 -rpc=${HOSTS[$i]}:$RPC_PORT -http=${HOSTS[$i]}:$HTTP_PORT"
  # Log (almost) everything.
  CMD_ARGS="${CMD_ARGS} -v 7"

  # Node-specific arguments for node container.
  NODE_ARGS="--hostname=${HOSTS[$i]} --name=${HOSTS[$i]} --dns=$DNS_IP"

  # Start Cockroach docker container and corral HTTP port and docker
  # IP address for container-local DNS.
  CIDS[$i]=$(docker run $STD_ARGS $NODE_ARGS $COCKROACH_IMAGE $CMD $CMD_ARGS)
  HTTP_PORTS[$i]=$(echo $(docker port ${CIDS[$i]} 8080) | sed 's/.*://')
  IP=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' ${CIDS[$i]})
  IP_HOST[$i]="$IP ${HOSTS[$i]}"
  echo "* ${HOSTS[$i]}"
done

# Write the DNS_FILE in one fell swoop.
printf -- '%s\n' "${IP_HOST[@]}" > $DNS_FILE

# If the docker host is not local, use boot2docker ssh to write
# the data locally.
#
# NOTE: once boot2docker has better support for sharing volumes
# this step will not be necessary. However, what currently happens
# is that the share folder on the boot2docker VM is simply empty,
# regardless of what we write to the local folder.
if [[ $DOCKERHOST != "127.0.0.1" ]]; then
  echo "writing $DNS_FILE to boot2docker VM"
  cat $DNS_FILE | boot2docker ssh "sudo -u root /bin/sh -c 'cat - > $DNS_FILE'"
fi

# Get gossip network contents from each node in turn.
echo -n "Waiting for complete gossip network of $((NODES*NODES)) peerings: "
MAX_WAIT=20 # seconds
for ATTEMPT in $(seq 1 $MAX_WAIT); do
  FOUND=0
  for i in $(seq 1 $NODES); do
    FOUND_NAMES=""
    GOSSIP_URL="$DOCKERHOST:${HTTP_PORTS[$i]}/_status/gossip"
    GOSSIP=$(curl --noproxy '*' -s $GOSSIP_URL)
    for j in $(seq 1 $((2*NODES))); do
      if [[ ! -z $(echo $GOSSIP | grep "node-$j") ]]; then
        FOUND=$((FOUND+1))
        FOUND_NAMES="$FOUND_NAMES node-$j"
      fi
    done
  done
  echo -n "$FOUND "
  # This will only be true if ALL hosts get ALL gossip.
  if [[ $FOUND == $((NODES*NODES)) ]]; then
    echo
    echo "All nodes verified in the cluster:"
    echo $FOUND_NAMES
    exit 0
  fi
  sleep 1
done

# Print all node logs for debugging.
echo
echo "Failed to verify nodes in cluster after $MAX_WAIT seconds"
echo "Last seen nodes: $FOUND_NODES"
for i in $(seq 1 $NODES); do
  echo ""
  echo "Output for ${HOSTS[$i]}..."
  echo "=========================="
  docker logs ${CIDS[$i]}
done

echo ""
echo "Output for dnsmasq..."
echo "====================="
docker logs $DNS_CID

docker kill ${CIDS[*]} > /dev/null
docker rm ${CIDS[*]} > /dev/null
docker kill $DNS_CID > /dev/null
docker rm $DNS_CID > /dev/null
exit 1
