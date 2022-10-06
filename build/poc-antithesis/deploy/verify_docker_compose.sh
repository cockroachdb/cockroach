#!/usr/bin/env bash
set -euo pipefail

# required parameters for docker-compose (set -u will throw 'unbound variable')
docker_registry=${docker_registry}
build_tag=${build_tag}
data_dir=${data_dir}

uid=$(id -u)
gid=$(id -g)

cleanup_on_exit() {
  echo "running 'docker-compose down' to clean up..."
  docker-compose -f deploy/docker-compose.yml down || true

  # N.B. docker creates files with root's uid/gid, so we resort to an ugly hack which chowns everything to the host's uid/gid
  docker run --rm -v ${data_dir}:/cockroach/cockroach-data ${docker_registry}/cockroachdb/workload:${build_tag} bash -c "chown -R $uid:$gid /cockroach/cockroach-data"

  # wipe roach data, keep logs
  rm -f ${data_dir}/roach?/* 2>/dev/null || true
  for d in $(find ${data_dir}/roach? -maxdepth 0 -type d); do
    mv $d/logs/* $d/ || true
  done
  find ${data_dir}/roach? -mindepth 1 -type d | xargs rm -rf || true
}
trap cleanup_on_exit EXIT

# start docker-compose and detach
docker-compose -f deploy/docker-compose.yml up --detach

is_workload_running() {
  docker ps -aqf name=workload
}

# wait until workload is ready
while ! is_workload_running;
do
    sleep 1
done

docker ps

echo "Waiting for workload to finish..."

# wait until workload returns or fails (time out is 5 minutes)
while ! done=$(docker logs workload 2>&1 |grep "300.0s") && is_workload_running;
do
    sleep 1
done

echo "Workload is done; dumping header and trailer..."
echo
echo "Header"
echo "================================================"
# dump workload log header and trailer
docker logs workload |head -25 || true
echo
echo "Trailer..."
echo "================================================"
docker logs workload --tail 10 || true
