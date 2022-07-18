#!/bin/bash

export docker_registry=gcr.io/cockroach-testeng-infra/
export build_tag="poc-antithesis-latest"

docker-compose build --no-cache
docker-compose up -d

echo "Initializing cluster..."
docker exec roach1 ./cockroach init --cluster-name=test --insecure

./run_bank_workload.sh
