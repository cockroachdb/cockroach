#!/bin/bash

docker-compose build --no-cache
docker-compose up -d

echo "Initializing cluster..."
docker exec roach1 ./cockroach init --cluster-name=test --insecure
