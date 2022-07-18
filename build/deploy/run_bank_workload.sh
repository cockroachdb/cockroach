#!/bin/bash

ready_roaches=0

while [ $ready_roaches -lt 3 ]; do
  roach1_liveness=$(docker-compose exec workload /cockroach/cockroach sql --insecure --url 'postgresql://root@roach1:26257?sslmode=disable' --execute "select membership from crdb_internal.kv_node_liveness" |grep active | wc -l);
  roach2_liveness=$(docker-compose exec workload /cockroach/cockroach sql --insecure --url 'postgresql://root@roach2:26257?sslmode=disable' --execute "select membership from crdb_internal.kv_node_liveness" |grep active | wc -l);
  roach3_liveness=$(docker-compose exec workload /cockroach/cockroach sql --insecure --url 'postgresql://root@roach3:26257?sslmode=disable' --execute "select membership from crdb_internal.kv_node_liveness" |grep active | wc -l);

  if [ $roach1_liveness -eq 3 ]; then
    ready_roaches=$(( $ready_roaches + 1 ))
  fi
  if [ $roach2_liveness -eq 3 ]; then
    ready_roaches=$(( $ready_roaches + 1 ))
  fi
  if [ $roach3_liveness -eq 3 ]; then
    ready_roaches=$(( $ready_roaches + 1 ))
  fi

  if [ $ready_roaches -lt 3 ]; then
    echo "Not all roaches are ready; $ready_roaches out of 3; sleeping..."
    sleep 1
  fi
done

echo "All roaches are ready!"

echo "Executing workload to generate tables and data..."
docker-compose exec workload /cockroach/cockroach workload init bank 'postgresql://root@roach1:26257?sslmode=disable'

echo "Running bank workload..."
docker-compose exec workload /cockroach/cockroach workload run bank --duration=5m 'postgresql://root@roach1:26257?sslmode=disable' --tolerate-errors
