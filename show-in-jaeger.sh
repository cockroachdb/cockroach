#!/usr/bin/env bash
set -euo pipefail
container=$(docker run -d -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 -p 9411:9411 -p 16686:16686 jaegertracing/all-in-one:1.28)
function cleanup {
  docker kill "${container}"
}
trap cleanup EXIT
sleep 1
curl --retry 5 --retry-delay 1 --retry-connrefused -X POST --data-binary @trace-jaeger.json -H "Content-Type: application/json" http://localhost:9411/api/v2/spans
echo "http://localhost:16686"
while :; do sleep 1000 ; done
