#!/bin/bash
set -euxo pipefail

sha=$(git rev-parse --short HEAD)
a="artifacts.${sha}.$(date '+%H%M%S')"
mkdir -p "${a}"

rt="${a}/roachtest"
rp="${a}/roachprod"
wl="${a}/workload"
cr="${a}/cockroach"

if [ ! -f "${rt}" ]; then
	./build/builder.sh mkrelease amd64-linux-gnu bin/{roach{prod,test},workload}
	mv -f bin.docker_amd64/roachprod "${rp}"
	mv -f bin.docker_amd64/workload "${wl}"
	mv -f bin.docker_amd64/roachtest "${rt}"
fi

if [ ! -f "${cr}" ]; then
	git clean -xffd ./pkg
	./build/builder.sh mkrelease amd64-linux-gnu
	mv cockroach-linux-2.6.32-gnu-amd64 "${cr}"
fi

TEST=restore2TB/nodes=10
time caffeinate -- "${rt}" run "${TEST}" --port 8081 --count 1 --cpu-quota 1200 --roachprod "${rp}" --workload "${wl}" --cockroach "${cr}" --artifacts "${a}"

