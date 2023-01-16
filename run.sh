#!/bin/bash
set -euo pipefail

clus=local

if [ $clus == "local" ] && [ ! -f ~/local/1/.inited ]; then
	roachprod create -n 3 $clus
	roachprod put $clus ./cockroach
	nohup roachprod start $clus &
	touch ~/local/1/.inited
	sleep 10
fi

go run ./pkg/cmd/bigcopy "$@" --db $(roachprod pgurl --external $clus:1 | sed "s/'//g")
