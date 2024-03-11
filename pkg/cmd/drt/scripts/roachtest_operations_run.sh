#! /bin/bash

cd ~

while true; do
	./roachtest-operations run-operation ".*" --certs-dir ./certs --cluster "cct-232" --cockroach-binary "cockroach" --virtual-cluster "application" | tee -a roachtest_ops.log
	sleep 10
done
