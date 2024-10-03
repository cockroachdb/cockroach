#!/bin/bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# if you have a roachtest that save tsdump files you can run this from the artifacts dir and it
# will try to start a admin for each tsdump-gob-run.sh it finds.   It prints out the adminurl.
# Requirements:
# - only works on a gceworker (maybe should use localhost for non-gceworker?)
# - assumes ports 8080 and 26257 are in use and starts +1 from there
# Wishlist:
# - figure out timespan and add that to url

i=1
ip=$(gcloud compute instances describe --format="value(networkInterfaces[0].accessConfigs[0].natIP)" $(hostname) --zone us-east1-b)
wd=$PWD
for c in $(find . -name *.gob-run.sh)
do
    cd $(dirname $c)
    addr=$(expr 8080 + $i)
    ./$(basename $c) --http-addr :$addr --listen-addr :$(expr 26257 + $i) --background --store=$(mktemp -d),ballast-size=0 &> /tmp/crdb.out
    if [ $? -ne 0 ]; then
        cat /tmp/crdb.out
        exit 1
    fi
    rm /tmp/crdb.out
    i=$(expr $i + 1)
    echo "http://$ip:$addr" $c
    cd $wd
done

