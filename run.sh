#!/bin/bash

./bin/roachprod destroy local || true
./bin/roachprod create -n 5 local
./bin/roachprod put local cockroach
./bin/roachprod start local
tail -F ~/local/*/logs/cockroach.log
