#!/bin/bash

set -euo pipefail

roachprod destroy local || true
roachprod create -n 3 local
roachprod put local ./cockroach

roachprod start local

(cat ~/local/*/logs/cockroach.log && tail -F ~/local/*/logs/cockroach.log) | grep --line-buffered -E 'XXX|YYY' | tee log.txt

