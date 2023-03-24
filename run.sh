#!/bin/bash

set -euxo pipefail

roachprod destroy local || true
roachprod create -n 1 local
roachprod put local cockroach
roachprod start local
roachprod monitor local --oneshot
roachprod stop local
roachprod start local
sleep 5
roachprod monitor local --oneshot
