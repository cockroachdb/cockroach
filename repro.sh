#!/bin/bash

set -euo pipefail

# For real cluster: roachprod create -n 7 $USER-snaps and fill
# in the name here
clus=local

# roachprod create -n 7 local
# roachprod wipe $clus
roachprod stop $clus:1-7 || true
roachprod put $clus ./cockroach-repro ./cockroach

roachprod start $clus:1-7

# Wait for >=3x replication for all replicas
# NB: this takes minutes, don't be alarmed.
# Probably worth reusing the cluster (which is
# why the `wipe` above is commented out).
time roachprod sql $clus:1 -- -e "
select crdb_internal.force_retry('3100ms') from crdb_internal.ranges_no_leases where array_length(replicas, 1) < 3;
"

# NB: this should be more like 1000 on a real cluster.
roachprod run $clus:1 -- ./cockroach workload fixtures import tpcc --warehouses=10 --checks=false.

# Start moving replicas off n[12] to n[34567].
# --wait makes sure we don't permanently remove the nodes & don't block here.
roachprod run $clus:1 ./cockroach node decommission --insecure --wait=none 1 2

# Can run this to have replicas move back to n[12]:
# Can then decommission other nodes, etc. This is to be run manually.
# roachprod run $clus:1 -- ./cockroach node recommission --insecure 1 2

