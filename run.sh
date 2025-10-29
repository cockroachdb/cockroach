#!/bin/bash
set -euxo pipefail

roachprod destroy local-oxide
roachprod create -n 5 local-oxide
roachprod put local-oxide ~/crdb/cockroach ./cockroach
roachprod start local-oxide
roachprod sql local-oxide:1 -- -e ' 
CREATE DATABASE d;
CREATE TABLE d.t (i INT PRIMARY KEY);
ALTER RANGE DEFAULT CONFIGURE ZONE USING num_replicas = 5, num_voters = 5;
'

# Wait for 5x replication.
sleep 120

# Create 300 ranges.
roachprod sql local-oxide:1 -- -e '
INSERT INTO d.t(i) SELECT generate_series(1,300);
ALTER TABLE d.t SPLIT AT SELECT i FROM d.t ORDER by i DESC;
'

echo "stopping n4 and n5"
date
# Stop n4 and n5.
roachprod stop local-oxide:4-5

# After 20 seconds, nodes should be known not to be live. Drop replication
# factor.
sleep 20

echo "lowering replication factor"
date
roachprod sql local-oxide:1 -- -e '
ALTER RANGE DEFAULT CONFIGURE ZONE USING num_replicas = 3, num_voters = 3;
'

roachprod adminui local-oxide:1

for i in $(seq 1 10); do
roachprod sql local-oxide:1 -- -e '
SELECT count(*) FROM d.t;
'
sleep 10
done

