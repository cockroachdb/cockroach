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
while ! roachprod sql local-oxide:1 -- -e "
WITH bad AS (
  SELECT range_id
  FROM crdb_internal.ranges_no_leases
  WHERE cardinality(replicas) != 5
  ORDER BY range_id
  LIMIT 10
),
agg AS (
  SELECT coalesce(string_agg(range_id::string, ', '), '') AS ids,
         count(*) AS n
  FROM bad
)
SELECT CASE
         WHEN n > 0 THEN crdb_internal.force_error(
           '22000',
           'Some ranges do not have 5 replicas: ' || ids
         )
       END
FROM agg;
"; do
  echo "waiting for replication..."
  sleep 5
done

# Create some ranges.
roachprod sql local-oxide:1 -- -e '
INSERT INTO d.t(i) SELECT generate_series(1,100);
ALTER TABLE d.t SPLIT AT SELECT i FROM d.t ORDER by i DESC;
'

echo "stopping n4 and n5"
date
# Stop n4 and n5.
roachprod stop local-oxide:4-5

# After 20 seconds, nodes should be known not to be live. Decommission.
sleep 20
echo "decommission n4 and n5"
date
roachprod run local-oxide:1 -- ./cockroach node decommission --insecure --port {pgport:1} 4 5

roachprod adminui local-oxide:1

for i in $(seq 1 10); do
roachprod sql local-oxide:1 -- -e '
SELECT count(*) FROM d.t;
'
sleep 10
done

