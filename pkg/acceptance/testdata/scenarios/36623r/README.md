Repro config for
https://github.com/cockroachdb/cockroach/issues/36623

- heavy workload 4000/5000 wh
- large geo-dist topology 9 nodes
- initialize but don't start workload
- decommission + quit node
- observe range imbalance

