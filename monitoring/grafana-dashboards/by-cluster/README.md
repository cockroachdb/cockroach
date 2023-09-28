# Grafana Dashboards

The files in this directory contain example Grafana dashboard configurations to act as a starting point for users 
looking to monitor CockroachDB via Grafana. 

Note that while the CockroachDB team provides these dashboards as an example, these dashboards are not guaranteed to
function out of the box with all deployment modes for Prometheus/Grafana/etc., and the CockroachDB team does not offer 
support as such.

Note that the dashboards assume the presence of the following labels on each CockroachDB metric:

- `job="cockroachdb"`: to denote the CockroachDB job that the metrics belong to. 
- `cluster`: to identify the specific cluster that the metrics belong to.
- `instance`: to identify the specific node/instance that the metrics belong to.

Users may have to modify the dashboards according to their own deployment configurations, such as the metric label selectors
used, before they function properly.

Please refer to the
[documentation on how to monitor CockroachDB with Prometheus](https://www.cockroachlabs.com/docs/stable/monitor-cockroachdb-with-prometheus)
for more details. 
