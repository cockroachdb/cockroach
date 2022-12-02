# Display CockroachDB metrics to Splunk Dashboard

This demonstration shows how the OpenTelemetry Collector can send data from CockroachDB to a Splunk instance.

## Setup

The architecture is simple: CockroachDB --> OTEL Collector --> Splunk.

### CockroachDB Cluster

As customary, we use a Load Balancer to interact with the CockroachDB cluster.
Create the `haproxy.cfg` file and save it on the current directory.

```yaml
# file: haproxy.cfg
global
  maxconn 4096

defaults
    mode                tcp
    timeout connect     10s
    timeout client      10m
    timeout server      10m
    option              clitcpka

listen psql
    bind :26257
    mode tcp
    balance roundrobin
    option httpchk GET /health?ready=1
    server cockroach1 cockroach1:26257 check port 8080
    server cockroach2 cockroach2:26257 check port 8080
    server cockroach3 cockroach3:26257 check port 8080
    server cockroach4 cockroach4:26257 check port 8080

listen http
    bind :8080
    mode tcp
    balance roundrobin
    option httpchk GET /health?ready=1
    server cockroach1 cockroach1:8080 check port 8080
    server cockroach2 cockroach2:8080 check port 8080
    server cockroach3 cockroach3:8080 check port 8080
    server cockroach4 cockroach4:8080 check port 8080

```

Create the docker network and containers

```bash
# create the network bridge
docker network create --driver=bridge --subnet=172.28.0.0/16 --ip-range=172.28.0.0/24 --gateway=172.28.0.1 demo-net

# CockroachDB cluster
docker run -d --name=cockroach1 --hostname=cockroach1 --net demo-net cockroachdb/cockroach:latest start --insecure --join=cockroach1,cockroach2,cockroach3
docker run -d --name=cockroach2 --hostname=cockroach2 --net demo-net cockroachdb/cockroach:latest start --insecure --join=cockroach1,cockroach2,cockroach3
docker run -d --name=cockroach3 --hostname=cockroach3 --net demo-net cockroachdb/cockroach:latest start --insecure --join=cockroach1,cockroach2,cockroach3
docker run -d --name=cockroach4 --hostname=cockroach4 --net demo-net cockroachdb/cockroach:latest start --insecure --join=cockroach1,cockroach2,cockroach3

# initialize the cluster
docker exec -it cockroach1 ./cockroach init --insecure

# HAProxy load balancer
docker run -d --name haproxy --net demo-net -p 26257:26257 -p 8080:8080 -v `pwd`/haproxy.cfg:/etc/haproxy.cfg:ro haproxy:latest -f /etc/haproxy.cfg
```

At this point you should be able to view the CockroachDB Admin UI at <http://localhost:8080>.

Start a workload against the cluster to generate some metrics

```bash
# init
docker run --rm --name workload --net demo-net cockroachdb/cockroach:latest workload init tpcc 'postgres://root@haproxy:26257?sslmode=disable' --warehouses 10
# run the workload - you might want to use a separate terminal
docker run --rm --name workload --net demo-net cockroachdb/cockroach:latest workload run tpcc 'postgres://root@haproxy:26257?sslmode=disable' --warehouses 10 --tolerate-errors
```

With 4 nodes, you can simulate a node failure (just stop the container) and view the range activity (replication, lease-transfers, etc).
You can optionally setup CDC to a kafka container, configure Row Level TTL, add more nodes, etc.

### Splunk

Start Splunk

```bash
docker run -d --name splunk --net demo-net -p 8088:8088 -p 8000:8000 -e "SPLUNK_START_ARGS=--accept-license" -e "SPLUNK_PASSWORD=cockroach" splunk/splunk
```

Login into Splunk at <http://localhost:8000> as user `admin` with password `cockroach`.

1. Create a data input and token for HEC
2. In Splunk, click **Settings > Data Inputs**.
3. Under **Local Inputs**, click **HTTP Event Collector**.
    1. Click **Global Settings**.
    2. For **All Tokens**, click **Enabled** if this button is not already selected.
    3. Uncheck the **SSL** checkbox.
    4. Click **Save**.
4. Configure an HEC token for sending data by clicking **New Token**.
5. On the **Select Source** page, for **Name**, enter a token name, for example "Metrics token".
6. Leave the other options blank or unselected.
7. Click **Next**.
8. On the **Input Settings** page, for **Source type**, click **New**.
9. In **Source Type**, set value to "otel".
10. For **Source Type Category**, select **Metrics**.
11. Next to **Default Index**, click **Create a new index**.
    In the **New Index** dialog box:
    1. Set **Index Name** to "metrics_idx".
    2. For **Index Data Type**, click **Metrics**.
    3. Click **Save**.
12. Select the newly created "metrics_idx".
13. Click **Review**, and then click **Submit**.
14. Copy the **Token Value** that is displayed. This HEC token is required for sending data.

### OpenTelemetry Collector

Create file `config.yaml` and save it in the current directory.
Ensure to replace the **Splunk Token** with the one you created in the previous step.

```yaml
# file: config.yaml
---
receivers:
  prometheus:
    config:
      scrape_configs:
        - job_name: 'cockroachdb'
          metrics_path: '/_status/vars'
          scrape_interval: 10s
          scheme: 'http'
          tls_config:
            insecure_skip_verify: true
          static_configs:
          - targets: 
              - cockroach1:8080
              - cockroach2:8080
              - cockroach3:8080
              - cockroach4:8080
            labels:
              cluster_id: 'cockroachdb'

exporters:
  splunk_hec:
    source: otel
    sourcetype: otel
    index: metrics_idx
    max_connections: 20
    disable_compression: false
    timeout: 10s
    tls:
      insecure_skip_verify: true
    token: TOKEN
    endpoint: "http://splunk:8088/services/collector"


service:
  pipelines:
    metrics:
      receivers: 
        - prometheus
      exporters: 
        - splunk_hec
```

Start the Collector

```bash
docker run -d --name otel --net demo-net -v `pwd`/config.yaml:/etc/config.yaml ghcr.io/open-telemetry/opentelemetry-collector-releases/opentelemetry-collector-contrib --config=/etc/config.yaml
```

CockroachDB metrics should be now pulled by the Prometheus **Receiver** and forwarded to Splunk via the Splunk HEC **Exporter**.

## Demo

After few minutes, you can do a quick test and run the below query in Splunk to make sure data is received correctly.
In Splunk, click on **Apps**, then **Search & Reporting**.
Enter below commands:

```bash
# check what metrics we're receiving
| mcatalog values(metric_name) WHERE index="metrics_idx"

# preview the data in its raw format
| mpreview index="metrics_idx"

# execute the query to show the SQL Statements
| mstats 
rate_sum(sql_select_count) as select, 
rate_sum(sql_insert_count) as insert, 
rate_sum(sql_update_count) as update, 
rate_sum(sql_delete_count) as delete
where index="metrics_idx" span=10s
```

If Splunk shows data, the pipeline is working correctly and you can load the dashboards.

1. Click on **Dashboards**, then on **Create New Dashboard**.
2. In the pop-up window
    1. In **Dashboard Title**, set "CockroachDB Overview"
    2. Select **Classic Dashboard**.
    3. Click **Create**.
3. The Dashboard is now in edit mode. Click on **Source**.
4. Replace the current content with the XML in the `overview.xml` file in this directory.
5. Click **Save**.
6. Repeat for every Dashboard file in this directory.
