#!/usr/bin/env bash

# The prometheus config is generated based on these constants
PROJECT="cockroach-drt"
WORKLOAD_PORT_RANGE=(2112 2120)
REGIONS=("us-east1-b" "us-west1-b" "europe-west2-b")

# Templates below are defined as `prom_` and support replacing {project} and {region}
# Workload templates also replace {port} for the range supplied above.

prom_header=$(cat <<'EOF'
global:
  scrape_interval:     "15s"
  evaluation_interval: "30s"
  # scrape_timeout is set to the global default (10s).

  external_labels:
    monitor: "prom-gce"

rule_files:
- "/opt/prom/prometheus/prometheus-alert.rules"

alerting:
  alertmanagers:
  - static_configs:
    - targets:
      - "127.0.0.1:9093"

scrape_configs:
- job_name: 'prometheus'
  metrics_path: prometheus/metrics
  scrape_interval: 30s
  static_configs:
    - targets: ['localhost:9090']

# Scrape the Node Exporter every 30 seconds.
- job_name: 'node'
  scrape_interval: 30s
  static_configs:
    - targets: ['localhost:9100']
EOF
)

# System metrics from port 9100, we only replace region here
prom_sys=$(cat <<'EOF'
- job_name: 'sys-{project}-{region}'
  gce_sd_configs:
    - project: {project}
      zone: {region}
      port: 9100
  relabel_configs:
    - source_labels: [__meta_gce_private_ip]
      regex:  '(.*)'
      replacement: '${1}:9100'
      target_label: instance
    - source_labels: [__meta_gce_instance_name]
      regex:  '(.*)'
      replacement: '${1}-sys'
      target_label: job
    - source_labels: [__meta_gce_instance_name]
      regex:  '(.*)'
      replacement: '${1}'
      target_label: host
    - source_labels: [__meta_gce_label_cluster]
      regex:  '(.*)'
      replacement: '${1}'
      target_label: cluster
    - source_labels: [__meta_gce_tags]
      regex:  '(.*)'
      replacement: '${1}'
      target_label: instance_tags
    - source_labels: [__meta_gce_label_test_name]
      regex:  '(.*)'
      replacement: '${1}'
      target_label: test_name
    - source_labels: [__meta_gce_label_test_run_id]
      regex:  '(.*)'
      replacement: '${1}'
      target_label: test_run_id
    - target_label: project
      replacement: {project}
    - target_label: zone
      replacement: {region}
    - source_labels: [zone]
      regex: (^.+[0-9])(-[a-f]$)
      replacement: '${1}'
      target_label: region
EOF
)

# Cockroach metrics are collected from port 26258, we only replace the region here
prom_cockroach=$(cat <<'EOF'
- job_name: 'crdb-{project}-{region}'
  metrics_path: _status/vars
  gce_sd_configs:
    - project: {project}
      zone: {region}
      #filter: 'labels.prom:metrics-26258'
      port: 26258
  relabel_configs:
    - source_labels: [__meta_gce_private_ip]
      regex:  '(.*)'
      replacement: '${1}'
      target_label: host_ip
    - source_labels: [__meta_gce_instance_name]
      regex:  '(.*)'
      replacement: 'cockroachdb'
      target_label: job
    - source_labels: [__meta_gce_instance_name]
      regex:  '(.*)'
      replacement: '${1}'
      target_label: instance
    - source_labels: [__meta_gce_tags]
      regex:  '(.*)'
      replacement: '${1}'
      # network tags
      target_label: instance_tags
    - source_labels: [__meta_gce_label_cluster]
      regex:  '(.*)'
      replacement: '${1}'
      target_label: cluster
    - source_labels: [__meta_gce_label_test_name]
      regex:  '(.*)'
      replacement: '${1}'
      target_label: test_name
    - source_labels: [__meta_gce_label_test_run_id]
      regex:  '(.*)'
      replacement: '${1}'
      target_label: test_run_id
    - target_label: project
      replacement: {project}
    - target_label: zone
      replacement: {region}
    - source_labels: [zone]
      regex: (^.+[0-9])(-[a-f]$)
      replacement: '${1}'
      target_label: region
EOF
)

prom_cockroach_secure=$(cat <<'EOF'
- job_name: 'crdb-secure-{project}-{region}'
  scheme: https
  metrics_path: _status/vars
  tls_config:
      # N.B. certs are self-signed
      insecure_skip_verify: true
  gce_sd_configs:
    - project: {project}
      zone: {region}
      #filter: 'labels.prom:metrics-26258'
      port: 26258
  relabel_configs:
    - source_labels: [__meta_gce_private_ip]
      regex:  '(.*)'
      replacement: '${1}'
      target_label: host_ip
    - source_labels: [__meta_gce_instance_name]
      regex:  '(.*)'
      replacement: 'cockroachdb'
      target_label: job
    - source_labels: [__meta_gce_instance_name]
      regex:  '(.*)'
      replacement: '${1}'
      target_label: instance
    - source_labels: [__meta_gce_tags]
      regex:  '(.*)'
      replacement: '${1}'
      # network tags
      target_label: instance_tags
    - source_labels: [__meta_gce_label_cluster]
      regex:  '(.*)'
      replacement: '${1}'
      target_label: cluster
    - source_labels: [__meta_gce_label_test_name]
      regex:  '(.*)'
      replacement: '${1}'
      target_label: test_name
    - source_labels: [__meta_gce_label_test_run_id]
      regex:  '(.*)'
      replacement: '${1}'
      target_label: test_run_id
    - target_label: project
      replacement: {project}
    - target_label: zone
      replacement: {region}
    - source_labels: [zone]
      regex: (^.+[0-9])(-[a-f]$)
      replacement: '${1}'
      target_label: region
EOF
)
prom_workload=$(cat <<'EOF'
- job_name: 'workload-{port}-{project}-{region}'
  gce_sd_configs:
    - project: {project}
      zone: {region}
      port: {port}
  relabel_configs:
    - source_labels: [__meta_gce_private_ip]
      regex:  '(.*)'
      replacement: '${1}'
      target_label: host_ip
    - source_labels: [__meta_gce_instance_name]
      regex:  '(.*)'
      replacement: 'workload-{port}'
      target_label: job
    - source_labels: [__meta_gce_instance_name]
      regex:  '(.*)'
      replacement: '${1}'
      target_label: instance
    - source_labels: [__meta_gce_tags]
      regex:  '(.*)'
      replacement: '${1}'
      # network tags
      target_label: instance_tags
    - source_labels: [__meta_gce_label_cluster]
      regex:  '(.*)'
      replacement: '${1}'
      target_label: cluster
    - source_labels: [__meta_gce_label_test_name]
      regex:  '(.*)'
      replacement: '${1}'
      target_label: test_name
    - source_labels: [__meta_gce_label_test_run_id]
      regex:  '(.*)'
      replacement: '${1}'
      target_label: test_run_id
    - target_label: project
      replacement: {project}
    - target_label: zone
      replacement: {region}
    - source_labels: [zone]
      regex: (^.+[0-9])(-[a-f]$)
      replacement: '${1}'
      target_label: region
EOF
)

# Replace project, region and port
replace_and_write() {
    text=$1
    region=$2
    port=$3
    m1="${text//\{region\}/$region}"
    m2="${m1//\{port\}/$port}"
    m3="${m2//\{project\}/$PROJECT}"
    echo "$m3"
    echo -e "\n"
}

# Generate header
echo "# This config was generated by $0 on" "$(date +"%Y-%m-%d %T")"
replace_and_write "$prom_header"

# Generate cockroach, cockroach-secure & system
for region in "${REGIONS[@]}"; do
    replace_and_write "$prom_sys" "$region"
    replace_and_write "$prom_cockroach" "$region"
    replace_and_write "$prom_cockroach_secure" "$region"
done

# Generate workloads
for region in "${REGIONS[@]}"; do
    for (( port=WORKLOAD_PORT_RANGE[0]; port<=WORKLOAD_PORT_RANGE[1]; port++ )); do
        replace_and_write "$prom_workload" "$region" "$port"
    done
done
