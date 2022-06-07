// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:generate mockgen -package=prometheus -destination=mocks_generated_test.go . Cluster

package prometheus

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v2"
)

// Client is an interface allowing queries against Prometheus.
type Client interface {
	Query(ctx context.Context, query string, ts time.Time) (model.Value, promv1.Warnings, error)
	QueryRange(ctx context.Context, query string, r promv1.Range) (model.Value, promv1.Warnings, error)
}

// ScrapeNode are nodes to scrape from.
type ScrapeNode struct {
	Nodes option.NodeListOption
	Port  int
}

// ScrapeConfig represents a single instance of scraping.
type ScrapeConfig struct {
	JobName     string
	MetricsPath string
	ScrapeNodes []ScrapeNode
}

// Config is a monitor that watches over the running of prometheus.
type Config struct {
	PrometheusNode option.NodeListOption
	ScrapeConfigs  []ScrapeConfig
	NodeExporter   option.NodeListOption
	Grafana        GrafanaConfig
}

// GrafanaConfig are options related to setting up a Grafana instance.
type GrafanaConfig struct {
	Enabled bool
	// DashboardURLs are URLs (must be accessible by prometheus node, e.g. gists)
	// to provision into Grafana. Failure to download them will be ignored.
	// Datasource UID for these dashboards should be "localprom" or they won't
	// load properly.
	//
	// NB: when using gists, https://gist.github.com/[gist_user]/[gist_id]/raw/
	// provides a link that always references the most up to date version.
	DashboardURLs []string
}

// WithWorkload sets up scraping for `workload` processes running on the given
// node(s) and port. Chains for convenience.
func (cfg *Config) WithWorkload(nodes option.NodeListOption, port int) *Config {
	sn := ScrapeNode{Nodes: nodes, Port: port}
	for i := range cfg.ScrapeConfigs {
		sc := &cfg.ScrapeConfigs[i]
		if sc.JobName == "workload" {
			sc.ScrapeNodes = append(sc.ScrapeNodes, sn)
			return cfg
		}
	}
	cfg.ScrapeConfigs = append(cfg.ScrapeConfigs, MakeWorkloadScrapeConfig("workload", []ScrapeNode{sn}))
	return cfg
}

// WithPrometheusNode specifies the node to set up prometheus on.
func (cfg *Config) WithPrometheusNode(node option.NodeListOption) *Config {
	cfg.PrometheusNode = node
	return cfg
}

// WithCluster adds scraping for a CockroachDB cluster running on the given nodes.
// Chains for convenience.
func (cfg *Config) WithCluster(nodes option.NodeListOption) *Config {
	cfg.ScrapeConfigs = append(cfg.ScrapeConfigs, MakeInsecureCockroachScrapeConfig(
		"cockroach", nodes))
	return cfg
}

// WithGrafanaDashboard adds links to dashboards to provision into Grafana. See
// cfg.Grafana.DashboardURLs for helpful tips.
// Enables Grafana if not already enabled.
// Chains for convenience.
func (cfg *Config) WithGrafanaDashboard(url string) *Config {
	cfg.Grafana.Enabled = true
	cfg.Grafana.DashboardURLs = append(cfg.Grafana.DashboardURLs, url)
	return cfg
}

// WithNodeExporter causes node_exporter to be set up on the specified machines.
// Chains for convenience.
func (cfg *Config) WithNodeExporter(nodes option.NodeListOption) *Config {
	cfg.NodeExporter = cfg.NodeExporter.Merge(nodes)
	return cfg
}

// Cluster is a subset of roachtest.Cluster.
// It is abstracted to prevent a circular dependency on roachtest, as Cluster
// requires the test interface.
type Cluster interface {
	ExternalIP(context.Context, *logger.Logger, option.NodeListOption) ([]string, error)
	Get(ctx context.Context, l *logger.Logger, src, dest string, opts ...option.Option) error
	RunE(ctx context.Context, node option.NodeListOption, args ...string) error
	PutString(
		ctx context.Context, content, dest string, mode os.FileMode, opts ...option.Option,
	) error
}

// Prometheus contains metadata of a running instance of prometheus.
type Prometheus struct {
	Config
}

// Init creates a prometheus instance on the given cluster.
func Init(
	ctx context.Context,
	cfg Config,
	c Cluster,
	l *logger.Logger,
	repeatFunc func(context.Context, option.NodeListOption, string, ...string) error,
) (_ *Prometheus, saveSnap func(artifactsDir string), _ error) {
	if len(cfg.NodeExporter) > 0 {
		if err := repeatFunc(ctx, cfg.NodeExporter, "download node exporter",
			`
(sudo systemctl stop node_exporter || true) &&
rm -rf node_exporter && mkdir -p node_exporter && curl -fsSL \
  https://github.com/prometheus/node_exporter/releases/download/v1.3.1/node_exporter-1.3.1.linux-amd64.tar.gz |
  tar zxv --strip-components 1 -C node_exporter
`); err != nil {
			return nil, nil, err
		}

		// Start node_exporter.
		if err := c.RunE(ctx, cfg.NodeExporter, `cd node_exporter &&
sudo systemd-run --unit node_exporter --same-dir ./node_exporter`,
		); err != nil {
			return nil, nil, err
		}
		cfg.ScrapeConfigs = append(cfg.ScrapeConfigs, ScrapeConfig{
			JobName:     "node_exporter",
			MetricsPath: "/metrics",
			ScrapeNodes: []ScrapeNode{{Nodes: cfg.NodeExporter, Port: 9100}},
		})
	}

	if err := repeatFunc(
		ctx,
		cfg.PrometheusNode,
		"reset prometheus",
		"sudo systemctl stop prometheus || echo 'no prometheus is running'",
	); err != nil {
		return nil, nil, err
	}

	if err := repeatFunc(
		ctx,
		cfg.PrometheusNode,
		"download prometheus",
		`sudo rm -rf /tmp/prometheus && mkdir /tmp/prometheus && cd /tmp/prometheus &&
			curl -fsSL https://storage.googleapis.com/cockroach-fixtures/prometheus/prometheus-2.27.1.linux-amd64.tar.gz | tar zxv --strip-components=1`,
	); err != nil {
		return nil, nil, err
	}

	yamlCfg, err := makeYAMLConfig(
		ctx,
		l,
		c,
		cfg.ScrapeConfigs,
	)
	if err != nil {
		return nil, nil, err
	}

	if err := c.PutString(
		ctx,
		yamlCfg,
		"/tmp/prometheus/prometheus.yml",
		0644,
		cfg.PrometheusNode,
	); err != nil {
		return nil, nil, err
	}

	// Start prometheus as systemd.
	if err := c.RunE(
		ctx,
		cfg.PrometheusNode,
		`cd /tmp/prometheus &&
sudo systemd-run --unit prometheus --same-dir \
	./prometheus --config.file=prometheus.yml --storage.tsdb.path=data/ --web.enable-admin-api`,
	); err != nil {
		return nil, nil, err
	}

	if cfg.Grafana.Enabled {
		// Install Grafana.
		if err := repeatFunc(ctx, cfg.PrometheusNode, "install grafana",
			`sudo apt-get install -qqy apt-transport-https &&
sudo apt-get install -qqy software-properties-common wget &&
wget -q -O - https://packages.grafana.com/gpg.key | sudo apt-key add - &&
echo "deb https://packages.grafana.com/enterprise/deb stable main" | sudo tee -a /etc/apt/sources.list.d/grafana.list &&
sudo apt-get update -qqy && sudo apt-get install -qqy grafana-enterprise && sudo mkdir -p /var/lib/grafana/dashboards`,
		); err != nil {
			return nil, nil, err
		}

		// Provision local prometheus instance as data source.
		if err := repeatFunc(ctx, cfg.PrometheusNode, "permissions",
			`sudo chmod 777 /etc/grafana/provisioning/datasources /etc/grafana/provisioning/dashboards /var/lib/grafana/dashboards`,
		); err != nil {
			return nil, nil, err
		}
		if err := c.PutString(ctx, `apiVersion: 1

datasources:
  - name: prometheusdata
    type: prometheus
    uid: localprom
    url: http://localhost:9090
`, "/etc/grafana/provisioning/datasources/prometheus.yaml", 0644, cfg.PrometheusNode); err != nil {
			return nil, nil, err
		}

		if err := c.PutString(ctx, `apiVersion: 1

providers:
 - name: 'default'
   orgId: 1
   folder: ''
   folderUid: ''
   type: file
   options:
     path: /var/lib/grafana/dashboards
`, "/etc/grafana/provisioning/dashboards/cockroach.yaml", 0644, cfg.PrometheusNode); err != nil {
			return nil, nil, err
		}

		for idx, u := range cfg.Grafana.DashboardURLs {
			if err := c.RunE(ctx, cfg.PrometheusNode,
				"curl", "-fsSL", u, "-o", fmt.Sprintf("/var/lib/grafana/dashboards/%d.json", idx),
			); err != nil {
				l.PrintfCtx(ctx, "failed to download dashboard from %s: %s", u, err)
			}
		}

		// Start Grafana. Default port is 3000.
		if err := c.RunE(ctx, cfg.PrometheusNode, `sudo systemctl restart grafana-server`); err != nil {
			return nil, nil, err
		}
	}

	p := &Prometheus{Config: cfg}
	return p, func(destDir string) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := p.Snapshot(ctx, c, l, destDir); err != nil {
			l.Printf("failed to get prometheus snapshot: %v", err)
		}
	}, nil
}

// Snapshot takes a snapshot of prometheus and stores the snapshot and a script to spin up
// a docker instance for it to the given directory.
func (pm *Prometheus) Snapshot(ctx context.Context, c Cluster, l *logger.Logger, dir string) error {
	if err := c.RunE(
		ctx,
		pm.PrometheusNode,
		`curl -XPOST http://localhost:9090/api/v1/admin/tsdb/snapshot &&
	cd /tmp/prometheus && tar cvf prometheus-snapshot.tar.gz data/snapshots`,
	); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "prometheus-docker-run.sh"), []byte(`#!/bin/sh
set -eu

tar xf prometheus-snapshot.tar.gz
snapdir=$(find data/snapshots -mindepth 1 -maxdepth 1 -type d)
promyml=$(mktemp)
chmod -R o+rw "${snapdir}" "${promyml}"

cat <<EOF > "${promyml}"
global:
  scrape_interval: 10s
  scrape_timeout: 5s
EOF

set -x
# Worked as of v2.33.1 so you can hard-code that if necessary.
docker run --privileged -p 9090:9090 \
    -v "${promyml}:/etc/prometheus/prometheus.yml" -v "${PWD}/${snapdir}:/prometheus" \
    prom/prometheus:latest \
    --config.file=/etc/prometheus/prometheus.yml \
    --storage.tsdb.path=/prometheus \
    --web.enable-admin-api
`), 0755); err != nil {
		return err
	}

	return c.Get(
		ctx,
		l,
		"/tmp/prometheus/prometheus-snapshot.tar.gz",
		dir,
		pm.PrometheusNode,
	)
}

const (
	// DefaultScrapeInterval is the default interval between prometheus
	// scrapes.
	DefaultScrapeInterval = 10 * time.Second
	// DefaultScrapeTimeout is the maximum amount of time before a scrape
	// is timed out.
	DefaultScrapeTimeout = 5 * time.Second
)

// makeYAMLConfig creates a prometheus YAML config for the server to use.
func makeYAMLConfig(
	ctx context.Context, l *logger.Logger, c Cluster, scrapeConfigs []ScrapeConfig,
) (string, error) {
	type yamlStaticConfig struct {
		Targets []string
	}

	type yamlScrapeConfig struct {
		JobName       string             `yaml:"job_name"`
		StaticConfigs []yamlStaticConfig `yaml:"static_configs"`
		MetricsPath   string             `yaml:"metrics_path"`
	}

	type yamlConfig struct {
		Global struct {
			ScrapeInterval string `yaml:"scrape_interval"`
			ScrapeTimeout  string `yaml:"scrape_timeout"`
		}
		ScrapeConfigs []yamlScrapeConfig `yaml:"scrape_configs"`
	}

	cfg := yamlConfig{}
	cfg.Global.ScrapeInterval = DefaultScrapeInterval.String()
	cfg.Global.ScrapeTimeout = DefaultScrapeTimeout.String()

	for _, scrapeConfig := range scrapeConfigs {
		var targets []string
		for _, scrapeNode := range scrapeConfig.ScrapeNodes {
			ips, err := c.ExternalIP(ctx, l, scrapeNode.Nodes)
			if err != nil {
				return "", err
			}
			for _, ip := range ips {
				targets = append(targets, fmt.Sprintf("%s:%d", ip, scrapeNode.Port))
			}
		}

		cfg.ScrapeConfigs = append(
			cfg.ScrapeConfigs,
			yamlScrapeConfig{
				JobName:     scrapeConfig.JobName,
				MetricsPath: scrapeConfig.MetricsPath,
				StaticConfigs: []yamlStaticConfig{
					{
						Targets: targets,
					},
				},
			},
		)
	}

	ret, err := yaml.Marshal(&cfg)
	return string(ret), err
}

// MakeWorkloadScrapeConfig creates a scrape config for a workload.
func MakeWorkloadScrapeConfig(jobName string, scrapeNodes []ScrapeNode) ScrapeConfig {
	return ScrapeConfig{
		JobName:     jobName,
		MetricsPath: "/",
		ScrapeNodes: scrapeNodes,
	}
}

// MakeInsecureCockroachScrapeConfig creates scrape configs for the given
// cockroach nodes. All nodes are assumed to be insecure and running on
// port 26258.
func MakeInsecureCockroachScrapeConfig(jobName string, nodes option.NodeListOption) ScrapeConfig {
	return ScrapeConfig{
		JobName:     jobName,
		MetricsPath: "/_status/vars",
		ScrapeNodes: []ScrapeNode{
			{
				Nodes: nodes,
				Port:  26258,
			},
		},
	}
}
