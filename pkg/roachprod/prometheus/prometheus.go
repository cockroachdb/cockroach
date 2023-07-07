// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package prometheus

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v2"
)

const defaultWorkloadPort = 2112

// Client is an interface allowing queries against Prometheus.
type Client interface {
	Query(ctx context.Context, query string, ts time.Time, opts ...promv1.Option) (model.Value, promv1.Warnings, error)
	QueryRange(ctx context.Context, query string, r promv1.Range, opts ...promv1.Option) (model.Value, promv1.Warnings, error)
}

// ScrapeNode is a node to scrape from.
type ScrapeNode struct {
	Node install.Node
	Port int
}

// ScrapeConfig represents a single instance of scraping, identified by the jobName
// Note how workload scrapes are set up differently than CRDB binary scrapes.
type ScrapeConfig struct {
	JobName     string
	MetricsPath string
	ScrapeNodes []ScrapeNode
	Labels      map[string]string // additional static labels to add
}

// Config is a monitor that watches over the running of prometheus.
type Config struct {
	// PrometheusNode identifies a single node in the cluster to run the prometheus instance on.
	// The type is install.Nodes merely for ease of use.
	PrometheusNode install.Nodes

	// ScrapeConfigs provides the configurations for each scraping instance
	ScrapeConfigs []ScrapeConfig

	// NodeExporter identifies each node in the cluster to scrape with the node exporter process
	NodeExporter install.Nodes

	// Grafana provides the info to set up grafana
	Grafana GrafanaConfig
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

	// DashboardJSON are strings containing the JSON for dashboards to
	// provision in addition to any other sources.
	DashboardJSON []string
}

// WithWorkload sets up a scraping config for a single `workload` running on the
// given node and port. If the workload is in the config, the node and port will be
// added to the workload's scrape config (i.e. allows for chaining). If port == 0,
// defaultWorkloadPort is used.
func (cfg *Config) WithWorkload(workloadName string, nodes install.Node, port int) *Config {

	// Find the workload's scrapeConfig, if it exists.
	var sc *ScrapeConfig
	for i := range cfg.ScrapeConfigs {
		existing := &cfg.ScrapeConfigs[i]
		// A workload scrape config name is unique.
		if existing.JobName == workloadName {
			sc = existing
			break
		}
	}
	if port == 0 {
		port = defaultWorkloadPort
	}
	sn := ScrapeNode{Node: nodes, Port: port}
	if sc == nil {
		cfg.ScrapeConfigs = append(cfg.ScrapeConfigs, MakeWorkloadScrapeConfig(workloadName, "/", []ScrapeNode{sn}))
	} else {
		sc.ScrapeNodes = append(sc.ScrapeNodes, sn)
	}
	return cfg
}

// MakeWorkloadScrapeConfig creates a scrape config for a workload.
func MakeWorkloadScrapeConfig(
	jobName string, metricsPath string, scrapeNodes []ScrapeNode,
) ScrapeConfig {
	return ScrapeConfig{
		JobName:     jobName,
		MetricsPath: metricsPath,
		ScrapeNodes: scrapeNodes,
	}
}

// WithPrometheusNode specifies the node to set up prometheus on.
func (cfg *Config) WithPrometheusNode(node install.Node) *Config {
	cfg.PrometheusNode = install.Nodes{node}
	return cfg
}

// WithCluster adds scraping for a CockroachDB cluster running on the given nodes.
// Chains for convenience.
func (cfg *Config) WithCluster(nodes install.Nodes) *Config {
	cfg.ScrapeConfigs = append(cfg.ScrapeConfigs, MakeInsecureCockroachScrapeConfig(nodes)...)
	return cfg
}

// WithTenantPod adds scraping for a tenant SQL pod running on the given nodes.
// Chains for convenience.
func (cfg *Config) WithTenantPod(node install.Node, tenantID int) *Config {
	cfg.ScrapeConfigs = append(cfg.ScrapeConfigs, MakeInsecureTenantPodScrapeConfig(node, tenantID)...)
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

// WithGrafanaDashboardJSON adds a string containing the JSON for a dashboard
// to provision into Grafana.
// Enables Grafana if not already enabled.
// Chains for convenience.
func (cfg *Config) WithGrafanaDashboardJSON(str string) *Config {
	cfg.Grafana.Enabled = true
	cfg.Grafana.DashboardJSON = append(cfg.Grafana.DashboardJSON, str)
	return cfg
}

// WithScrapeConfigs adds scraping configs to the prometheus instance. Chains
// for convenience.
func (cfg *Config) WithScrapeConfigs(config ...ScrapeConfig) *Config {
	cfg.ScrapeConfigs = append(cfg.ScrapeConfigs, config...)
	return cfg
}

// WithNodeExporter causes node_exporter to be set up on the specified machines,
// a separate process that sends hardware metrics to prometheus.
// For more on the node exporter process, see https://prometheus.io/docs/guides/node-exporter/
func (cfg *Config) WithNodeExporter(nodes install.Nodes) *Config {
	cfg.NodeExporter = nodes
	// Add a scrape config for each node running node_exporter
	for _, node := range cfg.NodeExporter {
		s := strconv.Itoa(int(node))
		cfg.ScrapeConfigs = append(cfg.ScrapeConfigs, ScrapeConfig{
			JobName:     "node_exporter-" + s,
			MetricsPath: "/metrics",
			Labels:      map[string]string{"node": s},
			ScrapeNodes: []ScrapeNode{{
				Node: node,
				Port: 9100}},
		})
	}
	return cfg
}

// MakeInsecureCockroachScrapeConfig creates a scrape config for each
// cockroach node. All nodes are assumed to be insecure and running on
// port 26258.
func MakeInsecureCockroachScrapeConfig(nodes install.Nodes) []ScrapeConfig {
	var sl []ScrapeConfig
	for _, node := range nodes {
		s := strconv.Itoa(int(node))
		sl = append(sl, ScrapeConfig{
			JobName:     "cockroach-n" + s,
			MetricsPath: "/_status/vars",
			Labels: map[string]string{
				"node":   s,
				"tenant": "system", // all CRDB nodes emit SQL metrics for the system tenant since it's embedded
			},
			ScrapeNodes: []ScrapeNode{
				{
					Node: node,
					Port: 26258,
				},
			},
		})
	}
	return sl
}

// MakeInsecureTenantPodScrapeConfig creates a scrape config for a given tenant
// SQL pod. All nodes are assumed to be insecure and running on port 8081.
func MakeInsecureTenantPodScrapeConfig(node install.Node, tenantID int) []ScrapeConfig {
	var sl []ScrapeConfig
	sl = append(sl, ScrapeConfig{
		JobName:     fmt.Sprintf("cockroach-tenant-t%d-n%d", tenantID, int(node)),
		MetricsPath: "/_status/vars",
		Labels: map[string]string{
			"node":   strconv.Itoa(int(node)),
			"tenant": strconv.Itoa(tenantID),
		},
		ScrapeNodes: []ScrapeNode{
			{
				Node: node,
				Port: 8081,
			},
		},
	})
	return sl
}

// Prometheus contains metadata of a running instance of prometheus.
type Prometheus struct {
	Config
}

// Init creates a prometheus instance on the given cluster.
func Init(
	ctx context.Context, l *logger.Logger, c *install.SyncedCluster, arch vm.CPUArch, cfg Config,
) (_ *Prometheus, _ error) {
	binArch := "amd64"
	if arch == vm.ArchARM64 {
		binArch = "arm64"
	}

	if len(cfg.NodeExporter) > 0 {
		// NB: when upgrading here, make sure to target a version that picks up this PR:
		// https://github.com/prometheus/node_exporter/pull/2311
		// At time of writing, there hasn't been a release in over half a year.
		if err := c.RepeatRun(ctx, l, l.Stdout, l.Stderr, cfg.NodeExporter,
			"download node exporter",
			fmt.Sprintf(`
(sudo systemctl stop node_exporter || true) &&
rm -rf node_exporter && mkdir -p node_exporter && curl -fsSL \
  https://storage.googleapis.com/cockroach-fixtures/prometheus/node_exporter-1.2.2.linux-%s.tar.gz |
  tar zxv --strip-components 1 -C node_exporter
`, binArch)); err != nil {
			return nil, err
		}

		// Start node_exporter.
		if err := c.Run(ctx, l, l.Stdout, l.Stderr, cfg.NodeExporter, "init node exporter",
			`cd node_exporter &&
sudo systemd-run --unit node_exporter --same-dir ./node_exporter`,
		); err != nil {
			// TODO(msbutler): download binary for target platform. currently we
			// hardcode downloading the linux binary.
			return nil, errors.Wrap(err, "grafana-start currently cannot run on darwin")
		}
	}
	if err := c.RepeatRun(
		ctx,
		l,
		l.Stdout,
		l.Stderr,
		cfg.PrometheusNode,
		"reset prometheus",
		"sudo systemctl stop prometheus || echo 'no prometheus is running'",
	); err != nil {
		return nil, err
	}

	if err := c.RepeatRun(
		ctx,
		l,
		l.Stdout,
		l.Stderr,
		cfg.PrometheusNode,
		"download prometheus",
		fmt.Sprintf(`sudo rm -rf /tmp/prometheus && mkdir /tmp/prometheus && cd /tmp/prometheus &&
			curl -fsSL https://storage.googleapis.com/cockroach-fixtures/prometheus/prometheus-2.27.1.linux-%s.tar.gz | tar zxv --strip-components=1`,
			binArch)); err != nil {
		return nil, err
	}
	// create and upload prom config
	nodeIPs, err := makeNodeIPMap(c)
	if err != nil {
		return nil, err
	}
	yamlCfg, err := makeYAMLConfig(cfg.ScrapeConfigs, nodeIPs)
	if err != nil {
		return nil, err
	}

	if err := c.PutString(
		ctx,
		l,
		cfg.PrometheusNode,
		yamlCfg,
		"/tmp/prometheus/prometheus.yml",
		0777,
	); err != nil {
		return nil, err
	}

	// Start prometheus as systemd.
	if err := c.Run(
		ctx,
		l,
		l.Stdout,
		l.Stderr,
		cfg.PrometheusNode,
		"start-prometheus",
		`cd /tmp/prometheus &&
sudo systemd-run --unit prometheus --same-dir \
	./prometheus --config.file=prometheus.yml --storage.tsdb.path=data/ --web.enable-admin-api`,
	); err != nil {
		return nil, err
	}

	if cfg.Grafana.Enabled {
		// Install Grafana.
		if err := c.RepeatRun(ctx, l,
			l.Stdout,
			l.Stderr, cfg.PrometheusNode, "install grafana",
			fmt.Sprintf(`
sudo apt-get install -qqy apt-transport-https &&
sudo apt-get install -qqy software-properties-common wget &&
sudo apt-get install -y adduser libfontconfig1 &&
wget https://dl.grafana.com/enterprise/release/grafana-enterprise_9.2.3_%s.deb -O grafana-enterprise_9.2.3_%s.deb &&
sudo dpkg -i grafana-enterprise_9.2.3_%s.deb &&
sudo mkdir -p /var/lib/grafana/dashboards`,
				binArch, binArch, binArch)); err != nil {
			return nil, err
		}

		// Provision local prometheus instance as data source.
		if err := c.RepeatRun(ctx, l,
			l.Stdout,
			l.Stderr, cfg.PrometheusNode, "permissions",
			`sudo chmod -R 777 /etc/grafana/provisioning/datasources /etc/grafana/provisioning/dashboards /var/lib/grafana/dashboards /etc/grafana/grafana.ini`,
		); err != nil {
			return nil, err
		}

		// Set up grafana config.
		if err := c.PutString(ctx, l, cfg.PrometheusNode, `apiVersion: 1

datasources:
  - name: prometheusdata
    type: prometheus
    uid: localprom
    url: http://localhost:9090
`, "/etc/grafana/provisioning/datasources/prometheus.yaml", 0777); err != nil {
			return nil, err
		}
		if err := c.PutString(ctx, l, cfg.PrometheusNode, `apiVersion: 1

providers:
 - name: 'default'
   orgId: 1
   folder: ''
   folderUid: ''
   type: file
   allowUiUpdates: true
   options:
     path: /var/lib/grafana/dashboards
`, "/etc/grafana/provisioning/dashboards/cockroach.yaml", 0777); err != nil {
			return nil, err
		}
		if err := c.PutString(ctx, l, cfg.PrometheusNode, `
[auth.anonymous]
enabled = true
org_role = Admin
`,
			"/etc/grafana/grafana.ini", 0777); err != nil {
			return nil, err
		}

		for idx, u := range cfg.Grafana.DashboardURLs {
			cmd := fmt.Sprintf("curl -fsSL %s -o /var/lib/grafana/dashboards/%d.json", u, idx)
			if err := c.Run(ctx, l, l.Stdout, l.Stderr, cfg.PrometheusNode, "download dashboard",
				cmd); err != nil {
				l.PrintfCtx(ctx, "failed to download dashboard from %s: %s", u, err)
			}
		}

		for idx, json := range cfg.Grafana.DashboardJSON {
			if err := c.PutString(ctx, l, cfg.PrometheusNode, json,
				fmt.Sprintf("/var/lib/grafana/dashboards/s-%d.json", idx), 0777); err != nil {
				return nil, err
			}
		}

		// Start Grafana. Default port is 3000.
		if err := c.Run(ctx, l, l.Stdout, l.Stderr, cfg.PrometheusNode, "start grafana",
			`sudo systemctl restart grafana-server`); err != nil {
			return nil, err
		}
	}

	p := &Prometheus{Config: cfg}
	return p, nil
}

// Snapshot takes a snapshot of prometheus and stores the snapshot and a script to spin up
// a docker instance for it to the given directory.
func Snapshot(
	ctx context.Context,
	c *install.SyncedCluster,
	l *logger.Logger,
	promNode install.Nodes,
	dir string,
) error {
	if err := c.Run(
		ctx,
		l,
		l.Stdout,
		l.Stderr,
		promNode,
		"prometheus snapshot",
		`sudo rm -rf /tmp/prometheus/data/snapshots/* && curl -XPOST http://localhost:9090/api/v1/admin/tsdb/snapshot &&
	cd /tmp/prometheus && tar cvf prometheus-snapshot.tar.gz data/snapshots`,
	); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "prometheus-docker-run.sh"), []byte(`#!/bin/sh
set -eu

rm -rf data/snapshots
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
		return errors.Wrap(err, "failed to write docker script")
	}

	return c.Get(
		ctx,
		l,
		promNode,
		"/tmp/prometheus/prometheus-snapshot.tar.gz",
		dir,
	)
}

// Shutdown stops all prom and grafana processes and, if dumpDir is passed,
// will download dump of prometheus data to the machine executing the roachprod binary.
func Shutdown(
	ctx context.Context,
	c *install.SyncedCluster,
	l *logger.Logger,
	nodes install.Nodes,
	dumpDir string,
) error {
	// We currently assume the last node contains the server.
	promNode := install.Nodes{nodes[len(nodes)-1]}

	dumpSnapshot := func(dumpDir string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := Snapshot(ctx, c, l, promNode, dumpDir); err != nil {
			l.Printf("failed to get prometheus snapshot: %v", err)
			return err
		}
		return nil
	}
	var shutdownErr error
	if dumpDir != "" {
		if err := dumpSnapshot(dumpDir); err != nil {
			shutdownErr = errors.CombineErrors(shutdownErr, err)
		}
	}
	if err := c.Run(ctx, l, l.Stdout, l.Stderr, nodes, "stop node exporter",
		`sudo systemctl stop node_exporter || echo 'Stopped node exporter'`); err != nil {
		l.Printf("Failed to stop node exporter: %v", err)
		shutdownErr = errors.CombineErrors(shutdownErr, err)
	}

	if err := c.Run(ctx, l, l.Stdout, l.Stderr, promNode, "stop grafana",
		`sudo systemctl stop grafana-server || echo 'Stopped grafana'`); err != nil {
		l.Printf("Failed to stop grafana server: %v", err)
		shutdownErr = errors.CombineErrors(shutdownErr, err)
	}

	if err := c.RepeatRun(
		ctx,
		l,
		l.Stdout,
		l.Stderr,
		promNode,
		"stop prometheus",
		"sudo systemctl stop prometheus || echo 'Stopped prometheus'",
	); err != nil {
		l.Printf("Failed to stop prometheus server: %v", err)
		shutdownErr = errors.CombineErrors(shutdownErr, err)
	}
	return shutdownErr
}

const (
	// DefaultScrapeInterval is the default interval between prometheus
	// scrapes.
	DefaultScrapeInterval = 10 * time.Second
	// DefaultScrapeTimeout is the maximum amount of time before a scrape
	// is timed out.
	DefaultScrapeTimeout = 5 * time.Second
)

func makeNodeIPMap(c *install.SyncedCluster) (map[install.Node]string, error) {
	nodes, err := install.ListNodes("all", len(c.VMs))
	if err != nil {
		return nil, err
	}
	nodeIP := make(map[install.Node]string)
	for i, n := range nodes {
		nodeIP[n] = c.VMs[nodes[i]-1].PublicIP
	}
	return nodeIP, nil
}

// makeYAMLConfig creates a prometheus YAML config for the server to use.
func makeYAMLConfig(scrapeConfigs []ScrapeConfig, nodeIPs map[install.Node]string) (string, error) {
	type tlsConfig struct {
		InsecureSkipVerify bool `yaml:"insecure_skip_verify"`
	}

	type yamlStaticConfig struct {
		Labels  map[string]string `yaml:",omitempty"`
		Targets []string
	}

	type yamlScrapeConfig struct {
		JobName       string             `yaml:"job_name"`
		StaticConfigs []yamlStaticConfig `yaml:"static_configs"`
		MetricsPath   string             `yaml:"metrics_path"`
		TLSConfig     tlsConfig          `yaml:"tls_config"`
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
			targets = append(targets, fmt.Sprintf("%s:%d", nodeIPs[scrapeNode.Node], scrapeNode.Port))
		}

		cfg.ScrapeConfigs = append(
			cfg.ScrapeConfigs,
			yamlScrapeConfig{
				JobName:     scrapeConfig.JobName,
				MetricsPath: scrapeConfig.MetricsPath,
				StaticConfigs: []yamlStaticConfig{
					{
						Labels:  scrapeConfig.Labels,
						Targets: targets,
					},
				},
				TLSConfig: tlsConfig{
					InsecureSkipVerify: true,
				},
			},
		)
	}
	ret, err := yaml.Marshal(&cfg)
	return string(ret), err
}
