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
	"gopkg.in/yaml.v2"
)

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
) (*Prometheus, error) {
	if err := c.RunE(
		ctx,
		cfg.PrometheusNode,
		"sudo systemctl stop prometheus || echo 'no prometheus is running'",
	); err != nil {
		return nil, err
	}

	if err := repeatFunc(
		ctx,
		cfg.PrometheusNode,
		"download prometheus",
		`rm -rf /tmp/prometheus && mkdir /tmp/prometheus && cd /tmp/prometheus &&
			curl -fsSL https://storage.googleapis.com/cockroach-fixtures/prometheus/prometheus-2.27.1.linux-amd64.tar.gz | tar zxv --strip-components=1`,
	); err != nil {
		return nil, err
	}

	yamlCfg, err := makeYAMLConfig(
		ctx,
		l,
		c,
		cfg.ScrapeConfigs,
	)
	if err != nil {
		return nil, err
	}

	if err := c.PutString(
		ctx,
		yamlCfg,
		"/tmp/prometheus/prometheus.yml",
		0644,
		cfg.PrometheusNode,
	); err != nil {
		return nil, err
	}

	// Start prometheus as systemd.
	if err := c.RunE(
		ctx,
		cfg.PrometheusNode,
		`cd /tmp/prometheus &&
sudo systemd-run --unit prometheus --same-dir \
	./prometheus --config.file=prometheus.yml --storage.tsdb.path=data/ --web.enable-admin-api`,
	); err != nil {
		return nil, err
	}
	return &Prometheus{Config: cfg}, nil
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
