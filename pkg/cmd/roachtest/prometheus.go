// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"gopkg.in/yaml.v2"
)

// prometheusScrapeNode are nodes to scrape from.
type prometheusScrapeNode struct {
	node option.NodeListOption
	port int
}

// prometheusConfig is a monitor that watches over the running of prometheus.
type prometheusConfig struct {
	prometheusNode option.NodeListOption
	jobName        string
	scrapeNodes    []prometheusScrapeNode
}

// run installs and runs prometheus.
// Prometheus will run in the background and log an error if it fails, as opposed
// to failing the test.
func (pm *prometheusConfig) run(ctx context.Context, t *test, c Cluster) {
	cfg, err := makePrometheusConfig(
		ctx,
		c,
		pm.jobName,
		pm.scrapeNodes,
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := repeatRunE(
		ctx,
		t,
		c,
		pm.prometheusNode,
		"install prometheus",
		fmt.Sprintf(
			`rm -rf /tmp/prometheus && mkdir /tmp/prometheus && cd /tmp/prometheus &&
			wget -O prometheus.tar.gz https://github.com/prometheus/prometheus/releases/download/v2.27.1/prometheus-2.27.1.linux-amd64.tar.gz &&
			tar xvf prometheus.tar.gz --strip-components=1 &&
			echo "%s" > prometheus.yml
		`,
			cfg,
		),
	); err != nil {
		t.Fatal(err)
	}

	// Start prometheus in an async thread.
	go func() {
		if err := c.RunE(
			ctx,
			pm.prometheusNode,
			`cd /tmp/prometheus &&
			./prometheus --config.file=prometheus.yml --storage.tsdb.path=data/ --web.enable-admin-api`,
		); err != nil {
			shout(ctx, t.l, os.Stderr, "erroring during running prometheus: %v", err)
		}
	}()
}

// cleanup attempts to download a snapshot of prometheus from the server
// before shutting down the prometheus node.
func (pm *prometheusConfig) cleanup(ctx context.Context, t *test, c Cluster) {
	if err := c.RunE(
		ctx,
		pm.prometheusNode,
		`curl -XPOST http://localhost:9090/api/v1/admin/tsdb/snapshot &&
	cd /tmp/prometheus && tar cvf prometheus-snapshot.tar.gz data/snapshots`,
	); err != nil {
		shout(ctx, t.l, os.Stderr, "failed to make prometheus snapshot: %v", err)
		return
	}

	if err := c.Get(
		ctx,
		t.l,
		"/tmp/prometheus/prometheus-snapshot.tar.gz",
		filepath.Join(t.ArtifactsDir(), "prometheus-snapshot.tar.gz"),
		pm.prometheusNode,
	); err != nil {
		shout(ctx, t.l, os.Stderr, "failed to get prometheus snapshot: %v", err)
		return
	}

	if err := c.RunE(
		ctx,
		pm.prometheusNode,
		`pgrep prometheus | xargs kill -2`,
	); err != nil {
		shout(ctx, t.l, os.Stderr, "failed to kill prometheus: %v", err)
	}
}

// makePrometheusConfig creates a prometheus YAML config for the server to use.
func makePrometheusConfig(
	ctx context.Context, c Cluster, jobName string, scrapeNodes []prometheusScrapeNode,
) (string, error) {
	type prometheusStaticConfig struct {
		Targets []string
	}

	type prometheusScrapeConfig struct {
		JobName       string                   `yaml:"job_name"`
		StaticConfigs []prometheusStaticConfig `yaml:"static_configs"`
		MetricsPath   string                   `yaml:"metrics_path"`
	}

	type prometheusYAMLConfig struct {
		Global struct {
			ScrapeInterval string `yaml:"scrape_interval"`
			ScrapeTimeout  string `yaml:"scrape_timeout"`
		}
		ScrapeConfigs []prometheusScrapeConfig `yaml:"scrape_configs"`
	}

	cfg := prometheusYAMLConfig{}
	cfg.Global.ScrapeInterval = "10s"
	cfg.Global.ScrapeTimeout = "5s"

	var targets []string
	for _, scrapeNode := range scrapeNodes {
		ips, err := c.ExternalIP(ctx, scrapeNode.node)
		if err != nil {
			return "", err
		}
		for _, ip := range ips {
			targets = append(targets, fmt.Sprintf("%s:%d", ip, scrapeNode.port))
		}
	}

	cfg.ScrapeConfigs = append(
		cfg.ScrapeConfigs,
		prometheusScrapeConfig{
			JobName:     jobName,
			MetricsPath: "/",
			StaticConfigs: []prometheusStaticConfig{
				{
					Targets: targets,
				},
			},
		},
	)

	ret, err := yaml.Marshal(&cfg)
	return string(ret), err
}
