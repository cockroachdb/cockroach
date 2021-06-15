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
	"strings"
)

type prometheus struct {
	c              *cluster
	prometheusNode nodeListOption
	nodes          nodeListOption
}

// initPrometheus installs and runs prometheus on prometheusNode, which scrapes
// prometheus metrics from the given nodes.
// The prometheus instance should be killed with the cleanup method.
func initPrometheus(
	ctx context.Context, t *test, c *cluster, prometheusNode nodeListOption, nodes nodeListOption,
) *prometheus {
	p := &prometheus{
		c:              c,
		prometheusNode: prometheusNode,
		nodes:          nodes,
	}

	ips, err := c.ExternalIP(ctx, nodes)
	if err != nil {
		t.Fatal(err)
	}
	for i, ip := range ips {
		ips[i] = fmt.Sprintf("'%s:2112'", ip)
	}
	cfg := fmt.Sprintf(
		`
global:
  scrape_interval: 10s
  scrape_timeout: 5s

scrape_configs:
  - job_name: 'workload'
    static_configs:
    - targets: [%s]
    metrics_path: '/'
`,
		strings.Join(ips, ","),
	)

	if err := repeatRunE(
		ctx,
		c,
		prometheusNode,
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

	go func() {
		// TODO: use stopper to cleanup properly.
		if err := p.c.RunE(
			ctx,
			p.prometheusNode,
			`cd /tmp/prometheus &&
			./prometheus --config.file=prometheus.yml --storage.tsdb.path=data/ --web.enable-admin-api`,
		); err != nil {
			shout(ctx, c.l, os.Stderr, "failed to initialize prometheus: %v", err)
		}
	}()

	return p
}

// cleanup kills the prometheus instance using SIGINT, which exits
// the prometheus thread cleanly.
func (p *prometheus) cleanup(ctx context.Context) {
	if err := p.c.RunE(
		ctx,
		p.prometheusNode,
		`pgrep prometheus | xargs kill -2`,
	); err != nil {
		shout(ctx, p.c.l, os.Stderr, "failed to kill prometheus: %v", err)
	}
}

// snapshot takes a snapshot of prometheus data from the prometheus
// node and store a compressed version in the cluster artifacts dir
// as the given filename.
func (p *prometheus) snapshot(ctx context.Context, filename string) {
	if err := p.c.RunE(
		ctx,
		p.prometheusNode,
		`curl -XPOST http://localhost:9090/api/v1/admin/tsdb/snapshot &&
	cd /tmp/prometheus && tar cvf prometheus-snapshot.tar.gz data/snapshots`,
	); err != nil {
		shout(ctx, p.c.l, os.Stderr, "failed to make prometheus snapshot: %v", err)
		return
	}

	if err := p.c.Get(
		ctx,
		p.c.l,
		"/tmp/prometheus/prometheus-snapshot.tar.gz",
		filepath.Join(p.c.t.ArtifactsDir(), filename),
		p.prometheusNode,
	); err != nil {
		shout(ctx, p.c.l, os.Stderr, "failed to get prometheus snapshot: %v", err)
		return
	}
}
