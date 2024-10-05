// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

const (
	// 3 node CRDB cluster, plus 1 node for workload
	numNodesBufferedLogging = 4
	fluentBitTCPPort        = 5170
	// YAML template string defining a FluentBit and HTTP log sink, both with buffering enabled.
	// Container ports for both sinks are left to be interpolated (%d).
	logConfigTemplate = "{ file-defaults: { buffered-writes: false, buffering: { max-staleness: 1s, flush-trigger-size: 256KiB, max-buffer-size: 50MiB } }, http-defaults: { format: json-fluent, buffering: { max-staleness: 5s, flush-trigger-size: 1.0MiB, max-buffer-size: 50MiB } }, fluent-defaults: { format: json-fluent, buffering: { max-staleness: 5s, flush-trigger-size: 1.0MiB, max-buffer-size: 50MiB } }, sinks: { file-groups: { group-a: { channels: [ALL] }, group-b: { channels: [ALL] }, group-c: { channels: [ALL] } }, fluent-servers: { test-output: { channels: {INFO: all}, net: tcp, address: localhost:%d, filter: INFO, redact: false } }, http-servers: { test-output: { channels: {INFO: all}, address: http://localhost:%d, filter: INFO, method: POST, unsafe-tls: true } } } }"
)

func registerBufferedLogging(r registry.Registry) {
	runBufferedLogging := func(
		ctx context.Context,
		t test.Test,
		c cluster.Cluster,
	) {
		// Install Docker, which we'll use for FluentBit.
		t.Status("installing docker")
		if err := c.Install(ctx, t.L(), c.CRDBNodes(), "docker"); err != nil {
			t.Fatalf("failed to install docker: %v", err)
		}

		t.Status("installing FluentBit containers on CRDB nodes")
		// Create FluentBit container on the node with a TCP input and dev/null output.
		err := c.RunE(ctx, option.WithNodes(c.CRDBNodes()), fmt.Sprintf(
			"sudo docker run -d -p %d:%d --name=fluentbit fluent/fluent-bit -i tcp -o null",
			fluentBitTCPPort,
			fluentBitTCPPort))
		if err != nil {
			t.Fatalf("failed to install FluentBit containers: %v", err)
		}

		// Install Cockroach, including on the workload node,
		// since we'll use ./cockroach workload.
		t.Status("installing cockroach")

		// Start each node with a log config containing fluent-server and http-server sinks.
		t.Status("starting cockroach on nodes")
		startOpts := option.DefaultStartOpts()
		logCfg := fmt.Sprintf(logConfigTemplate, fluentBitTCPPort, fluentBitTCPPort)
		startOpts.RoachprodOpts.ExtraArgs = []string{
			"--log", logCfg,
		}
		c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.CRDBNodes())

		// Construct pgurls for the workload runner. As a roundabout way of detecting deadlocks,
		// we set a client timeout on the workload pgclient. If the server becomes unavailable
		// due to a deadlock, the timeout will eventually trigger and cause the test to fail.
		// We've had buffered logging bugs in the past that deadlocked without the nodes dying,
		// so this helps detect such a case.
		secureUrls, err := roachprod.PgURL(ctx,
			t.L(),
			c.MakeNodes(c.CRDBNodes()),
			install.CockroachNodeCertsDir, /* certsDir */
			roachprod.PGURLOptions{
				External: false,
				Secure:   true,
			})
		require.NoError(t, err)
		workloadPGURLs := make([]string, len(secureUrls))
		for i, url := range secureUrls {
			// URLs already are wrapped in '', but we need to add a timeout flag.
			// Trim the trailing ' and re-add with the flag.
			trimmed := strings.TrimSuffix(url, "'")
			// Define a 60s client statement timeout.
			workloadPGURLs[i] = fmt.Sprintf("%s&statement_timeout=60000'", trimmed)
		}

		// Init & run a workload on the workload node.
		t.Status("initializing workload")
		initWorkloadCmd := fmt.Sprintf("./cockroach workload init kv %s ", secureUrls[0])
		c.Run(ctx, option.WithNodes(c.WorkloadNode()), initWorkloadCmd)

		t.Status("running workload")
		m := c.NewMonitor(ctx, c.CRDBNodes())
		m.Go(func(ctx context.Context) error {
			joinedURLs := strings.Join(workloadPGURLs, " ")
			runWorkloadCmd := fmt.Sprintf("./cockroach workload run kv --concurrency=32 --duration=1h %s", joinedURLs)
			return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), runWorkloadCmd)
		})
		m.Wait()
	}

	r.Add(registry.TestSpec{
		Name:             "buffered_logging",
		Owner:            registry.OwnerObservability,
		Cluster:          r.MakeClusterSpec(numNodesBufferedLogging, spec.WorkloadNode()),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runBufferedLogging(ctx, t, c)
		},
	})
}
