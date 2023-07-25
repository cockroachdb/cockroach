package tests

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/stretchr/testify/require"
	"net"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/errors"
)

const (
	// 3 node CRDB cluster, plus 1 node for workload
	numNodesNetworkLogging = 4
	fluentBitTCPPort       = 5170
	// YAML template string defining a FluentBit and HTTP log sink, both with buffering enabled.
	// Addresses for both sinks are left to be interpolated (%s).
	logConfigTemplate = "{ http-defaults: { format: json-fluent, buffering: { max-staleness: 5s, flush-trigger-size: 1.0MiB, max-buffer-size: 50MiB } }, fluent-defaults: { format: json-fluent, buffering: { max-staleness: 5s, flush-trigger-size: 1.0MiB, max-buffer-size: 50MiB } }, sinks: { fluent-servers: { test-output: { channels: {INFO: all}, net: tcp, address: %s, filter: INFO, redact: false } }, http-servers: { test-output: { channels: {INFO: all}, address: http://%s, filter: INFO, method: POST, unsafe-tls: true } } } }"
)

func registerNetworkLogging(r registry.Registry) {
	runNetworkLogging := func(
		ctx context.Context,
		t test.Test,
		c cluster.Cluster,
	) {
		crdbNodes := c.Spec().NodeCount - 1
		workloadNode := c.Spec().NodeCount

		// Install Docker, which we'll use for FluentBit.
		t.Status("installing docker")
		if err := c.Install(ctx, t.L(), c.Range(1, crdbNodes), "docker"); err != nil {
			t.Fatalf("failed to install docker: %v", err)
		}

		// Create FluentBit Docker containers on each node, and get their IP addresses.
		nodeFBAddresses, err := installFluentBit(ctx, t, c)
		if err != nil {
			t.Fatalf("installing fluentbit: %v", err)
		}

		// Install Cockroach, including on the workload node,
		// since we'll use ./cockroach workload.
		t.Status("installing cockroach")
		c.Put(ctx, t.Cockroach(), "./cockroach", c.All())

		// Start each node with a log config containing a fluent-server sink.
		for _, n := range c.Range(1, crdbNodes) {
			t.Status(fmt.Sprintf("starting cockroach on node %d", n))
			node := c.Node(n)
			fbIP, ok := nodeFBAddresses[n]
			if !ok {
				t.Fatalf("no FluentBit IP address for node %v", n)
			}
			startOpts := option.DefaultStartOptsNoBackups()
			logCfg := fmt.Sprintf(logConfigTemplate, fbIP, fbIP)
			startOpts.RoachprodOpts.ExtraArgs = []string{
				"--log", logCfg,
			}
			c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(install.SecureOption(true)), node)
		}

		// Construct pgurls for the workload runner. As a roundabout way of detecting deadlocks,
		// we set a client timeout on the workload pgclient. If the server becomes unavailable
		// due to a deadlock, the timeout will eventually trigger and cause the test to fail.
		// We've had network logging bugs in the past that deadlocked without the nodes dying,
		// so this helps detect such a case.
		secureUrls, err := roachprod.PgURL(ctx,
			t.L(),
			c.MakeNodes(c.Range(1, crdbNodes)),
			"certs", /* certsDir */
			roachprod.PGURLOptions{
				External: false,
				Secure:   true})
		require.NoError(t, err)
		workloadPGURLs := make([]string, len(secureUrls))
		for i, url := range secureUrls {
			// URLs already are wrapped in '', but we need to add a timeout flag.
			// Trim the trailing ' and re-add with the flag.
			trimmed := strings.TrimSuffix(url, "'")
			workloadPGURLs[i] = fmt.Sprintf("%s&statement_timeout=10s'", trimmed)
		}

		// Init & run a workload on the workload node.
		t.Status("initializing workload")
		initWorkloadCmd := fmt.Sprintf("./cockroach workload init kv %s ", secureUrls[0])
		c.Run(ctx, c.Node(workloadNode), initWorkloadCmd)

		t.Status("running workload")
		m := c.NewMonitor(ctx, c.Range(1, crdbNodes))
		m.Go(func(ctx context.Context) error {
			joinedURLs := strings.Join(workloadPGURLs, " ")
			runWorkloadCmd := fmt.Sprintf("./cockroach workload run kv --concurrency=32 --duration=1h %s", joinedURLs)
			c.Run(ctx, c.Node(workloadNode), runWorkloadCmd)
			return nil
		})
		m.Wait()
	}

	r.Add(registry.TestSpec{
		Name:    "network_logging",
		Owner:   registry.OwnerObsInf,
		Tags:    registry.Tags("daily"),
		Cluster: r.MakeClusterSpec(numNodesNetworkLogging),
		Leases:  registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runNetworkLogging(ctx, t, c)
		},
	})
}

func installFluentBit(ctx context.Context, t test.Test, c cluster.Cluster) (map[int]string, error) {
	numCRDBNodes := c.Spec().NodeCount - 1
	fbContainerIPs := make(map[int]string)
	for _, n := range c.Range(1, numCRDBNodes) {
		node := c.Node(n)
		t.Status(fmt.Sprintf("installing FluentBit container on node %d", n))
		// Create FluentBit container on the node with a TCP input and dev/null output.
		res, err := c.RunWithDetailsSingleNode(ctx, t.L(), node, fmt.Sprintf(
			"sudo docker run -d -p %d:%d --name=fluentbit fluent/fluent-bit -i tcp -o null",
			fluentBitTCPPort,
			fluentBitTCPPort))
		if err != nil {
			return nil, errors.Wrapf(err, "failed to init FluentBit container")
		}
		// Get the IP of the FluentBit container for our CRDB log config.
		res, err = c.RunWithDetailsSingleNode(ctx, t.L(), node,
			"sudo docker inspect --format='{{.NetworkSettings.IPAddress}}' fluentbit")
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get IP address for FluentBit container: %v")
		}
		stdout := strings.TrimSpace(res.Stdout)
		ip := net.ParseIP(stdout)
		if ip == nil {
			return nil, errors.Newf("invalid IP address for FluentBit container: %s", stdout)
		}
		fbContainerIPs[n] = fmt.Sprintf("%s:%d", stdout, fluentBitTCPPort)
		t.Status(fmt.Sprintf("completed FluentBit installation on node %d", n))
	}
	return fbContainerIPs, nil
}
