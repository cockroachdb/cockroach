// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"net/url"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

var (
	leftExternalConn  = url.URL{Scheme: "external", Host: "replication-uri-left"}
	rightExternalConn = url.URL{Scheme: "external", Host: "replication-uri-right"}
)

type ycsbWorkload struct {
	// workload is the YCSB workload letter e.g. a, b, ..., f.
	workloadType string
	// initRows is the number of records to pre-load into the user table.
	initRows int
	// waitDuration is the duration the workload should run for.
	debugRunDuration time.Duration
	// initSplits is the count of initial splits before resuming work
	initSplits int
}

func (ycsb ycsbWorkload) sourceInitCmd(tenantName string, nodes option.NodeListOption) string {
	cmd := roachtestutil.NewCommand(`./cockroach workload init ycsb --families=false`).
		MaybeFlag(ycsb.initRows > 0, "insert-count", ycsb.initRows).
		MaybeFlag(ycsb.initSplits > 0, "splits", ycsb.initSplits).
		Arg("{pgurl%s:%s}", nodes, tenantName)
	return cmd.String()
}

func (ycsb ycsbWorkload) sourceRunCmd(tenantName string, nodes option.NodeListOption) string {
	cmd := roachtestutil.NewCommand(`./cockroach workload run ycsb --families=false`).
		Option("tolerate-errors").
		Flag("workload", ycsb.workloadType).
		MaybeFlag(ycsb.debugRunDuration > 0, "duration", ycsb.debugRunDuration).
		Arg("{pgurl%s:%s}", nodes, tenantName)
	return cmd.String()
}

func (ycsb ycsbWorkload) runDriver(
	workloadCtx context.Context, c cluster.Cluster, t test.Test, setup *c2cSetup,
) error {
	return defaultWorkloadDriver(workloadCtx, setup, c, ycsb)
}

type LDRWorkload struct {
	workload          streamingWorkload
	manualSchemaSetup bool
	dbName            string
	tableNames        []string
}

func registerLogicalDataReplicationTests(r registry.Registry) {
	specs := []ldrTestSpec{
		{
			name: "ldr/kv0/workload=both/basic/immediate",
			clusterSpec: multiClusterSpec{
				leftNodes:  3,
				rightNodes: 3,
				clusterOpts: []spec.Option{
					spec.CPU(8),
					spec.WorkloadNode(),
					spec.WorkloadNodeCPU(8),
					spec.VolumeSize(100),
				},
			},
			ldrConfig: ldrConfig{mode: ModeValidated},
			run:       TestLDRBasic,
		},
		{
			name: "ldr/kv0/workload=both/basic/validated",
			clusterSpec: multiClusterSpec{
				leftNodes:  3,
				rightNodes: 3,
				clusterOpts: []spec.Option{
					spec.CPU(8),
					spec.WorkloadNode(),
					spec.WorkloadNodeCPU(8),
					spec.VolumeSize(100),
				},
			},
			ldrConfig: ldrConfig{mode: ModeValidated},
			run:       TestLDRBasic,
		},
		{
			name: "ldr/kv0/workload=both/update_heavy/immediate",
			clusterSpec: multiClusterSpec{
				leftNodes:  3,
				rightNodes: 3,
				clusterOpts: []spec.Option{
					spec.CPU(8),
					spec.WorkloadNode(),
					spec.WorkloadNodeCPU(8),
					spec.VolumeSize(100),
				},
			},
			ldrConfig: ldrConfig{mode: ModeValidated},
			run:       TestLDRUpdateHeavy,
		},
		{
			name: "ldr/kv0/workload=both/update_heavy/validated",
			clusterSpec: multiClusterSpec{
				leftNodes:  3,
				rightNodes: 3,
				clusterOpts: []spec.Option{
					spec.CPU(8),
					spec.WorkloadNode(),
					spec.WorkloadNodeCPU(8),
					spec.VolumeSize(100),
				},
			},
			ldrConfig: ldrConfig{mode: ModeValidated},
			run:       TestLDRUpdateHeavy,
		},
		{
			name: "ldr/kv0/workload=both/shutdown_node",
			clusterSpec: multiClusterSpec{
				leftNodes:  3,
				rightNodes: 3,
				clusterOpts: []spec.Option{
					spec.CPU(8),
					spec.WorkloadNode(),
					spec.WorkloadNodeCPU(8),
					spec.VolumeSize(100),
				},
			},
			run: TestLDROnNodeShutdown,
		},
		{
			name: "ldr/kv0/workload=both/network_partition",
			clusterSpec: multiClusterSpec{
				leftNodes:  3,
				rightNodes: 3,
				clusterOpts: []spec.Option{
					spec.CPU(8),
					spec.WorkloadNode(),
					spec.WorkloadNodeCPU(8),
					spec.VolumeSize(100),
				},
			},
			run: TestLDROnNetworkPartition,
		},
		{
			name: "ldr/kv0/workload=both/schema_change",
			clusterSpec: multiClusterSpec{
				leftNodes:  3,
				rightNodes: 3,
				clusterOpts: []spec.Option{
					spec.CPU(8),
					spec.WorkloadNode(),
					spec.WorkloadNodeCPU(8),
					spec.VolumeSize(100),
				},
			},
			run: TestLDRSchemaChange,
		},
		{
			name: "ldr/tpcc",
			clusterSpec: multiClusterSpec{
				leftNodes:  3,
				rightNodes: 3,
				clusterOpts: []spec.Option{
					spec.CPU(8),
					spec.WorkloadNode(),
					spec.WorkloadNodeCPU(8),
					spec.VolumeSize(100),
				},
			},
			ldrConfig: ldrConfig{
				initialScanTimeout: 30 * time.Minute,
			},
			run: TestLDRTPCC,
		},
		{
			name: "ldr/create/tpcc",
			clusterSpec: multiClusterSpec{
				leftNodes:  3,
				rightNodes: 3,
				clusterOpts: []spec.Option{
					spec.CPU(8),
					spec.WorkloadNode(),
					spec.WorkloadNodeCPU(8),
					spec.VolumeSize(100),
				},
			},
			ldrConfig: ldrConfig{
				// Usually takes around 10 minutes for 1000 tpcc init scan.
				initialScanTimeout: 20 * time.Minute,
				createTables:       true,
			},
			run: TestLDRCreateTablesTPCC,
		},
	}

	for _, sp := range specs {

		r.Add(registry.TestSpec{
			Name:             sp.name,
			Owner:            registry.OwnerDisasterRecovery,
			Timeout:          60 * time.Minute,
			CompatibleClouds: registry.OnlyGCE,
			Suites:           registry.Suites(registry.Nightly),
			Cluster:          sp.clusterSpec.ToSpec(r),
			Leases:           registry.MetamorphicLeases,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				rng, seed := randutil.NewPseudoRand()
				t.L().Printf("random seed is %d", seed)
				mc := multiCluster{
					c:    c,
					rng:  rng,
					spec: sp.clusterSpec,
				}
				setup, cleanup := mc.Start(ctx, t)
				defer cleanup()
				sp.run(ctx, t, c, setup, sp.ldrConfig)
			},
		})
	}
}

func TestLDRBasic(
	ctx context.Context, t test.Test, c cluster.Cluster, setup multiClusterSetup, ldrConfig ldrConfig,
) {
	duration := 15 * time.Minute
	initRows := 1000
	maxBlockBytes := 1024

	if c.IsLocal() {
		duration = 30 * time.Second
		initRows = 10
		maxBlockBytes = 32
	}

	ldrWorkload := LDRWorkload{
		workload: replicateKV{
			readPercent:             0,
			debugRunDuration:        duration,
			maxBlockBytes:           maxBlockBytes,
			initRows:                initRows,
			tolerateErrors:          true,
			initWithSplitAndScatter: !c.IsLocal()},
		dbName:     "kv",
		tableNames: []string{"kv"},
	}

	leftJobID, rightJobID := setupLDR(ctx, t, c, setup, ldrWorkload, ldrConfig)
	workloadDoneCh := make(chan struct{})
	monitor := c.NewMonitor(ctx, setup.CRDBNodes())
	validateLatency := setupLatencyVerifiers(ctx, t, c, monitor, leftJobID, rightJobID, setup, workloadDoneCh, 2*time.Minute)

	monitor.Go(func(ctx context.Context) error {
		defer close(workloadDoneCh)
		return c.RunE(ctx, option.WithNodes(setup.workloadNode), ldrWorkload.workload.sourceRunCmd("system", setup.CRDBNodes()))
	})

	monitor.Wait()
	validateLatency()
	VerifyCorrectness(ctx, c, t, setup, leftJobID, rightJobID, 2*time.Minute, ldrWorkload)
}

func TestLDRSchemaChange(
	ctx context.Context, t test.Test, c cluster.Cluster, setup multiClusterSetup, ldrConfig ldrConfig,
) {
	duration := 15 * time.Minute
	if c.IsLocal() {
		duration = 5 * time.Minute
	}

	ldrWorkload := LDRWorkload{
		workload: replicateKV{
			readPercent:             0,
			debugRunDuration:        duration,
			maxBlockBytes:           1024,
			initRows:                1000,
			initWithSplitAndScatter: true,
			tolerateErrors:          true,
		},
		dbName:     "kv",
		tableNames: []string{"kv"},
	}

	leftJobID, rightJobID := setupLDR(ctx, t, c, setup, ldrWorkload, ldrConfig)

	workloadDoneCh := make(chan struct{})
	monitor := c.NewMonitor(ctx, setup.CRDBNodes())
	validateLatency := setupLatencyVerifiers(ctx, t, c, monitor, leftJobID, rightJobID, setup, workloadDoneCh, 2*time.Minute)

	monitor.Go(func(ctx context.Context) error {
		defer close(workloadDoneCh)
		return c.RunE(ctx, option.WithNodes(setup.workloadNode), ldrWorkload.workload.sourceRunCmd("system", setup.CRDBNodes()))
	})

	// Run allowlisted schema changes on the replicated table on both the left
	// and right sides.
	setup.left.sysSQL.Exec(t, fmt.Sprintf("CREATE INDEX idx_left ON %s.%s(v, k)", ldrWorkload.dbName, ldrWorkload.tableNames[0]))
	setup.right.sysSQL.Exec(t, fmt.Sprintf("CREATE INDEX idx_right ON %s.%s(v, k)", ldrWorkload.dbName, ldrWorkload.tableNames[0]))
	setup.left.sysSQL.Exec(t, fmt.Sprintf("DROP INDEX %s.%s@idx_left", ldrWorkload.dbName, ldrWorkload.tableNames[0]))
	setup.right.sysSQL.Exec(t, fmt.Sprintf("DROP INDEX %s.%s@idx_right", ldrWorkload.dbName, ldrWorkload.tableNames[0]))

	// Verify that a non-allowlisted schema change fails.
	setup.left.sysSQL.ExpectErr(t,
		"schema change is disallowed on table .* because it is referenced by one or more logical replication jobs",
		fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN not_null_col INT NOT NULL DEFAULT 10", ldrWorkload.dbName, ldrWorkload.tableNames[0]),
	)

	monitor.Wait()
	validateLatency()
	VerifyCorrectness(ctx, c, t, setup, leftJobID, rightJobID, 2*time.Minute, ldrWorkload)
}

// TestLDRTPCC runs tpcc 10 warehouses across the two clusters during LDR
// steady state. The left is initialized with 10 warehouses and the right is
// initialized with 1.
func TestLDRTPCC(
	ctx context.Context, t test.Test, c cluster.Cluster, setup multiClusterSetup, ldrConfig ldrConfig,
) {
	duration := 10 * time.Minute
	warehouses := 10
	if c.IsLocal() {
		duration = 3 * time.Minute
		warehouses = 10
	}

	workload := LDRWorkload{
		workload: replicateTPCC{
			warehouses:     warehouses,
			duration:       duration,
			repairOrderIDs: true,
		},
		dbName:            "tpcc",
		manualSchemaSetup: true,
		tableNames:        []string{"customer", "district", "history", "item", "new_order", "order_line", "order", "stock", "warehouse"},
	}

	// Init the clusters manually, so the left has 10 warehouses and the right has
	// 1. Ideally the right cluster's table's would be empty, but you cant run
	// tpcc with 0 warehouses.
	//
	// TODO(msbutler): eventually LDR will create the right cluster's replicating
	// tables.
	c.Run(ctx,
		option.WithNodes(setup.workloadNode),
		fmt.Sprintf("./cockroach workload init tpcc --warehouses=1 --fks=false {pgurl:%d:system}", setup.right.nodes[0]))
	c.Run(ctx,
		option.WithNodes(setup.workloadNode),
		fmt.Sprintf("./cockroach workload init tpcc --warehouses=10 --fks=false {pgurl:%d:system}", setup.left.nodes[0]))
	leftJobID, rightJobID := setupLDR(ctx, t, c, setup, workload, ldrConfig)

	workloadDoneCh := make(chan struct{})
	maxExpectedLatency := 3 * time.Minute
	monitor := c.NewMonitor(ctx, setup.CRDBNodes())
	validateLatency := setupLatencyVerifiers(ctx, t, c, monitor, leftJobID, rightJobID, setup, workloadDoneCh, maxExpectedLatency)

	monitor.Go(func(ctx context.Context) error {
		defer close(workloadDoneCh)
		// Run workload on both clusters.
		return c.RunE(ctx, option.WithNodes(setup.workloadNode), workload.workload.sourceRunCmd("system", setup.CRDBNodes()))
	})

	monitor.Wait()
	validateLatency()
	VerifyCorrectness(ctx, c, t, setup, leftJobID, rightJobID, 2*time.Minute, workload)
}

// TestLDRCreateTablesTPCC inits the left cluster with 1000 warehouse tpcc,
// begins unidirectional fast initial scan LDR, starts a tpcc 1000 wh workload
// on the left, and observes initial scan, catchup scan, and steady state
// performance.
func TestLDRCreateTablesTPCC(
	ctx context.Context, t test.Test, c cluster.Cluster, setup multiClusterSetup, ldrConfig ldrConfig,
) {
	// duration is 30 minutes because during this time, the following occurs:
	// - 10 minute initial scan
	// - 5 mins with logical_replication.consumer.low_admission_priority.enabled =
	// false (during initial benchmarking, flipping this to true caused the
	// catchup scan to take 15 minutes).
	// - 15 mins of steady state. If the above scans take too long, the latency
	// verifier may trip.
	duration := 30 * time.Minute
	warehouses := 1000
	if c.IsLocal() {
		duration = 3 * time.Minute
		warehouses = 10
	}

	workload := LDRWorkload{
		workload: replicateTPCC{
			warehouses:     warehouses,
			duration:       duration,
			repairOrderIDs: true,
		},
		dbName:            "tpcc",
		manualSchemaSetup: true,
		tableNames:        []string{"customer", "district", "history", "item", "new_order", "order_line", "order", "stock", "warehouse"},
	}

	setup.right.sysSQL.Exec(t, "CREATE DATABASE tpcc")
	setup.right.sysSQL.Exec(t, "SET CLUSTER SETTING logical_replication.consumer.low_admission_priority.enabled = false")
	c.Run(ctx,
		option.WithNodes(setup.workloadNode),
		fmt.Sprintf("./cockroach workload init tpcc --warehouses=%d --fks=false {pgurl:%d:system}", warehouses, setup.left.nodes[0]))

	workloadDoneCh := make(chan struct{})
	monitor := c.NewMonitor(ctx, setup.CRDBNodes())
	monitor.Go(func(ctx context.Context) error {
		defer close(workloadDoneCh)
		// Run workload on the left cluster. Unlike other ldr tests at the moment,
		// this one spins up the source workload before LDR begins. This tests LWW
		// on data ingested via the offline initial scan.
		return c.RunE(ctx, option.WithNodes(setup.workloadNode), workload.workload.sourceRunCmd("system", setup.left.nodes))
	})
	_, rightJobID := setupLDR(ctx, t, c, setup, workload, ldrConfig)

	maxExpectedLatency := 3 * time.Minute
	validateLatency := setupLatencyVerifiers(ctx, t, c, monitor, 0 /* leftJobID */, rightJobID, setup, workloadDoneCh, maxExpectedLatency)

	monitor.Wait()
	validateLatency()

	// On the 1000 tpcc workload, this takes about 12 minutes.
	VerifyCorrectness(ctx, c, t, setup, 0 /* leftJobID */, rightJobID, 2*time.Minute, workload)
}

func TestLDRUpdateHeavy(
	ctx context.Context, t test.Test, c cluster.Cluster, setup multiClusterSetup, ldrConfig ldrConfig,
) {

	duration := 6 * time.Minute
	if c.IsLocal() {
		duration = 3 * time.Minute
	}

	ldrWorkload := LDRWorkload{
		workload: ycsbWorkload{
			workloadType:     "A",
			debugRunDuration: duration,
			initRows:         1000,
			initSplits:       1000,
		},
		dbName:     "ycsb",
		tableNames: []string{"usertable"},
	}

	leftJobID, rightJobID := setupLDR(ctx, t, c, setup, ldrWorkload, ldrConfig)

	workloadDoneCh := make(chan struct{})
	maxExpectedLatency := 3 * time.Minute
	monitor := c.NewMonitor(ctx, setup.CRDBNodes())
	validateLatency := setupLatencyVerifiers(ctx, t, c, monitor, leftJobID, rightJobID, setup, workloadDoneCh, maxExpectedLatency)

	monitor.Go(func(ctx context.Context) error {
		defer close(workloadDoneCh)
		return c.RunE(ctx, option.WithNodes(setup.workloadNode), ldrWorkload.workload.sourceRunCmd("system", setup.CRDBNodes()))
	})

	monitor.Wait()
	validateLatency()
	VerifyCorrectness(ctx, c, t, setup, leftJobID, rightJobID, 2*time.Minute, ldrWorkload)
}

func TestLDROnNodeShutdown(
	ctx context.Context, t test.Test, c cluster.Cluster, setup multiClusterSetup, ldrConfig ldrConfig,
) {

	duration := 10 * time.Minute
	if c.IsLocal() {
		duration = 3 * time.Minute
	}

	ldrWorkload := LDRWorkload{
		workload: replicateKV{
			readPercent:             0,
			debugRunDuration:        duration,
			maxBlockBytes:           1024,
			initRows:                1000,
			tolerateErrors:          true,
			initWithSplitAndScatter: true},
		dbName:     "kv",
		tableNames: []string{"kv"},
	}

	leftJobID, rightJobID := setupLDR(ctx, t, c, setup, ldrWorkload, ldrConfig)

	findCoordinatorNode := func(info *clusterInfo, jobID int, rightSide bool) int {
		var coordinatorNode int
		testutils.SucceedsWithin(t, func() error {
			return info.db.QueryRowContext(ctx,
				`SELECT coordinator_id FROM crdb_internal.jobs WHERE job_id = $1`, jobID).Scan(&coordinatorNode)
		}, time.Minute)
		if rightSide {
			// From the right cluster's perspective, node ids range from 1 to
			// num_dest_nodes, but from roachprod's perspective they range from
			// num_source_nodes+1 to num_crdb_roachprod nodes. We need to adjust for
			// this to shut down the right node. Example: if the coordinator node on the
			// dest cluster is 1, and there are 4 src cluster nodes, then
			// shut down roachprod node 5.
			coordinatorNode += len(setup.left.nodes)
		}
		return coordinatorNode
	}

	findNonCoordinatorNode := func(info *clusterInfo, rng *rand.Rand, coordinatorNode int) int {
		for {
			anotherNode := info.nodes.SeededRandNode(rng)[0]
			if anotherNode != coordinatorNode {
				return anotherNode
			}
		}
	}

	var shutdownSide *clusterInfo
	var shutdownNode int
	var coordinatorNode int
	var newGatewayNode int
	if setup.rng.Intn(2) == 0 {
		t.L().Printf("Shutting down on right side")
		shutdownSide = setup.right
		coordinatorNode = findCoordinatorNode(setup.right, rightJobID, true)
	} else {
		t.L().Printf("Shutting down node on left side")
		shutdownSide = setup.left
		coordinatorNode = findCoordinatorNode(setup.left, leftJobID, false)
	}
	if setup.rng.Intn(2) == 0 {
		shutdownNode = findNonCoordinatorNode(shutdownSide, setup.rng, coordinatorNode)
		newGatewayNode = coordinatorNode
		t.L().Printf("Shutting down worker node %d, new gateway node %d", shutdownNode, newGatewayNode)
	} else {
		shutdownNode = coordinatorNode
		newGatewayNode = findNonCoordinatorNode(shutdownSide, setup.rng, coordinatorNode)
		t.L().Printf("Shutting down coordinator node %d, new gateway node %d", shutdownNode, newGatewayNode)
	}

	// Switch gateway node to another node that will not shutdown, so we can still serve queries.
	shutdownSide.gatewayNodes[0] = newGatewayNode
	shutdownSide.db = c.Conn(ctx, t.L(), shutdownSide.gatewayNodes[0])
	defer shutdownSide.db.Close()
	shutdownSide.sysSQL = sqlutils.MakeSQLRunner(shutdownSide.db)

	var stopOpts option.StopOpts
	if setup.rng.Intn(2) == 0 {
		stopOpts = option.DefaultStopOpts()
		t.L().Printf("Shutting down node immediately")
	} else {
		stopOpts = option.NewStopOpts(option.Graceful(shutdownGracePeriod))
		t.L().Printf("Shutting down node gracefully")
	}

	// Setup latency verifiers, remembering to account for latency spike from killing a node
	maxExpectedLatency := 5 * time.Minute
	workloadDoneCh := make(chan struct{})
	monitor := c.NewMonitor(ctx, setup.CRDBNodes())
	validateLatency := setupLatencyVerifiers(ctx, t, c, monitor, leftJobID, rightJobID, setup, workloadDoneCh, maxExpectedLatency)

	monitor.Go(func(ctx context.Context) error {
		defer close(workloadDoneCh)
		return c.RunE(ctx, option.WithNodes(setup.workloadNode), ldrWorkload.workload.sourceRunCmd("system", setup.CRDBNodes()))
	})

	// Let workload run for a bit before we kill a node
	sleepNanos := setup.rng.Int63n((ldrWorkload.workload.(replicateKV).debugRunDuration / 10).Nanoseconds())
	sleepDuration := time.Duration(sleepNanos)
	t.L().Printf("Sleeping for %s before shutdown", sleepDuration)
	time.Sleep(sleepDuration)

	monitor.ExpectDeath()
	if err := c.StopE(ctx, t.L(), stopOpts, c.Node(shutdownNode)); err != nil {
		t.Fatalf("Unable to shutdown node: %s", err)
	}

	monitor.Wait()
	validateLatency()
	VerifyCorrectness(ctx, c, t, setup, leftJobID, rightJobID, 5*time.Minute, ldrWorkload)
}

// TestLDROnNetworkPartition aims to see what happens when both clusters
// are separated from one another by a network partition. This test will
// aim to keep the workload going on both sides and wait for reconciliation
// once the network partition has completed
func TestLDROnNetworkPartition(
	ctx context.Context, t test.Test, c cluster.Cluster, setup multiClusterSetup, ldrConfig ldrConfig,
) {
	duration := 10 * time.Minute
	if c.IsLocal() {
		duration = 3 * time.Minute
	}

	ldrWorkload := LDRWorkload{
		workload: replicateKV{
			readPercent:             0,
			debugRunDuration:        duration,
			maxBlockBytes:           1024,
			initRows:                1000,
			tolerateErrors:          true,
			initWithSplitAndScatter: true},
		dbName:     "kv",
		tableNames: []string{"kv"},
	}

	leftJobID, rightJobID := setupLDR(ctx, t, c, setup, ldrWorkload, ldrConfig)

	monitor := c.NewMonitor(ctx, setup.CRDBNodes())
	monitor.Go(func(ctx context.Context) error {
		return c.RunE(ctx, option.WithNodes(setup.workloadNode), ldrWorkload.workload.sourceRunCmd("system", setup.CRDBNodes()))
	})

	// Let workload run for a bit before we kill a node
	time.Sleep(ldrWorkload.workload.(replicateKV).debugRunDuration / 10)

	failNodesLength := len(setup.CRDBNodes()) / 2
	nodesToFail, err := setup.CRDBNodes().SeededRandList(setup.rng, failNodesLength)
	if err != nil {
		t.Fatal(err)
	}

	// We're not using the entire blackholeFailer setup, so break the interface contract and use this directly
	blackholeFailer := &blackholeFailer{t: t, c: c, input: true, output: true}
	disconnectDuration := ldrWorkload.workload.(replicateKV).debugRunDuration / 5
	t.L().Printf("Disconnecting nodes %v", nodesToFail)
	for _, nodeID := range nodesToFail {
		blackholeFailer.FailPartial(ctx, nodeID, setup.CRDBNodes())
	}

	// Sleep while workload continues
	t.L().Printf("Sleeping for %.2f minutes", disconnectDuration.Minutes())
	time.Sleep(disconnectDuration)

	// Re-enable
	blackholeFailer.Cleanup(ctx)
	t.L().Printf("Nodes reconnected. Waiting for workload to complete")

	monitor.Wait()
	VerifyCorrectness(ctx, c, t, setup, leftJobID, rightJobID, 5*time.Minute, ldrWorkload)
}

type ldrJobInfo struct {
	*jobRecord
}

// GetHighWater returns the replicated time.
func (c *ldrJobInfo) GetHighWater() time.Time {
	replicatedTime := c.progress.GetLogicalReplication().ReplicatedTime
	if replicatedTime.IsEmpty() {
		return time.Time{}
	}
	return replicatedTime.GoTime()
}

var _ jobInfo = (*ldrJobInfo)(nil)

func getLogicalDataReplicationJobInfo(db *gosql.DB, jobID int) (jobInfo, error) {
	jr, err := getJobRecord(db, jobID)
	if err != nil {
		return nil, err
	}
	return &ldrJobInfo{jr}, nil
}

type ldrTestSpec struct {
	name        string
	clusterSpec multiClusterSpec
	run         func(context.Context, test.Test, cluster.Cluster, multiClusterSetup, ldrConfig)
	ldrConfig   ldrConfig
}

type mode int

func (m mode) String() string {
	switch m {
	case ModeImmediate:
		return "immediate"
	case ModeValidated:
		return "validated"
	default:
		return "default"
	}
}

const (
	Default = iota
	ModeImmediate
	ModeValidated
)

type multiClusterSpec struct {
	clusterOpts []spec.Option

	leftNodes  int
	rightNodes int
}

func (mcs multiClusterSpec) NodeCount() int {
	return mcs.leftNodes + mcs.rightNodes + 1
}

func (mcs multiClusterSpec) LeftClusterStart() int {
	return 1
}

func (mcs multiClusterSpec) RightClusterStart() int {
	return mcs.leftNodes + 1
}

func (mcs multiClusterSpec) ToSpec(r registry.Registry) spec.ClusterSpec {
	return r.MakeClusterSpec(mcs.NodeCount(), mcs.clusterOpts...)
}

func (mcs *multiClusterSpec) LeftNodesList() option.NodeListOption {
	return option.NewNodeListOptionRange(1, mcs.leftNodes)
}

func (mcs *multiClusterSpec) RightNodesList() option.NodeListOption {
	return option.NewNodeListOptionRange(mcs.leftNodes+1, mcs.leftNodes+mcs.rightNodes)
}

type multiCluster struct {
	spec multiClusterSpec
	rng  *rand.Rand
	c    cluster.Cluster
}

type multiClusterSetup struct {
	workloadNode option.NodeListOption
	left         *clusterInfo
	right        *clusterInfo
	rng          *rand.Rand
}

func (mcs *multiClusterSetup) CRDBNodes() option.NodeListOption {
	return mcs.left.nodes.Merge(mcs.right.nodes)
}

func (mc *multiCluster) StartCluster(
	ctx context.Context, t test.Test, desc string, nodes option.NodeListOption, initTarget int,
) (*clusterInfo, func()) {
	startOps := option.NewStartOpts(option.NoBackupSchedule)
	startOps.RoachprodOpts.InitTarget = initTarget
	roachtestutil.SetDefaultAdminUIPort(mc.c, &startOps.RoachprodOpts)
	clusterSettings := install.MakeClusterSettings()
	mc.c.Start(ctx, t.L(), startOps, clusterSettings, nodes)

	node := nodes.SeededRandNode(mc.rng)

	addr, err := mc.c.ExternalPGUrl(ctx, t.L(), node, roachprod.PGURLOptions{})
	require.NoError(t, err)

	t.L().Printf("Randomly chosen %s node %d for gateway with address %s", desc, node, addr)

	require.NoError(t, err)

	db := mc.c.Conn(ctx, t.L(), node[0])
	sqlRunner := sqlutils.MakeSQLRunner(db)

	sqlRunner.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)

	pgURL, err := copyPGCertsAndMakeURL(ctx, t, mc.c, node, clusterSettings.PGUrlCertsDir, addr[0])
	require.NoError(t, err)

	cleanup := func() {
		if t.Failed() {
			debugCtx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			filename := fmt.Sprintf("%s_debug.zip", desc)
			if err := mc.c.FetchDebugZip(debugCtx, t.L(), filename, nodes); err != nil {
				t.L().Printf("failed to download debug zip to %s from node %s", filename, nodes)
			}
		}
		db.Close()
	}

	return &clusterInfo{
		pgURL:  pgURL,
		sysSQL: sqlRunner,
		db:     db,
		// Need to keep the node that we started the stream on
		gatewayNodes: []int{node[0]},
		nodes:        nodes,
	}, cleanup
}

func (mc *multiCluster) Start(ctx context.Context, t test.Test) (multiClusterSetup, func()) {
	leftCluster := mc.c.Range(1, mc.spec.leftNodes)
	rightCluster := mc.c.Range(mc.spec.leftNodes+1, mc.spec.leftNodes+mc.spec.rightNodes)
	workloadNode := mc.c.WorkloadNode()

	left, cleanupLeft := mc.StartCluster(ctx, t, "left", leftCluster, mc.spec.LeftClusterStart())
	right, cleanupRight := mc.StartCluster(ctx, t, "right", rightCluster, mc.spec.RightClusterStart())

	return multiClusterSetup{
			workloadNode: workloadNode,
			left:         left,
			right:        right,
			rng:          mc.rng,
		},
		func() {
			cleanupLeft()
			cleanupRight()
		}
}

type ldrConfig struct {
	mode               mode
	initialScanTimeout time.Duration
	createTables       bool
}

func setupLDR(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	setup multiClusterSetup,
	ldrWorkload LDRWorkload,
	ldrConfig ldrConfig,
) (int, int) {
	if !ldrWorkload.manualSchemaSetup {
		c.Run(ctx,
			option.WithNodes(setup.workloadNode),
			ldrWorkload.workload.sourceInitCmd("system", setup.right.nodes))

		c.Run(ctx,
			option.WithNodes(setup.workloadNode),
			ldrWorkload.workload.sourceInitCmd("system", setup.left.nodes))
	}

	tableNamesToStr := func(dbname string, tableNames []string) string {
		var tableNamesStr string
		for i, tableName := range tableNames {
			if i == 0 {
				tableNamesStr = fmt.Sprintf("(%s.%s", dbname, tableName)
			} else {
				tableNamesStr = fmt.Sprintf("%s, %s.%s", tableNamesStr, dbname, tableName)
			}
		}
		tableNamesStr = fmt.Sprintf("%s)", tableNamesStr)
		return tableNamesStr
	}

	dbName, tableNamesStr := ldrWorkload.dbName, tableNamesToStr(ldrWorkload.dbName, ldrWorkload.tableNames)

	startLDR := func(targetDB *sqlutils.SQLRunner, sourceURL string) int {

		options := ""
		if ldrConfig.mode != Default {
			options = fmt.Sprintf("WITH mode='%s'", ldrConfig.mode)
		}
		targetDB.Exec(t, fmt.Sprintf("USE %s", dbName))
		ldrCmd := fmt.Sprintf("CREATE LOGICAL REPLICATION STREAM FROM TABLES %s ON $1 INTO TABLES %s %s", tableNamesStr, tableNamesStr, options)
		if ldrConfig.createTables {
			ldrCmd = fmt.Sprintf("CREATE LOGICALLY REPLICATED TABLES %s FROM TABLES %s ON $1 %s WITH UNIDIRECTIONAL", tableNamesStr, tableNamesStr, options)
		}
		r := targetDB.QueryRow(t,
			ldrCmd,
			sourceURL,
		)
		var jobID int
		r.Scan(&jobID)
		return jobID
	}
	externalConnCmd := "CREATE EXTERNAL CONNECTION IF NOT EXISTS '%s' AS '%s'"
	setup.right.sysSQL.Exec(t, fmt.Sprintf(externalConnCmd, leftExternalConn.Host, setup.left.PgURLForDatabase(dbName)))
	setup.left.sysSQL.Exec(t, fmt.Sprintf(externalConnCmd, rightExternalConn.Host, setup.right.PgURLForDatabase(dbName)))
	t.L().Printf("created external connections")

	rightJobID := startLDR(setup.right.sysSQL, leftExternalConn.String())
	var leftJobID int
	if !ldrConfig.createTables {
		leftJobID = startLDR(setup.left.sysSQL, rightExternalConn.String())
	}
	// TODO(ssd): We wait for the replicated time to
	// avoid starting the workload here until we
	// have the behaviour around initial scans
	// sorted out.
	initialScanTimeout := 2 * time.Minute
	if ldrConfig.initialScanTimeout != 0 {
		initialScanTimeout = ldrConfig.initialScanTimeout
	}
	initScanMon := c.NewMonitor(ctx, setup.CRDBNodes())
	approxInitScanStart := timeutil.Now()
	t.L().Printf("Waiting for initial scan(s) to complete")
	initScanMon.Go(func(ctx context.Context) error {
		if leftJobID == 0 {
			t.L().Printf("No left job created")
			return nil
		}
		waitForReplicatedTime(t, leftJobID, setup.left.db, getLogicalDataReplicationJobInfo, initialScanTimeout)
		t.L().Printf("Initial scan for left job completed in %s", timeutil.Since(approxInitScanStart))
		return nil
	})
	initScanMon.Go(func(ctx context.Context) error {
		waitForReplicatedTime(t, rightJobID, setup.right.db, getLogicalDataReplicationJobInfo, initialScanTimeout)
		t.L().Printf("Initial scan for right job completed in %s", timeutil.Since(approxInitScanStart))
		return nil
	})
	initScanMon.Wait()
	t.L().Printf("LDR Setup complete")
	return leftJobID, rightJobID
}

// setupLatencyVerifiers sets up latency verifiers for the left and right ldr
// jobs. If the left job ID is 0, then this function assumes the left job does
// not exist.
func setupLatencyVerifiers(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	mon cluster.Monitor,
	leftJobID, rightJobID int,
	setup multiClusterSetup,
	workloadDoneCh chan struct{},
	maxExpectedLatency time.Duration,
) func() {

	var llv *latencyVerifier
	if leftJobID != 0 {
		llv = makeLatencyVerifier("ldr-left", 0, maxExpectedLatency, t.L(),
			getLogicalDataReplicationJobInfo, t.Status, false /* tolerateErrors */)
		defer llv.maybeLogLatencyHist()
	}

	rlv := makeLatencyVerifier("ldr-right", 0, maxExpectedLatency, t.L(),
		getLogicalDataReplicationJobInfo, t.Status, false /* tolerateErrors */)
	defer rlv.maybeLogLatencyHist()

	debugZipFetcher := &sync.Once{}

	mon.Go(func(ctx context.Context) error {
		if leftJobID == 0 {
			t.L().Printf("No left job created")
			return nil
		}
		if err := llv.pollLatencyUntilJobSucceeds(ctx, setup.left.db, leftJobID, time.Second, workloadDoneCh); err != nil {
			debugZipFetcher.Do(func() { getDebugZips(ctx, t, c, setup) })
			return err
		}
		return nil
	})
	mon.Go(func(ctx context.Context) error {
		if err := rlv.pollLatencyUntilJobSucceeds(ctx, setup.right.db, rightJobID, time.Second, workloadDoneCh); err != nil {
			debugZipFetcher.Do(func() { getDebugZips(ctx, t, c, setup) })
			return err
		}
		return nil
	})
	return func() {
		rlv.assertValid(t)
		if leftJobID != 0 {
			llv.assertValid(t)
		}
	}
}

func VerifyCorrectness(
	ctx context.Context,
	c cluster.Cluster,
	t test.Test,
	setup multiClusterSetup,
	leftJobID, rightJobID int,
	waitTime time.Duration,
	ldrWorkload LDRWorkload,
) {
	now := timeutil.Now()
	t.L().Printf("Waiting for replicated times to catchup before verifying left and right clusters")
	if leftJobID != 0 {
		waitForReplicatedTimeToReachTimestamp(t, leftJobID, setup.left.db, getLogicalDataReplicationJobInfo, waitTime, now)
		require.NoError(t, replicationtestutils.CheckEmptyDLQs(ctx, setup.left.db, ldrWorkload.dbName))
	}
	waitForReplicatedTimeToReachTimestamp(t, rightJobID, setup.right.db, getLogicalDataReplicationJobInfo, waitTime, now)
	require.NoError(t, replicationtestutils.CheckEmptyDLQs(ctx, setup.right.db, ldrWorkload.dbName))

	t.L().Printf("Verifying equality of left and right clusters")
	for _, tableName := range ldrWorkload.tableNames {
		m := c.NewMonitor(context.Background(), setup.CRDBNodes())
		var leftFingerprint, rightFingerprint [][]string
		queryStmt := fmt.Sprintf("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE %s.%s", ldrWorkload.dbName, tableName)
		m.Go(func(ctx context.Context) error {
			leftFingerprint = setup.left.sysSQL.QueryStr(t, queryStmt)
			return nil
		})
		m.Go(func(ctx context.Context) error {
			rightFingerprint = setup.right.sysSQL.QueryStr(t, queryStmt)
			return nil
		})
		m.Wait()
		require.Equal(t, leftFingerprint, rightFingerprint, "fingerprint mismatch for table %s", tableName)
	}
}

func getDebugZips(ctx context.Context, t test.Test, c cluster.Cluster, setup multiClusterSetup) {
	if err := c.FetchDebugZip(ctx, t.L(), "latency_left_debug.zip", setup.left.nodes); err != nil {
		t.L().Errorf("could not fetch debug zip: %v", err)
	}
	if err := c.FetchDebugZip(ctx, t.L(), "latency_right_debug.zip", setup.right.nodes); err != nil {
		t.L().Errorf("could not fetch debug zip: %v", err)
	}
}
