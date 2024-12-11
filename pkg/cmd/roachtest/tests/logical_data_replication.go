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
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
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
	workload  streamingWorkload
	dbName    string
	tableName string
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
			mode: ModeImmediate,
			run:  TestLDRBasic,
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
			mode: ModeValidated,
			run:  TestLDRBasic,
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
			mode: ModeImmediate,
			run:  TestLDRUpdateHeavy,
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
			mode: ModeValidated,
			run:  TestLDRUpdateHeavy,
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
			RequiresLicense:  true,
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
				sp.run(ctx, t, c, setup, sp.mode)
			},
		})
	}
}

func TestLDRBasic(
	ctx context.Context, t test.Test, c cluster.Cluster, setup multiClusterSetup, mode mode,
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
			initWithSplitAndScatter: !c.IsLocal()},
		dbName:    "kv",
		tableName: "kv",
	}

	leftJobID, rightJobID := setupLDR(ctx, t, c, setup, ldrWorkload, mode)

	// Setup latency verifiers
	maxExpectedLatency := 2 * time.Minute
	llv := makeLatencyVerifier("ldr-left", 0, maxExpectedLatency, t.L(),
		getLogicalDataReplicationJobInfo, t.Status, false /* tolerateErrors */)
	defer llv.maybeLogLatencyHist()

	rlv := makeLatencyVerifier("ldr-right", 0, maxExpectedLatency, t.L(),
		getLogicalDataReplicationJobInfo, t.Status, false /* tolerateErrors */)
	defer rlv.maybeLogLatencyHist()

	workloadDoneCh := make(chan struct{})
	debugZipFetcher := &sync.Once{}

	monitor := c.NewMonitor(ctx, setup.CRDBNodes())
	monitor.Go(func(ctx context.Context) error {
		if err := llv.pollLatencyUntilJobSucceeds(ctx, setup.left.db, leftJobID, time.Second, workloadDoneCh); err != nil {
			debugZipFetcher.Do(func() { getDebugZips(ctx, t, c, setup) })
			return err
		}
		return nil
	})
	monitor.Go(func(ctx context.Context) error {
		if err := rlv.pollLatencyUntilJobSucceeds(ctx, setup.right.db, rightJobID, time.Second, workloadDoneCh); err != nil {
			debugZipFetcher.Do(func() { getDebugZips(ctx, t, c, setup) })
			return err
		}
		return nil
	})

	monitor.Go(func(ctx context.Context) error {
		defer close(workloadDoneCh)
		return c.RunE(ctx, option.WithNodes(setup.workloadNode), ldrWorkload.workload.sourceRunCmd("system", setup.CRDBNodes()))
	})

	monitor.Wait()

	llv.assertValid(t)
	rlv.assertValid(t)

	VerifyCorrectness(ctx, c, t, setup, leftJobID, rightJobID, 2*time.Minute, ldrWorkload)
}

func TestLDRSchemaChange(
	ctx context.Context, t test.Test, c cluster.Cluster, setup multiClusterSetup, mode mode,
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
			initWithSplitAndScatter: true},
		dbName:    "kv",
		tableName: "kv",
	}

	leftJobID, rightJobID := setupLDR(ctx, t, c, setup, ldrWorkload, mode)

	// Setup latency verifiers
	maxExpectedLatency := 2 * time.Minute
	llv := makeLatencyVerifier("ldr-left", 0, maxExpectedLatency, t.L(),
		getLogicalDataReplicationJobInfo, t.Status, false /* tolerateErrors */)
	defer llv.maybeLogLatencyHist()

	rlv := makeLatencyVerifier("ldr-right", 0, maxExpectedLatency, t.L(),
		getLogicalDataReplicationJobInfo, t.Status, false /* tolerateErrors */)
	defer rlv.maybeLogLatencyHist()

	workloadDoneCh := make(chan struct{})
	debugZipFetcher := &sync.Once{}

	monitor := c.NewMonitor(ctx, setup.CRDBNodes())
	monitor.Go(func(ctx context.Context) error {
		if err := llv.pollLatencyUntilJobSucceeds(ctx, setup.left.db, leftJobID, time.Second, workloadDoneCh); err != nil {
			debugZipFetcher.Do(func() { getDebugZips(ctx, t, c, setup) })
			return err
		}
		return nil
	})
	monitor.Go(func(ctx context.Context) error {
		if err := rlv.pollLatencyUntilJobSucceeds(ctx, setup.right.db, rightJobID, time.Second, workloadDoneCh); err != nil {
			debugZipFetcher.Do(func() { getDebugZips(ctx, t, c, setup) })
			return err
		}
		return nil
	})

	monitor.Go(func(ctx context.Context) error {
		defer close(workloadDoneCh)
		return c.RunE(ctx, option.WithNodes(setup.workloadNode), ldrWorkload.workload.sourceRunCmd("system", setup.CRDBNodes()))
	})

	// Run allowlisted schema changes on the replicated table on both the left
	// and right sides.
	setup.left.sysSQL.Exec(t, fmt.Sprintf("CREATE INDEX idx_left ON %s.%s(v, k)", ldrWorkload.dbName, ldrWorkload.tableName))
	setup.right.sysSQL.Exec(t, fmt.Sprintf("CREATE INDEX idx_right ON %s.%s(v, k)", ldrWorkload.dbName, ldrWorkload.tableName))
	setup.left.sysSQL.Exec(t, fmt.Sprintf("DROP INDEX %s.%s@idx_left", ldrWorkload.dbName, ldrWorkload.tableName))
	setup.right.sysSQL.Exec(t, fmt.Sprintf("DROP INDEX %s.%s@idx_right", ldrWorkload.dbName, ldrWorkload.tableName))

	// Verify that a non-allowlisted schema change fails.
	setup.left.sysSQL.ExpectErr(t,
		"schema change is disallowed on table .* because it is referenced by one or more logical replication jobs",
		fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN not_null_col INT NOT NULL DEFAULT 10", ldrWorkload.dbName, ldrWorkload.tableName),
	)

	monitor.Wait()

	llv.assertValid(t)
	rlv.assertValid(t)

	VerifyCorrectness(ctx, c, t, setup, leftJobID, rightJobID, 2*time.Minute, ldrWorkload)
}

func TestLDRUpdateHeavy(
	ctx context.Context, t test.Test, c cluster.Cluster, setup multiClusterSetup, mode mode,
) {

	duration := 10 * time.Minute
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
		dbName:    "ycsb",
		tableName: "usertable",
	}

	leftJobID, rightJobID := setupLDR(ctx, t, c, setup, ldrWorkload, mode)

	// Setup latency verifiers
	maxExpectedLatency := 3 * time.Minute
	llv := makeLatencyVerifier("ldr-left", 0, maxExpectedLatency, t.L(),
		getLogicalDataReplicationJobInfo, t.Status, false /* tolerateErrors */)
	defer llv.maybeLogLatencyHist()

	rlv := makeLatencyVerifier("ldr-right", 0, maxExpectedLatency, t.L(),
		getLogicalDataReplicationJobInfo, t.Status, false /* tolerateErrors */)
	defer rlv.maybeLogLatencyHist()

	workloadDoneCh := make(chan struct{})
	debugZipFetcher := &sync.Once{}

	monitor := c.NewMonitor(ctx, setup.CRDBNodes())
	monitor.Go(func(ctx context.Context) error {
		if err := llv.pollLatencyUntilJobSucceeds(ctx, setup.left.db, leftJobID, time.Second, workloadDoneCh); err != nil {
			debugZipFetcher.Do(func() { getDebugZips(ctx, t, c, setup) })
			return err
		}
		return nil
	})
	monitor.Go(func(ctx context.Context) error {
		if err := rlv.pollLatencyUntilJobSucceeds(ctx, setup.right.db, rightJobID, time.Second, workloadDoneCh); err != nil {
			debugZipFetcher.Do(func() { getDebugZips(ctx, t, c, setup) })
			return err
		}
		return nil
	})

	monitor.Go(func(ctx context.Context) error {
		defer close(workloadDoneCh)
		return c.RunE(ctx, option.WithNodes(setup.workloadNode), ldrWorkload.workload.sourceRunCmd("system", setup.CRDBNodes()))
	})

	monitor.Wait()

	llv.assertValid(t)
	rlv.assertValid(t)

	VerifyCorrectness(ctx, c, t, setup, leftJobID, rightJobID, 2*time.Minute, ldrWorkload)
}

func TestLDROnNodeShutdown(
	ctx context.Context, t test.Test, c cluster.Cluster, setup multiClusterSetup, mode mode,
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
			initWithSplitAndScatter: true},
		dbName:    "kv",
		tableName: "kv",
	}

	leftJobID, rightJobID := setupLDR(ctx, t, c, setup, ldrWorkload, mode)

	// Setup latency verifiers, remembering to account for latency spike from killing a node
	maxExpectedLatency := 5 * time.Minute
	llv := makeLatencyVerifier("ldr-left", 0, maxExpectedLatency, t.L(),
		getLogicalDataReplicationJobInfo, t.Status, false /* tolerateErrors */)
	defer llv.maybeLogLatencyHist()

	rlv := makeLatencyVerifier("ldr-right", 0, maxExpectedLatency, t.L(),
		getLogicalDataReplicationJobInfo, t.Status, false /* tolerateErrors */)
	defer rlv.maybeLogLatencyHist()

	workloadDoneCh := make(chan struct{})
	debugZipFetcher := &sync.Once{}

	monitor := c.NewMonitor(ctx, setup.CRDBNodes())
	monitor.Go(func(ctx context.Context) error {
		if err := llv.pollLatencyUntilJobSucceeds(ctx, setup.left.db, leftJobID, time.Second, workloadDoneCh); err != nil {
			debugZipFetcher.Do(func() { getDebugZips(ctx, t, c, setup) })
			return err
		}
		return nil
	})
	monitor.Go(func(ctx context.Context) error {
		if err := rlv.pollLatencyUntilJobSucceeds(ctx, setup.right.db, rightJobID, time.Second, workloadDoneCh); err != nil {
			debugZipFetcher.Do(func() { getDebugZips(ctx, t, c, setup) })
			return err
		}
		return nil
	})

	monitor.Go(func(ctx context.Context) error {
		defer close(workloadDoneCh)
		return c.RunE(ctx, option.WithNodes(setup.workloadNode), ldrWorkload.workload.sourceRunCmd("system", setup.CRDBNodes()))
	})

	// Let workload run for a bit before we kill a node
	time.Sleep(ldrWorkload.workload.(replicateKV).debugRunDuration / 10)

	findNodeToStop := func(info *clusterInfo, rng *rand.Rand) int {
		for {
			anotherNode := info.nodes.SeededRandNode(rng)[0]
			if anotherNode != info.gatewayNodes[0] {
				return anotherNode
			}
		}
	}

	t.L().Printf("Finding node to stop Left")
	nodeToStopL := findNodeToStop(setup.left, setup.rng)
	t.L().Printf("Finding node to stop right")
	nodeToStopR := findNodeToStop(setup.right, setup.rng)

	// Graceful shutdown on both nodes
	// TODO(naveen.setlur): maybe switch this to a less graceful shutdown via SIGKILL
	stopOpts := option.NewStopOpts(option.Graceful(shutdownGracePeriod))
	t.L().Printf("Shutting down node-left: %d", nodeToStopL)
	monitor.ExpectDeath()
	if err := c.StopE(ctx, t.L(), stopOpts, c.Node(nodeToStopL)); err != nil {
		t.Fatalf("Unable to shutdown node: %s", err)
	}

	t.L().Printf("Shutting down node-right: %d", nodeToStopR)
	monitor.ExpectDeath()
	if err := c.StopE(ctx, t.L(), stopOpts, c.Node(nodeToStopR)); err != nil {
		t.Fatalf("Unable to shutdown node: %s", err)
	}

	monitor.Wait()
	VerifyCorrectness(ctx, c, t, setup, leftJobID, rightJobID, 5*time.Minute, ldrWorkload)
}

// TestLDROnNetworkPartition aims to see what happens when both clusters
// are separated from one another by a network partition. This test will
// aim to keep the workload going on both sides and wait for reconciliation
// once the network partition has completed
func TestLDROnNetworkPartition(
	ctx context.Context, t test.Test, c cluster.Cluster, setup multiClusterSetup, mode mode,
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
			initWithSplitAndScatter: true},
		dbName:    "kv",
		tableName: "kv",
	}

	leftJobID, rightJobID := setupLDR(ctx, t, c, setup, ldrWorkload, mode)

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
	run         func(context.Context, test.Test, cluster.Cluster, multiClusterSetup, mode)
	mode        mode
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

func setupLDR(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	setup multiClusterSetup,
	ldrWorkload LDRWorkload,
	mode mode,
) (int, int) {
	c.Run(ctx,
		option.WithNodes(setup.workloadNode),
		ldrWorkload.workload.sourceInitCmd("system", setup.left.nodes))
	c.Run(ctx,
		option.WithNodes(setup.workloadNode),
		ldrWorkload.workload.sourceInitCmd("system", setup.right.nodes))

	dbName, tableName := ldrWorkload.dbName, ldrWorkload.tableName

	startLDR := func(targetDB *sqlutils.SQLRunner, sourceURL string) int {
		options := ""
		if mode != Default {
			options = fmt.Sprintf("WITH mode='%s'", mode)
		}
		targetDB.Exec(t, fmt.Sprintf("USE %s", dbName))
		r := targetDB.QueryRow(t,
			fmt.Sprintf("CREATE LOGICAL REPLICATION STREAM FROM TABLE %s ON $1 INTO TABLE %s %s", tableName, tableName, options),
			sourceURL,
		)
		var jobID int
		r.Scan(&jobID)
		return jobID
	}

	leftJobID := startLDR(setup.left.sysSQL, setup.right.PgURLForDatabase(dbName))
	rightJobID := startLDR(setup.right.sysSQL, setup.left.PgURLForDatabase(dbName))

	// TODO(ssd): We wait for the replicated time to
	// avoid starting the workload here until we
	// have the behaviour around initial scans
	// sorted out.
	waitForReplicatedTime(t, leftJobID, setup.left.db, getLogicalDataReplicationJobInfo, 2*time.Minute)
	waitForReplicatedTime(t, rightJobID, setup.right.db, getLogicalDataReplicationJobInfo, 2*time.Minute)

	t.L().Printf("LDR Setup complete")
	return leftJobID, rightJobID
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
	t.L().Printf("Verifying left and right tables")
	now := timeutil.Now()

	waitForReplicatedTimeToReachTimestamp(t, leftJobID, setup.left.db, getLogicalDataReplicationJobInfo, waitTime, now)
	waitForReplicatedTimeToReachTimestamp(t, rightJobID, setup.right.db, getLogicalDataReplicationJobInfo, waitTime, now)
	require.NoError(t, replicationtestutils.CheckEmptyDLQs(ctx, setup.left.db, ldrWorkload.dbName))
	require.NoError(t, replicationtestutils.CheckEmptyDLQs(ctx, setup.right.db, ldrWorkload.dbName))

	m := c.NewMonitor(context.Background(), setup.CRDBNodes())
	var leftFingerprint, rightFingerprint [][]string
	queryStmt := fmt.Sprintf("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE %s.%s", ldrWorkload.dbName, ldrWorkload.tableName)
	m.Go(func(ctx context.Context) error {
		leftFingerprint = setup.left.sysSQL.QueryStr(t, queryStmt)
		return nil
	})
	m.Go(func(ctx context.Context) error {
		rightFingerprint = setup.right.sysSQL.QueryStr(t, queryStmt)
		return nil
	})
	m.Wait()
	require.Equal(t, leftFingerprint, rightFingerprint)
}

func getDebugZips(ctx context.Context, t test.Test, c cluster.Cluster, setup multiClusterSetup) {
	if err := c.FetchDebugZip(ctx, t.L(), "latency_left_debug.zip", setup.left.nodes); err != nil {
		t.L().Errorf("could not fetch debug zip: %v", err)
	}
	if err := c.FetchDebugZip(ctx, t.L(), "latency_right_debug.zip", setup.right.nodes); err != nil {
		t.L().Errorf("could not fetch debug zip: %v", err)
	}
}
