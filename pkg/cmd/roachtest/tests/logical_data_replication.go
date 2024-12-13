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

func registerLogicalDataReplicationTests(r registry.Registry) {
	for _, sp := range []ldrTestSpec{
		{
			name: "ldr/kv0/workload=both/ingestion=both",
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
			run: func(ctx context.Context, t test.Test, c cluster.Cluster, setup multiClusterSetup) {
				kvWorkload := replicateKV{
					readPercent:             0,
					debugRunDuration:        15 * time.Minute,
					maxBlockBytes:           1024,
					initWithSplitAndScatter: true}

				c.Run(ctx,
					option.WithNodes(setup.workloadNode),
					kvWorkload.sourceInitCmd("system", setup.left.nodes))
				c.Run(ctx,
					option.WithNodes(setup.workloadNode),
					kvWorkload.sourceInitCmd("system", setup.right.nodes))

				// Setup LDR-specific columns
				setup.left.sysSQL.Exec(t, "ALTER TABLE kv.kv ADD COLUMN crdb_replication_origin_timestamp DECIMAL NOT VISIBLE DEFAULT NULL ON UPDATE NULL")
				setup.right.sysSQL.Exec(t, "ALTER TABLE kv.kv ADD COLUMN crdb_replication_origin_timestamp DECIMAL NOT VISIBLE DEFAULT NULL ON UPDATE NULL")

				startLDR := func(targetDB *sqlutils.SQLRunner, sourceURL string) int {
					targetDB.Exec(t, "USE kv")
					r := targetDB.QueryRow(t,
						"CREATE LOGICAL REPLICATION STREAM FROM TABLE kv ON $1 INTO TABLE kv",
						sourceURL,
					)
					var jobID int
					r.Scan(&jobID)
					return jobID
				}

				leftJobID := startLDR(setup.left.sysSQL, setup.right.PgURLForDatabase("kv"))
				rightJobID := startLDR(setup.right.sysSQL, setup.left.PgURLForDatabase("kv"))

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
				getDebugZips := func() {
					if err := c.FetchDebugZip(ctx, t.L(), "latency_left_debug.zip", setup.left.nodes); err != nil {
						t.L().Errorf("could not fetch debug zip: %v", err)
					}
					if err := c.FetchDebugZip(ctx, t.L(), "latency_right_debug.zip", setup.right.nodes); err != nil {
						t.L().Errorf("could not fetch debug zip: %v", err)
					}
				}

				monitor := c.NewMonitor(ctx, setup.CRDBNodes())
				monitor.Go(func(ctx context.Context) error {
					if err := llv.pollLatencyUntilJobSucceeds(ctx, setup.left.db, leftJobID, time.Second, workloadDoneCh); err != nil {
						debugZipFetcher.Do(getDebugZips)
						return err
					}
					return nil
				})
				monitor.Go(func(ctx context.Context) error {
					if err := rlv.pollLatencyUntilJobSucceeds(ctx, setup.right.db, rightJobID, time.Second, workloadDoneCh); err != nil {
						debugZipFetcher.Do(getDebugZips)
						return err
					}
					return nil
				})

				// TODO(ssd): We wait for the replicated time to
				// avoid starting the workload here until we
				// have the behaviour around initial scans
				// sorted out.
				waitForReplicatedTime(t, leftJobID, setup.left.db, getLogicalDataReplicationJobInfo, 2*time.Minute)
				waitForReplicatedTime(t, rightJobID, setup.right.db, getLogicalDataReplicationJobInfo, 2*time.Minute)

				monitor.Go(func(ctx context.Context) error {
					defer close(workloadDoneCh)
					return c.RunE(ctx, option.WithNodes(setup.workloadNode), kvWorkload.sourceRunCmd("system", setup.CRDBNodes()))
				})

				monitor.Wait()

				now := timeutil.Now()

				waitForReplicatedTimeToReachTimestamp(t, leftJobID, setup.left.db, getLogicalDataReplicationJobInfo, 2*time.Minute, now)
				waitForReplicatedTimeToReachTimestamp(t, rightJobID, setup.right.db, getLogicalDataReplicationJobInfo, 2*time.Minute, now)

				llv.assertValid(t)
				rlv.assertValid(t)

				// TODO(ssd): Decide how we want to fingerprint
				// this table while we are using in-row storage
				// for crdb_internal_mvcc_timestamp.
				var leftCount, rightCount int
				setup.left.sysSQL.QueryRow(t, "SELECT count(1) FROM kv.kv").Scan(&leftCount)
				setup.right.sysSQL.QueryRow(t, "SELECT count(1) FROM kv.kv").Scan(&rightCount)
				require.Equal(t, leftCount, rightCount)
			},
		},
	} {
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
				sp.run(ctx, t, c, setup)

			},
		})
	}
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
	run         func(context.Context, test.Test, cluster.Cluster, multiClusterSetup)
}

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
		pgURL:        pgURL,
		sysSQL:       sqlRunner,
		db:           db,
		gatewayNodes: nodes,
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
		},
		func() {
			cleanupLeft()
			cleanupRight()
		}
}
