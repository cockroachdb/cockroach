// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	spec2 "github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// loqTestSpec defines workload to run on cluster as well as range size which
// affects how frequently ranges are split. Splitting behavior introduces
// uncertainty that triggers situations where plan can't be created or resulting
// cluster state is not workable.
type loqTestSpec struct {
	wl workload
	// We don't use one "small" limit for different workloads as it depends on the
	// data size and needs to be tweaked per workload to have a non zero failure
	// rate.
	rangeSizeMB int64
}

// testName builds a test name based on mode and workload. mode argument is
// optional and is not included in the path if not set. It is done to preserve
// names for offline tests that have a success history stored as roachperf
// artifacts. If we rename it, they'll have to be checked manually.
// TODO: remove this exception once offline recovery is deprecated.
func (s loqTestSpec) testName(mode string) string {
	sizeName := "default"
	if s.rangeSizeMB > 0 {
		sizeName = fmt.Sprintf("%dmb", s.rangeSizeMB)
	}
	if len(mode) > 0 {
		return fmt.Sprintf("loqrecovery/%s/workload=%s/rangeSize=%s", mode, s.wl, sizeName)
	}
	return fmt.Sprintf("loqrecovery/workload=%s/rangeSize=%s", s.wl, sizeName)
}

func registerLOQRecovery(r registry.Registry) {
	spec := r.MakeClusterSpec(6, spec2.WorkloadNode())
	for _, s := range []loqTestSpec{
		{wl: movrLoqWorkload{concurrency: 32}, rangeSizeMB: 2},
		{wl: movrLoqWorkload{concurrency: 32}},
		{wl: tpccLoqWorkload{warehouses: 100, concurrency: 32}, rangeSizeMB: 16},
		{wl: tpccLoqWorkload{warehouses: 100, concurrency: 32}},
	} {
		testSpec := s
		r.Add(registry.TestSpec{
			Name:             s.testName(""),
			Owner:            registry.OwnerKV,
			Benchmark:        true,
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Cluster:          spec,
			Leases:           registry.MetamorphicLeases,
			SkipPostValidations: registry.PostValidationReplicaDivergence |
				registry.PostValidationInvalidDescriptors | registry.PostValidationNoDeadNodes,
			NonReleaseBlocker: true,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runRecoverLossOfQuorum(ctx, t, c, testSpec)
			},
		})
		r.Add(registry.TestSpec{
			Name:             s.testName("half-online"),
			Owner:            registry.OwnerKV,
			Benchmark:        true,
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Cluster:          spec,
			Leases:           registry.MetamorphicLeases,
			SkipPostValidations: registry.PostValidationReplicaDivergence |
				registry.PostValidationInvalidDescriptors | registry.PostValidationNoDeadNodes,
			NonReleaseBlocker: true,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runHalfOnlineRecoverLossOfQuorum(ctx, t, c, testSpec)
			},
		})
	}
}

type testOutcomeMetric int

const (
	// Plan can not be created.
	planCantBeCreated testOutcomeMetric = 0
	// Nodes fail to start after recovery was performed.
	restartFailed testOutcomeMetric = 10
	// Nodes started, but workload failed to succeed on any writes after restart.
	workloadFailed testOutcomeMetric = 20
	// Nodes restarted but decommission didn't work.
	decommissionFailed testOutcomeMetric = 30
	// Nodes restarted and workload produced some writes.
	success testOutcomeMetric = 100
)

var outcomeNames = map[testOutcomeMetric]string{
	planCantBeCreated:  "planCantBeCreated",
	restartFailed:      "restartFailed",
	decommissionFailed: "decommissionFailed",
	workloadFailed:     "workloadFailed",
	success:            "success",
}

// recoveryImpossibleError is an error indicating that we have encountered
// recovery failure that is detected by recovery procedures and is not a test
// failure.
type recoveryImpossibleError struct {
	testOutcome testOutcomeMetric
}

func (r *recoveryImpossibleError) Error() string {
	return fmt.Sprintf("recovery failed with code %d", r.testOutcome)
}

/*
The purpose of this test is to exercise loss of quorum recovery on a real
cluster and ensure that recovery doesn't cause it to crash or fail in unexpected
ways.
This test does not expect recovery to always succeed functionally e.g. failure
to create a recovery plan is a success.
Test will fail for infrastructure failure reasons like failure to setup a
cluster, run workload, collect replica info, distribute recovery plan etc.
But it would succeed if plan can't be created or workload won't be able to write
data after recovery.
Level of success is currently written as a "fake" perf value so that it could
be analyzed retrospectively. That gives as an indication if recovery ever
succeed without a need to break the build every time we stop the cluster in the
state where recovery is impossible.
While not ideal, it could be changed later when we have an appropriate facility
to store such results.
*/
func runRecoverLossOfQuorum(ctx context.Context, t test.Test, c cluster.Cluster, s loqTestSpec) {
	// To debug or analyze recovery failures, enable this option. The test will
	// start failing every time recovery is not successful. That would let you
	// get the logs and plans and check if there was a node panic happening as a
	// result of recovery.
	// Debug state is taken from --debug option, but could be overridden here
	// if we want to stress multiple time to and collect details without keeping
	// clusters running.
	debugFailures := t.IsDebug()

	// Nodes that we plan to keep after simulated failure.
	remaining := []int{1, 4, 5}
	planName := "recover-plan.json"
	pgURL := fmt.Sprintf("{pgurl:1-%d}", c.Spec().NodeCount-1)
	dbName := "test_db"
	workloadHistogramFile := "restored.json"

	settings := install.MakeClusterSettings(install.EnvOption([]string{
		"COCKROACH_MIN_RANGE_MAX_BYTES=1",
	}))
	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.CRDBNodes())

	// Cleanup stale files generated during recovery. We do this for the case
	// where the cluster is reused and cli would refuse to overwrite files
	// blindly.
	args := []string{"rm", "-f", planName}
	for _, node := range remaining {
		args = append(args, fmt.Sprintf("replica-info-%d.json", node))
	}
	c.Run(ctx, option.WithNodes(c.All()), args...)

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	t.L().Printf("started cluster")

	// Lower the dead node detection threshold to make decommissioning nodes
	// faster. 1:30 is currently the smallest allowed value, otherwise we could
	// go as low as possible to improve test time.
	_, err := db.Exec("SET CLUSTER SETTING server.time_until_store_dead = '1m30s'")
	require.NoError(t, err, "failed to set dead store timeout")
	if debugFailures {
		// For debug runs enable statement tracing to have a better visibility of which
		// queries failed.
		_, err = db.Exec("SET CLUSTER SETTING sql.trace.stmt.enable_threshold = '30s'")
	}

	m := c.NewMonitor(ctx, c.CRDBNodes())
	m.Go(func(ctx context.Context) error {
		t.L().Printf("initializing workload")

		c.Run(ctx, option.WithNodes(c.WorkloadNode()), s.wl.initCmd(pgURL, dbName))

		if s.rangeSizeMB > 0 {
			err = setDBRangeLimits(ctx, db, dbName, s.rangeSizeMB*(1<<20))
			require.NoError(t, err, "failed to set range limits configuration")
		}

		// Lower default statement timeout so that we can detect if workload gets
		// stuck. We don't do it earlier because init could have long-running
		// queries.
		_, err = db.Exec("SET CLUSTER SETTING sql.defaults.statement_timeout = '60s'")
		require.NoError(t, err, "failed to set default statement timeout")

		t.L().Printf("running workload")
		c.Run(ctx, option.WithNodes(c.WorkloadNode()), s.wl.runCmd(pgURL, dbName, roachtestutil.IfLocal(c, "10s", "30s"), ""))
		t.L().Printf("workload finished")

		m.ExpectDeaths(int32(c.Spec().NodeCount - 1))
		stopOpts := option.DefaultStopOpts()
		c.Stop(ctx, t.L(), stopOpts, c.CRDBNodes())

		planArguments := ""
		for _, node := range remaining {
			t.L().Printf("collecting replica info from %d", node)
			name := fmt.Sprintf("replica-info-%d.json", node)
			collectCmd := "./cockroach debug recover collect-info --store={store-dir} " + name
			c.Run(ctx, option.WithNodes(c.Nodes(node)), collectCmd)
			if err := c.Get(ctx, t.L(), name, path.Join(t.ArtifactsDir(), name),
				c.Node(node)); err != nil {
				t.Fatalf("failed to collect node replica info %s from node %d: %s", name, node, err)
			}
			c.Put(ctx, path.Join(t.ArtifactsDir(), name), name, c.WorkloadNode())
			planArguments += " " + name
		}
		t.L().Printf("running plan creation")
		planCmd := "./cockroach debug recover make-plan --confirm y -o " + planName + planArguments
		if err = c.RunE(ctx, option.WithNodes(c.WorkloadNode()), planCmd); err != nil {
			t.L().Printf("failed to create plan, test can't proceed assuming unrecoverable cluster: %s",
				err)
			return &recoveryImpossibleError{testOutcome: planCantBeCreated}
		}

		if err := c.Get(ctx, t.L(), planName, path.Join(t.ArtifactsDir(), planName),
			c.WorkloadNode()); err != nil {
			t.Fatalf("failed to collect plan %s from controller node %s: %s", planName, c.WorkloadNode(), err)
		}

		t.L().Printf("distributing and applying recovery plan")
		c.Put(ctx, path.Join(t.ArtifactsDir(), planName), planName, c.Nodes(remaining...))
		applyCommand := "./cockroach debug recover apply-plan --store={store-dir} --confirm y " + planName
		c.Run(ctx, option.WithNodes(c.Nodes(remaining...)), applyCommand)

		// Ignore node failures because they could fail if recovered ranges
		// generate panics. We don't want test to fail in that case, and we
		// rely on query and workload failures to expose that.
		m.ExpectDeaths(int32(len(remaining)))
		settings.Env = append(settings.Env, "COCKROACH_SCAN_INTERVAL=10s")
		c.Start(ctx, t.L(), option.NewStartOpts(option.SkipInit), settings, c.Nodes(remaining...))

		t.L().Printf("waiting for nodes to restart")
		if err = timeutil.RunWithTimeout(ctx, "wait-for-restart", time.Minute,
			func(ctx context.Context) error {
				var err error
				for {
					if ctx.Err() != nil {
						return &recoveryImpossibleError{testOutcome: restartFailed}
					}
					// Note that conn doesn't actually connect, it just creates driver
					// and prepares URL. Actual connection is done when statement is
					// being executed.
					db, err = c.ConnE(ctx, t.L(), 1, option.ConnectTimeout(15*time.Second))
					if err == nil {
						break
					}
					time.Sleep(5 * time.Second)
				}
				// Restoring range limits if they were changed to improve recovery
				// times.
				for {
					if ctx.Err() != nil {
						return &recoveryImpossibleError{testOutcome: restartFailed}
					}
					err = setDBRangeLimits(ctx, db, dbName, 0 /* zero restores default size */)
					if err == nil {
						break
					}
					time.Sleep(5 * time.Second)
				}
				return nil
			}); err != nil {
			return err
		}

		t.L().Printf("mark dead nodes as decommissioned")
		if err := timeutil.RunWithTimeout(ctx, "mark-nodes-decommissioned", 5*time.Minute,
			func(ctx context.Context) error {
				decommissionCmd := fmt.Sprintf(
					"./cockroach node decommission --wait none --url={pgurl:%d} 2 3", 1)
				return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), decommissionCmd)
			}); err != nil {
			// Timeout means we failed to recover ranges especially system ones
			// correctly. We don't wait for all ranges to drain from the nodes to
			// avoid waiting for snapshots which could take 10s of minutes to finish.
			return &recoveryImpossibleError{testOutcome: decommissionFailed}
		}
		t.L().Printf("resuming workload")
		if err = c.RunE(ctx, option.WithNodes(c.WorkloadNode()),
			s.wl.runCmd(
				fmt.Sprintf("{pgurl:1,4-%d}", len(c.CRDBNodes())), dbName, roachtestutil.IfLocal(c, "30s", "3m"),
				workloadHistogramFile)); err != nil {
			return &recoveryImpossibleError{testOutcome: workloadFailed}
		}
		t.L().Printf("workload finished")

		t.L().Printf("ensuring nodes are decommissioned")
		if err := setSnapshotRate(ctx, db, 512); err != nil {
			// Set snapshot executes SQL query against cluster, if query failed then
			// cluster is not healthy after recovery and that means restart failed.
			return &recoveryImpossibleError{testOutcome: restartFailed}
		}
		if err := timeutil.RunWithTimeout(ctx, "decommission-removed-nodes", 5*time.Minute,
			func(ctx context.Context) error {
				decommissionCmd := fmt.Sprintf(
					"./cockroach node decommission --wait all --url={pgurl:%d} 2 3", 1)
				return c.RunE(ctx, option.WithNodes(c.WorkloadNode()), decommissionCmd)
			}); err != nil {
			// Timeout means we failed to drain all ranges from failed nodes, possibly
			// because some ranges were not recovered.
			return &recoveryImpossibleError{testOutcome: decommissionFailed}
		}
		return nil
	})

	testOutcome := success
	if err = m.WaitE(); err != nil {
		testOutcome = restartFailed
		if recErr := (*recoveryImpossibleError)(nil); errors.As(err, &recErr) {
			testOutcome = recErr.testOutcome
		} else {
			t.L().Printf("restart failed with: %s", err)
		}
	}

	recordOutcome, buffer := initPerfCapture(t, c)
	if err := recordOutcome(testOutcome); err != nil {
		t.L().Errorf("failed to record outcome: %s", err)
	}
	buffer.upload(ctx, t, c)

	if testOutcome == success {
		t.L().Printf("recovery succeeded 🍾 \U0001F973")
	} else {
		t.L().Printf("recovery failed with error %s(%d)", outcomeNames[testOutcome], testOutcome)
		if debugFailures && testOutcome > 0 {
			t.Fatalf("test failed with error %s(%d)", outcomeNames[testOutcome], testOutcome)
		}
	}
}

func runHalfOnlineRecoverLossOfQuorum(
	ctx context.Context, t test.Test, c cluster.Cluster, s loqTestSpec,
) {
	// To debug or analyze recovery failures, enable this option. The test will
	// start failing every time recovery is not successful. That would let you
	// get the logs and plans and check if there was a node panic happening as a
	// result of recovery.
	// Debug state is taken from --debug option, but could be overridden here
	// if we want to stress multiple time to and collect details without keeping
	// clusters running.
	debugFailures := t.IsDebug()

	// Nodes that we plan to keep after simulated failure.
	remaining := []int{1, 4, 5}
	killed := []int{2, 3}
	killedNodes := c.Nodes(killed...)
	planName := "recover-plan.json"
	pgURL := fmt.Sprintf("{pgurl:1-%d}", c.Spec().NodeCount-1)
	dbName := "test_db"
	workloadHistogramFile := "restored.json"

	settings := install.MakeClusterSettings(install.EnvOption([]string{
		"COCKROACH_MIN_RANGE_MAX_BYTES=1",
	}))
	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.CRDBNodes())

	// Cleanup stale files generated during recovery. We do this for the case
	// where the cluster is reused and cli would refuse to overwrite files
	// blindly.
	c.Run(ctx, option.WithNodes(c.All()), "rm", "-f", planName)

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	t.L().Printf("started cluster")

	// Lower the dead node detection threshold to make decommissioning nodes
	// faster. 1:30 is currently the smallest allowed value, otherwise we could
	// go as low as possible to improve test time.
	_, err := db.Exec("SET CLUSTER SETTING server.time_until_store_dead = '1m30s'")
	require.NoError(t, err, "failed to set dead store timeout")
	if debugFailures {
		// For debug runs enable statement tracing to have a better visibility of which
		// queries failed.
		_, err = db.Exec("SET CLUSTER SETTING sql.trace.stmt.enable_threshold = '30s'")
	}

	m := c.NewMonitor(ctx, c.CRDBNodes())
	m.Go(func(ctx context.Context) error {
		t.L().Printf("initializing workload")

		c.Run(ctx, option.WithNodes(c.WorkloadNode()), s.wl.initCmd(pgURL, dbName))

		if s.rangeSizeMB > 0 {
			err = setDBRangeLimits(ctx, db, dbName, s.rangeSizeMB*(1<<20))
			require.NoError(t, err, "failed to set range limits configuration")
		}

		// Lower default statement timeout so that we can detect if workload gets
		// stuck. We don't do it earlier because init could have long-running
		// queries.
		_, err = db.Exec("SET CLUSTER SETTING sql.defaults.statement_timeout = '60s'")
		require.NoError(t, err, "failed to set default statement timeout")

		t.L().Printf("running workload")
		c.Run(ctx, option.WithNodes(c.WorkloadNode()), s.wl.runCmd(pgURL, dbName, roachtestutil.IfLocal(c, "10s", "30s"), ""))
		t.L().Printf("workload finished")

		m.ExpectDeaths(int32(len(killed)))
		stopOpts := option.DefaultStopOpts()
		c.Stop(ctx, t.L(), stopOpts, killedNodes)

		t.L().Printf("running plan creation")
		addrs, err := c.ExternalAddr(ctx, t.L(), c.Node(1))
		require.NoError(t, err, "infra failure, can't get IP addr of cluster node")
		require.NotEmpty(t, addrs, "infra failure, can't get IP addr of cluster node")
		addr := addrs[0]
		planCmd := "./cockroach debug recover make-plan --confirm y --host " + addr + " -o " + planName

		if err = c.RunE(ctx, option.WithNodes(c.WorkloadNode()), planCmd); err != nil {
			t.L().Printf("failed to create plan, test can't proceed assuming unrecoverable cluster: %s",
				err)
			return &recoveryImpossibleError{testOutcome: planCantBeCreated}
		}

		if err := c.Get(ctx, t.L(), planName, path.Join(t.ArtifactsDir(), planName),
			c.WorkloadNode()); err != nil {
			t.Fatalf("failed to collect plan %s from controller node %s: %s", planName, c.WorkloadNode(), err)
		}

		t.L().Printf("staging recovery plan")
		applyCommand := "./cockroach debug recover apply-plan --confirm y --host " + addr + " " + planName
		c.Run(ctx, option.WithNodes(c.WorkloadNode()), applyCommand)

		// Ignore node failures because they could fail if recovered ranges
		// generate panics. We don't want test to fail in that case, and we
		// rely on query and workload failures to expose that.
		m.ExpectDeaths(int32(len(remaining)))
		settings.Env = append(settings.Env, "COCKROACH_SCAN_INTERVAL=10s")

		t.L().Printf("performing rolling restart of surviving nodes")
		for _, id := range remaining {
			c.Stop(ctx, t.L(), stopOpts, c.Node(id))
			c.Start(ctx, t.L(), option.NewStartOpts(option.SkipInit), settings, c.Node(id))
		}

		t.L().Printf("waiting for nodes to process recovery")
		verifyCommand := "./cockroach debug recover verify --host " + addr + " " + planName
		if err = timeutil.RunWithTimeout(ctx, "wait-for-restart", 2*time.Minute,
			func(ctx context.Context) error {
				for {
					res, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.WorkloadNode()), verifyCommand)
					if res.RemoteExitStatus == 0 {
						if ctx.Err() != nil {
							return &recoveryImpossibleError{testOutcome: restartFailed}
						}
						if err != nil {
							// No exit code and error means roachprod failure (ssh etc).
							return err
						}
						break
					}
					t.L().Printf("recovery is not finished: %s", res.Stderr)
					time.Sleep(10 * time.Second)
				}
				var err error
				for {
					if ctx.Err() != nil {
						return &recoveryImpossibleError{testOutcome: restartFailed}
					}
					db, err = c.ConnE(ctx, t.L(), remaining[len(remaining)-1], option.ConnectTimeout(15*time.Second))
					if err == nil {
						break
					}
					time.Sleep(5 * time.Second)
				}
				// Restoring range limits if they were changed to improve recovery
				// times.
				for {
					if ctx.Err() != nil {
						return &recoveryImpossibleError{testOutcome: restartFailed}
					}
					err = setDBRangeLimits(ctx, db, dbName, 0 /* zero restores default size */)
					if err == nil {
						break
					}
					time.Sleep(5 * time.Second)
				}
				return nil
			}); err != nil {
			return err
		}

		t.L().Printf("resuming workload")
		if err = c.RunE(ctx, option.WithNodes(c.WorkloadNode()),
			s.wl.runCmd(
				fmt.Sprintf("{pgurl:1,4-%d}", len(c.CRDBNodes())), dbName, roachtestutil.IfLocal(c, "30s", "3m"),
				workloadHistogramFile)); err != nil {
			return &recoveryImpossibleError{testOutcome: workloadFailed}
		}
		t.L().Printf("workload finished")

		t.L().Printf("ensuring nodes are decommissioned")
		if err := setSnapshotRate(ctx, db, 512); err != nil {
			// Set snapshot executes SQL query against cluster, if query failed then
			// cluster is not healthy after recovery and that means restart failed.
			return &recoveryImpossibleError{testOutcome: restartFailed}
		}
		// In half online mode, nodes will update dead nodes' status upon
		// restart. Check that it actually happened. We also need to have retry
		// since decommission is done in the background with retries.
		if err = timeutil.RunWithTimeout(ctx, "wait-for-decommission", 5*time.Minute,
			func(ctx context.Context) error {
				// Keep trying to query until either we get no rows (all nodes are
				// decommissioned or removed) or task times out. In timeout case, test
				// error will be a cause for timeout thus exposing error code.
				res := workloadFailed
				for ; ; time.Sleep(5 * time.Second) {
					rows, err := db.QueryContext(
						ctx,
						"select node_id from crdb_internal.kv_node_liveness where node_id in ($1, $2) and membership = 'active'",
						killed[0],
						killed[1])
					if ctx.Err() != nil {
						return &recoveryImpossibleError{testOutcome: res}
					}
					if err != nil {
						continue
					}
					if rows.Next() {
						res = decommissionFailed
						continue
					}
					return nil
				}
			}); err != nil {
			return err
		}

		return nil
	})

	testOutcome := success
	if err = m.WaitE(); err != nil {
		testOutcome = restartFailed
		if recErr := (*recoveryImpossibleError)(nil); errors.As(err, &recErr) {
			testOutcome = recErr.testOutcome
		} else {
			t.L().Printf("restart failed with: %s", err)
		}
	}

	recordOutcome, buffer := initPerfCapture(t, c)
	if err = recordOutcome(testOutcome); err != nil {
		t.L().Errorf("failed to record outcome: %s", err)
	}
	buffer.upload(ctx, t, c)

	if testOutcome == success {
		t.L().Printf("recovery succeeded 🍾 \U0001F973")
	} else {
		t.L().Printf("recovery failed with error %s(%d)", outcomeNames[testOutcome], testOutcome)
		if debugFailures && testOutcome > 0 {
			t.Fatalf("test failed with error %s(%d)", outcomeNames[testOutcome], testOutcome)
		}
	}
}

func setDBRangeLimits(ctx context.Context, db *gosql.DB, dbName string, size int64) error {
	query := fmt.Sprintf("ALTER DATABASE %s CONFIGURE ZONE USING range_max_bytes=%d, range_min_bytes=1024",
		dbName, size)
	if size == 0 {
		query = fmt.Sprintf("ALTER DATABASE %s CONFIGURE ZONE USING range_max_bytes = COPY FROM PARENT, range_min_bytes = COPY FROM PARENT",
			dbName)
	}
	_, err := db.ExecContext(ctx, query)
	return err
}

func setSnapshotRate(ctx context.Context, db *gosql.DB, sizeMB int64) error {
	queries := []string{
		"RESET CLUSTER SETTING kv.snapshot_rebalance.max_rate",
	}
	if sizeMB > 0 {
		queries = []string{
			fmt.Sprintf("SET CLUSTER SETTING kv.snapshot_rebalance.max_rate = '%dMiB'", sizeMB),
		}
	}
	for _, query := range queries {
		_, err := db.ExecContext(ctx, query)
		if err != nil {
			return err
		}
	}
	return nil
}

type perfArtifact bytes.Buffer

// upload puts generated perf artifact on first node so that it could be
// collected by CI infrastructure automagically.
func (p *perfArtifact) upload(ctx context.Context, t test.Test, c cluster.Cluster) {
	// Upload the perf artifacts to any one of the nodes so that the test
	// runner copies it into an appropriate directory path.
	dest := filepath.Join(t.PerfArtifactsDir(), roachtestutil.GetBenchmarkMetricsFileName(t))
	if err := c.RunE(ctx, option.WithNodes(c.Node(1)), "mkdir -p "+filepath.Dir(dest)); err != nil {
		t.L().Errorf("failed to create perf dir: %+v", err)
	}
	if err := c.PutString(ctx, (*bytes.Buffer)(p).String(), dest, 0755, c.Node(1)); err != nil {
		t.L().Errorf("failed to upload perf artifacts to node: %s", err.Error())
	}
}

// Register histogram and create a function that would record test outcome value.
// Returned buffer contains all recorded ticks for the test and is updated
// every time metric function is called.
func initPerfCapture(
	t test.Test, c cluster.Cluster,
) (func(testOutcomeMetric) error, *perfArtifact) {
	exporter := roachtestutil.CreateWorkloadHistogramExporter(t, c)
	reg := histogram.NewRegistryWithExporter(time.Second*time.Duration(success), histogram.MockWorkloadName, exporter)
	bytesBuf := bytes.NewBuffer([]byte{})
	writer := io.Writer(bytesBuf)
	exporter.Init(&writer)

	writeSnapshot := func() {
		reg.Tick(func(tick histogram.Tick) {
			_ = tick.Exporter.SnapshotAndWrite(tick.Hist, tick.Now, tick.Elapsed, &tick.Name)
		})
	}

	recordOutcome := func(metric testOutcomeMetric) error {
		reg.GetHandle().Get("recovery_result").Record(time.Duration(metric) * time.Second)
		writeSnapshot()
		return exporter.Close(nil)
	}

	// Capture start time for the test.
	writeSnapshot()

	return recordOutcome, (*perfArtifact)(bytesBuf)
}

type workload interface {
	initCmd(pgURL string, db string) string
	runCmd(pgURL string, db string, duration string, histFile string) string
}

type movrLoqWorkload struct {
	concurrency int
}

func (movrLoqWorkload) initCmd(pgURL string, db string) string {
	return fmt.Sprintf(`./cockroach workload init movr %s --data-loader import --db %s`, pgURL, db)
}

func (w movrLoqWorkload) runCmd(pgURL string, db string, duration string, histFile string) string {
	if len(histFile) == 0 {
		return fmt.Sprintf(
			`./cockroach workload run movr %s --db %s --duration %s`,
			pgURL, db, duration)
	}
	return fmt.Sprintf(
		`./cockroach workload run movr %s --db %s --histograms %s --duration %s --concurrency %d`,
		pgURL, db, histFile, duration, w.concurrency)
}

func (w movrLoqWorkload) String() string {
	return "movr"
}

type tpccLoqWorkload struct {
	warehouses  int
	concurrency int
}

func (w tpccLoqWorkload) initCmd(pgURL string, db string) string {
	return fmt.Sprintf(`./cockroach workload init tpcc %s --data-loader import --db %s --warehouses %d`,
		pgURL, db,
		w.warehouses)
}

func (w tpccLoqWorkload) runCmd(pgURL string, db string, duration string, histFile string) string {
	if len(histFile) == 0 {
		return fmt.Sprintf(
			`./cockroach workload run tpcc %s --db %s --duration %s --warehouses %d --concurrency %d`,
			pgURL, db, duration, w.warehouses, w.concurrency)
	}
	return fmt.Sprintf(
		`./cockroach workload run tpcc %s --db %s --histograms %s --duration %s --warehouses %d --concurrency %d`,
		pgURL, db, histFile, duration, w.warehouses, w.concurrency)
}

func (w tpccLoqWorkload) String() string {
	return "tpcc"
}
