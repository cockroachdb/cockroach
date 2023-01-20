// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"path"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
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

func (s loqTestSpec) String() string {
	sizeName := "default"
	if s.rangeSizeMB > 0 {
		sizeName = fmt.Sprintf("%dmb", s.rangeSizeMB)
	}
	return fmt.Sprintf("loqrecovery/workload=%s/rangeSize=%s", s.wl, sizeName)
}

func registerLOQRecovery(r registry.Registry) {
	spec := r.MakeClusterSpec(6)
	for _, s := range []loqTestSpec{
		{wl: movrLoqWorkload{concurrency: 32}, rangeSizeMB: 2},
		{wl: movrLoqWorkload{concurrency: 32}},
		{wl: tpccLoqWorkload{warehouses: 100, concurrency: 32}, rangeSizeMB: 16},
		{wl: tpccLoqWorkload{warehouses: 100, concurrency: 32}},
	} {
		testSpec := s
		r.Add(registry.TestSpec{
			Name:              s.String(),
			Owner:             registry.OwnerReplication,
			Tags:              []string{`default`},
			Cluster:           spec,
			NonReleaseBlocker: true,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runRecoverLossOfQuorum(ctx, t, c, testSpec)
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

	// Number of cockroach cluster nodes.
	maxNode := c.Spec().NodeCount - 1
	nodes := c.Range(1, maxNode)
	// Controller node runs offline procedures and workload.
	controller := c.Spec().NodeCount
	// Nodes that we plan to keep after simulated failure.
	remaining := []int{1, 4, 5}
	planName := "recover-plan.json"
	pgURL := fmt.Sprintf("{pgurl:1-%d}", c.Spec().NodeCount-1)
	dbName := "test_db"
	workloadHistogramFile := "restored.json"

	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
	startOpts := option.DefaultStartOpts()
	settings := install.MakeClusterSettings()
	c.Start(ctx, t.L(), startOpts, settings, nodes)

	// Cleanup stale files generated during recovery. We do this for the case
	// where the cluster is reused and cli would refuse to overwrite files
	// blindly.
	args := []string{"rm", "-f", planName}
	for _, node := range remaining {
		args = append(args, fmt.Sprintf("replica-info-%d.json", node))
	}
	c.Run(ctx, c.All(), args...)

	db := c.Conn(ctx, t.L(), 1, "")
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

	m := c.NewMonitor(ctx, nodes)
	m.Go(func(ctx context.Context) error {
		t.L().Printf("initializing workload")

		c.Run(ctx, c.Node(controller), s.wl.initCmd(pgURL, dbName))

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
		c.Run(ctx, c.Node(controller), s.wl.runCmd(pgURL, dbName, ifLocal(c, "10s", "30s"), ""))
		t.L().Printf("workload finished")

		m.ExpectDeaths(int32(c.Spec().NodeCount - 1))
		stopOpts := option.DefaultStopOpts()
		c.Stop(ctx, t.L(), stopOpts, nodes)

		planArguments := ""
		for _, node := range remaining {
			t.L().Printf("collecting replica info from %d", node)
			name := fmt.Sprintf("replica-info-%d.json", node)
			collectCmd := "./cockroach debug recover collect-info --store={store-dir} " + name
			c.Run(ctx, c.Nodes(node), collectCmd)
			if err := c.Get(ctx, t.L(), name, path.Join(t.ArtifactsDir(), name),
				c.Node(node)); err != nil {
				t.Fatalf("failed to collect node replica info %s from node %d: %s", name, node, err)
			}
			c.Put(ctx, path.Join(t.ArtifactsDir(), name), name, c.Nodes(controller))
			planArguments += " " + name
		}
		t.L().Printf("running plan creation")
		planCmd := "./cockroach debug recover make-plan --confirm y -o " + planName + planArguments
		if err = c.RunE(ctx, c.Node(controller), planCmd); err != nil {
			t.L().Printf("failed to create plan, test can't proceed assuming unrecoverable cluster: %s",
				err)
			return &recoveryImpossibleError{testOutcome: planCantBeCreated}
		}

		if err := c.Get(ctx, t.L(), planName, path.Join(t.ArtifactsDir(), planName),
			c.Node(controller)); err != nil {
			t.Fatalf("failed to collect plan %s from controller node %d: %s", planName, controller, err)
		}

		t.L().Printf("distributing and applying recovery plan")
		c.Put(ctx, path.Join(t.ArtifactsDir(), planName), planName, c.Nodes(remaining...))
		applyCommand := "./cockroach debug recover apply-plan --store={store-dir} --confirm y " + planName
		c.Run(ctx, c.Nodes(remaining...), applyCommand)

		startOpts.RoachprodOpts.SkipInit = true
		// Ignore node failures because they could fail if recovered ranges
		// generate panics. We don't want test to fail in that case, and we
		// rely on query and workload failures to expose that.
		m.ExpectDeaths(int32(len(remaining)))
		settings.Env = append(settings.Env, "COCKROACH_SCAN_INTERVAL=10s")
		c.Start(ctx, t.L(), startOpts, settings, c.Nodes(remaining...))

		t.L().Printf("waiting for nodes to restart")
		if err = contextutil.RunWithTimeout(ctx, "wait-for-restart", time.Minute,
			func(ctx context.Context) error {
				var err error
				for {
					if ctx.Err() != nil {
						return &recoveryImpossibleError{testOutcome: restartFailed}
					}
					db, err = c.ConnE(ctx, t.L(), 1, "")
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
		if err := contextutil.RunWithTimeout(ctx, "mark-nodes-decommissioned", 5*time.Minute,
			func(ctx context.Context) error {
				decommissionCmd := fmt.Sprintf(
					"./cockroach node decommission --wait none --insecure --url={pgurl:%d} 2 3", 1)
				return c.RunE(ctx, c.Node(controller), decommissionCmd)
			}); err != nil {
			// Timeout means we failed to recover ranges especially system ones
			// correctly. We don't wait for all ranges to drain from the nodes to
			// avoid waiting for snapshots which could take 10s of minutes to finish.
			return &recoveryImpossibleError{testOutcome: decommissionFailed}
		}
		t.L().Printf("resuming workload")
		if err = c.RunE(ctx, c.Node(controller),
			s.wl.runCmd(
				fmt.Sprintf("{pgurl:1,4-%d}", maxNode), dbName, ifLocal(c, "30s", "3m"),
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
		if err := contextutil.RunWithTimeout(ctx, "decommission-removed-nodes", 5*time.Minute,
			func(ctx context.Context) error {
				decommissionCmd := fmt.Sprintf(
					"./cockroach node decommission --wait all --insecure --url={pgurl:%d} 2 3", 1)
				return c.RunE(ctx, c.Node(controller), decommissionCmd)
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

	recordOutcome, buffer := initPerfCapture()
	recordOutcome(testOutcome)
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
		"RESET CLUSTER SETTING kv.snapshot_recovery.max_rate",
	}
	if sizeMB > 0 {
		queries = []string{
			fmt.Sprintf("SET CLUSTER SETTING kv.snapshot_rebalance.max_rate = '%dMiB'", sizeMB),
			fmt.Sprintf("SET CLUSTER SETTING kv.snapshot_recovery.max_rate = '%dMiB'", sizeMB),
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
	dest := filepath.Join(t.PerfArtifactsDir(), "stats.json")
	if err := c.RunE(ctx, c.Node(1), "mkdir -p "+filepath.Dir(dest)); err != nil {
		t.L().Errorf("failed to create perf dir: %+v", err)
	}
	if err := c.PutString(ctx, (*bytes.Buffer)(p).String(), dest, 0755, c.Node(1)); err != nil {
		t.L().Errorf("failed to upload perf artifacts to node: %s", err.Error())
	}
}

// Register histogram and create a function that would record test outcome value.
// Returned buffer contains all recorded ticks for the test and is updated
// every time metric function is called.
func initPerfCapture() (func(testOutcomeMetric), *perfArtifact) {
	reg := histogram.NewRegistry(time.Second*time.Duration(success), histogram.MockWorkloadName)
	bytesBuf := bytes.NewBuffer([]byte{})
	jsonEnc := json.NewEncoder(bytesBuf)

	writeSnapshot := func() {
		reg.Tick(func(tick histogram.Tick) {
			_ = jsonEnc.Encode(tick.Snapshot())
		})
	}

	recordOutcome := func(metric testOutcomeMetric) {
		reg.GetHandle().Get("recovery_result").Record(time.Duration(metric) * time.Second)
		writeSnapshot()
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
