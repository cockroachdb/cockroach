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
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
)

func registerLOQRecovery(r registry.Registry) {
	spec := r.MakeClusterSpec(6)
	r.Add(registry.TestSpec{
		Name:              "loqrecovery/upreplicate=true",
		Owner:             registry.OwnerKV,
		Tags:              []string{`default`},
		Cluster:           spec,
		NonReleaseBlocker: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			recoverLossOfQuorum(ctx, t, c, true)
		},
	})
	r.Add(registry.TestSpec{
		Name:              "loqrecovery/upreplicate=false",
		Owner:             registry.OwnerKV,
		Tags:              []string{`default`},
		Cluster:           spec,
		NonReleaseBlocker: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			recoverLossOfQuorum(ctx, t, c, false)
		},
	})
}

type testOutcomeMetric int

const (
	// Nodes fail to start after recovery was performed.
	restartFailed testOutcomeMetric = iota
	// Nodes restarted but decommission didn't work.
	decommissionFailed
	// Nodes started, but workload failed to succeed on any writes after restart.
	workloadFailed
	// Nodes restarted and workload produced some writes.
	success
)

func recoverLossOfQuorum(
	ctx context.Context, t test.Test, c cluster.Cluster, waitForUpreplicate bool,
) {
	nodes := c.Spec().NodeCount - 1
	controller := c.Spec().NodeCount
	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
	startOpts := option.DefaultStartOpts()
	settings := install.MakeClusterSettings()
	c.Start(ctx, t.L(), startOpts, settings, c.Range(1, nodes))

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	t.L().Printf("Started cluster")

	// Lower the dead node detection threshold to make decommissioning nodes
	// faster.
	_, err := db.Exec("SET CLUSTER SETTING server.time_until_store_dead = '1m30s'")
	if err != nil {
		t.Fatalf("Failed to set dead timeout to 1m: %s", err)
	}

	m := c.NewMonitor(ctx, c.Range(1, nodes))
	// Preload cluster with data
	m.Go(func(ctx context.Context) error {
		t.L().Printf("Initializing workload")

		workloadInitCmd := fmt.Sprintf(`./cockroach workload init movr {pgurl:1-%d}`, nodes)
		c.Run(ctx, c.Node(controller), workloadInitCmd)

		t.L().Printf("Running workload")

		duration := "--duration=" + ifLocal(c, "10s", "30s")
		workloadRunCmd := fmt.Sprintf(
			`./cockroach workload run movr {pgurl:1-%d} --tolerate-errors %s`,
			nodes, duration)
		c.Run(ctx, c.Node(controller), workloadRunCmd)

		t.L().Printf("Workload finished")
		return nil
	})
	m.Wait()

	recordOutcome, buffer := initPerfCapture()

	if waitForUpreplicate {
		t.L().Printf("Waiting for up-replication.")
		for {
			var fullyReplicated bool
			if err := db.QueryRow(
				// Check if all ranges are replicated enough.
				"SELECT min(array_length(replicas, 1)) >= 3 FROM crdb_internal.ranges",
			).Scan(&fullyReplicated); err != nil {
				t.Fatal(err)
			}
			if fullyReplicated {
				break
			}
			time.Sleep(time.Second)
		}
	}

	// Perform recovery
	stopOpts := option.DefaultStopOpts()
	c.Stop(ctx, t.L(), stopOpts, c.Range(1, nodes))

	//removed := []int{2, 3}
	remaining := []int{1, 4, 5}
	planArguments := ""
	for _, node := range remaining {
		t.L().Printf("Collecting replica info from %d", node)
		name := fmt.Sprintf("replica-info-%d.json", node)
		collectCmd := "./cockroach debug recover collect-info --store={store-dir} " + name
		c.Run(ctx, c.Nodes(node), collectCmd)
		if err := c.Get(ctx, t.L(), name, name, c.Node(node)); err != nil {
			t.Errorf("failed to collect node replica info %s from node %d: %s", name, node, err)
		}
		c.Put(ctx, name, name, c.Nodes(controller))
		planArguments += " " + name
	}
	t.L().Printf("Running plan creation.")
	planName := "recover-plan.json"
	// We don't use force (--force) because resulting config would break cluster.
	planCmd := "./cockroach debug recover make-plan --confirm y -o " + planName + planArguments
	c.Run(ctx, c.Node(controller), planCmd)
	if err := c.Get(ctx, t.L(), planName, planName, c.Node(controller)); err != nil {
		t.Errorf("failed to collect plan %s from controller node %d: %s", planName, controller, err)
	}

	t.L().Printf("Distributing and applying recovery plan.")
	c.Put(ctx, planName, planName, c.Nodes(remaining...))
	applyCommand := "./cockroach debug recover apply-plan --store={store-dir} --confirm y " + planName
	c.Run(ctx, c.Nodes(remaining...), applyCommand)

	// We track recovered state explicitly to prevent test from failing regardless
	// of the outcome. Since recovery is not guaranteed in all situations, we
	// publish success into perf instead to get a feel how well recovery performs
	// in practice.
	// This variable contains error for the next test operation. If test stops
	// before this operation finishes, this value would be recorded as outcome.
	// We do it because test steps could break in multiple places, and we don't
	// need fine-grained error reporting, so we set error code preemptively once.
	testOutcome := restartFailed

	// Restart cluster and continue workload
	startOpts.RoachprodOpts.SkipInit = true
	c.Start(ctx, t.L(), startOpts, settings, c.Nodes(remaining...))

	m = c.NewMonitor(ctx, c.Nodes(remaining...))
	m.Go(func(ctx context.Context) error {
		// Check for node 1 to come back
		t.L().Printf("Waiting for nodes to restart.")
		if err := testutils.SucceedsSoonError(func() error {
			db, err := c.ConnE(ctx, t.L(), 1)
			if err != nil {
				return err
			}
			_, err = db.Exec("select 1")
			return err
		}); err != nil {
			//nolint:returnerrcheck
			return nil
		}

		testOutcome = decommissionFailed
		t.L().Printf("Decommissioning dead nodes.")
		if err := contextutil.RunWithTimeout(ctx, "decom", time.Minute*5,
			func(ctx context.Context) error {
				decommissionCmd := fmt.Sprintf(
					"./cockroach node decommission --wait none --insecure --url={pgurl:%d} 2 3", 1)
				return c.RunE(ctx, c.Node(controller), decommissionCmd)
			}); err != nil {
			// Timeout means we failed to recover ranges especially system ones
			// correctly or completely.
			//nolint:returnerrcheck
			return nil
		}

		testOutcome = workloadFailed
		t.L().Printf("Resuming workload.")
		histFile := "restored.json"
		duration := "--duration=" + ifLocal(c, "10s", "30s")
		workloadRunCmd := fmt.Sprintf(
			`./cockroach workload run movr {pgurl:1,4-%d} --tolerate-errors --histograms %s %s`,
			nodes, histFile, duration)
		c.Run(ctx, c.Node(controller), workloadRunCmd)
		t.L().Printf("Workload finished")

		// Check if any workload histogram contains a non-zero value. That means
		// some writes succeed after recovery and we treat is as a proxy for
		// some success.
		if err := c.Get(ctx, t.L(), histFile, histFile, c.Node(controller)); err != nil {
			t.L().Printf("failed to retrieve workload histogram file %s from node %d", histFile,
				controller)
			//nolint:returnerrcheck
			return nil
		}
		sMap, err := histogram.DecodeSnapshots(histFile)
		if err != nil {
			t.L().Printf("failed to parse histograms from workload")
			//nolint:returnerrcheck
			return nil
		}
		for _, ticks := range sMap {
			for _, tick := range ticks {
				for _, count := range tick.Hist.Counts {
					if count > 0 {
						testOutcome = success
						return nil
					}
				}
			}
		}
		return nil
	})
	m.Wait()

	recordOutcome(testOutcome)
	buffer.Upload(ctx, t, c)
	if testOutcome == success {
		t.L().Printf("Recovery succeeded. Pop champagne!")
	} else {
		t.L().Printf("Recovery failed.")
	}
}

type perfArtifact bytes.Buffer

func (p *perfArtifact) Upload(ctx context.Context, t test.Test, c cluster.Cluster) {
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
