// Copyright 2020 The Cockroach Authors.
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
	"time"
)

type backgroundFn func(ctx context.Context, u *versionUpgradeTest) error

// A backgroundStepper is a tool to run long-lived commands while a cluster is
// going through a sequence of version upgrade operations.
// It exposes a `launch` step that launches the method carrying out long-running
// work (in the background) and a `stop` step collecting any errors.
type backgroundStepper struct {
	// This is the operation that will be launched in the background. When the
	// context gets cancelled, it should shut down and return without an error.
	// The way to typically get this is:
	//
	//  err := doSomething(ctx)
	//  ctx.Err() != nil {
	//    return nil
	//  }
	//  return err
	run backgroundFn

	// Internal.
	m      *monitor
	cancel func()
}

func makeBackgroundStepper(run backgroundFn) backgroundStepper {
	return backgroundStepper{run: run}
}

// launch spawns the function the background step was initialized with.
func (s *backgroundStepper) launch(ctx context.Context, _ *test, u *versionUpgradeTest) {
	s.m = newMonitor(ctx, u.c)
	ctx, s.m.cancel = context.WithCancel(ctx)
	s.m.Go(func(ctx context.Context) error {
		return s.run(ctx, u)
	})
}

func (s *backgroundStepper) stop(_ context.Context, t *test, _ *versionUpgradeTest) {
	s.m.cancel()
	if err := s.m.WaitE(); err != nil {
		t.Fatal(err)
	}
}

func backgroundTPCCWorkload(warehouses int, tpccDB string) backgroundStepper {
	return makeBackgroundStepper(func(ctx context.Context, u *versionUpgradeTest) error {
		cmd := []string{
			"./workload fixtures import tpcc",
			fmt.Sprintf("--warehouses=%d", warehouses),
			fmt.Sprintf("--db=%s", tpccDB),
		}
		// The last node is the load node.
		err := u.c.RunE(ctx, u.c.Node(u.c.spec.NodeCount), cmd...)
		if ctx.Err() != nil {
			// If the context is cancelled, that's probably why the workload  returned
			// so swallow error. (This is how the harness tells us to shut down the
			// workload).
			return nil
		}
		return err
	})
}

func pauseAllJobsStep() versionStep {
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		db := u.conn(ctx, t, 1)
		_, err := db.ExecContext(
			ctx,
			`PAUSE JOBS (SELECT job_id FROM [SHOW JOBS] WHERE status = 'running');`,
		)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func makeResumeAllJobsAndWaitStep(d time.Duration) versionStep {
	var numResumes int
	return func(ctx context.Context, t *test, u *versionUpgradeTest) {
		numResumes++
		t.l.Printf("Resume all jobs number: %d", numResumes)
		db := u.conn(ctx, t, 1)
		_, err := db.ExecContext(
			ctx,
			`RESUME JOBS (SELECT job_id FROM [SHOW JOBS] WHERE status = 'paused');`,
		)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(d)
	}
}

func runJobsMixedVersions(
	ctx context.Context, t *test, c *cluster, warehouses int, predecessorVersion string,
) {
	// An empty string means that the cockroach binary specified by flag
	// `cockroach` will be used.
	const mainVersion = ""
	roachNodes := c.Range(1, c.spec.NodeCount-1)
	backGroundTPCC := backgroundTPCCWorkload(warehouses, "tpcc-pred")
	resumeAllJobsAndWaitStep := makeResumeAllJobsAndWaitStep(10 * time.Second)
	c.Put(ctx, workload, "./workload", c.Node(c.spec.NodeCount))

	u := newVersionUpgradeTest(c,
		// We use the last node for running workloads.
		uploadAndStartFromCheckpointFixture(roachNodes, predecessorVersion),
		waitForUpgradeStep(roachNodes),
		preventAutoUpgradeStep(1),

		backGroundTPCC.launch,
		func(ctx context.Context, _ *test, u *versionUpgradeTest) {
			time.Sleep(10 * time.Second)
		},
		pauseAllJobsStep(),

		// Roll the nodes into the new version one by one, while repeatedly pausing
		// and resuming all jobs.
		binaryUpgradeStep(c.Node(3), mainVersion),
		resumeAllJobsAndWaitStep,
		backGroundTPCC.stop,
		pauseAllJobsStep(),

		binaryUpgradeStep(c.Node(2), mainVersion),
		resumeAllJobsAndWaitStep,
		pauseAllJobsStep(),

		binaryUpgradeStep(c.Node(1), mainVersion),
		resumeAllJobsAndWaitStep,
		pauseAllJobsStep(),

		binaryUpgradeStep(c.Node(4), mainVersion),
		resumeAllJobsAndWaitStep,
		pauseAllJobsStep(),

		// Roll back again, which ought to be fine because the cluster upgrade was
		// not finalized.
		binaryUpgradeStep(c.Node(2), predecessorVersion),
		resumeAllJobsAndWaitStep,
		pauseAllJobsStep(),

		binaryUpgradeStep(c.Node(4), predecessorVersion),
		resumeAllJobsAndWaitStep,
		pauseAllJobsStep(),

		binaryUpgradeStep(c.Node(3), predecessorVersion),
		resumeAllJobsAndWaitStep,
		pauseAllJobsStep(),

		binaryUpgradeStep(c.Node(1), predecessorVersion),
		resumeAllJobsAndWaitStep,
		pauseAllJobsStep(),

		// Roll nodes forward and finalize upgrade.
		binaryUpgradeStep(c.Node(4), mainVersion),
		resumeAllJobsAndWaitStep,
		pauseAllJobsStep(),

		binaryUpgradeStep(c.Node(3), mainVersion),
		resumeAllJobsAndWaitStep,
		pauseAllJobsStep(),

		binaryUpgradeStep(c.Node(1), mainVersion),
		resumeAllJobsAndWaitStep,
		pauseAllJobsStep(),

		binaryUpgradeStep(c.Node(2), mainVersion),
		resumeAllJobsAndWaitStep,
		pauseAllJobsStep(),

		allowAutoUpgradeStep(1),
		waitForUpgradeStep(roachNodes),
		resumeAllJobsAndWaitStep,
	)

	u.run(ctx, t)
}

func registerJobsMixedVersions(r *testRegistry) {
	r.Add(testSpec{
		Name:  "jobs/mixed-versions",
		Owner: OwnerBulkIO,
		// Use a 5 node cluster -- 5 nodes will run cockroach, and the last will be
		// the workload driver node.
		Cluster: makeClusterSpec(5),
		Run: func(ctx context.Context, t *test, c *cluster) {
			predV, err := PredecessorVersion(r.buildVersion)
			if err != nil {
				t.Fatal(err)
			}
			warehouses := 500
			if local {
				warehouses = 50
			}
			runJobsMixedVersions(ctx, t, c, warehouses, predV)
		},
	})
}
