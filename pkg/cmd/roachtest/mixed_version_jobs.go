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

	"github.com/cockroachdb/cockroach/pkg/jobs"
)

type backgroundFn func(ctx context.Context, u *versionUpgradeTest) error

// A backgroundStepper is a tool to run long-lived commands while a cluster is
// going through a sequence of version upgrade operations.
// It exposes a `launch` step that launches the method carrying out long-running
// work (in the background) and a `stop` step collecting any errors.
type backgroundStepper struct {
	// This is the operation that will be launched in the background. When the
	// context gets canceled, it should shut down and return without an error.
	// The way to typically get this is:
	//
	//  err := doSomething(ctx)
	//  ctx.Err() != nil {
	//    return nil
	//  }
	//  return err
	run backgroundFn

	// Internal.
	m *monitor
}

func makeBackgroundStepper(run backgroundFn) backgroundStepper {
	return backgroundStepper{run: run}
}

// launch spawns the function the background step was initialized with.
func (s *backgroundStepper) launch(ctx context.Context, _ *test, u *versionUpgradeTest) {
	s.m = newMonitor(ctx, u.c)
	_, s.m.cancel = context.WithCancel(ctx)
	s.m.Go(func(ctx context.Context) error {
		return s.run(ctx, u)
	})
}

func (s *backgroundStepper) stop(ctx context.Context, t *test, u *versionUpgradeTest) {
	s.m.cancel()
	// We don't care about the workload failing since we only use it to produce a
	// few `IMPORT` jobs. And indeed workload will fail because it does not
	// tolerate pausing of its jobs.
	_ = s.m.WaitE()
	db := u.conn(ctx, t, 1)
	t.l.Printf("Resuming any paused jobs left")
	for {
		_, err := db.ExecContext(
			ctx,
			`RESUME JOBS (SELECT job_id FROM [SHOW JOBS] WHERE status = $1);`,
			jobs.StatusPaused,
		)
		if err != nil {
			t.Fatal(err)
		}
		row := db.QueryRow(
			"SELECT count(*) FROM [SHOW JOBS] WHERE status = $1",
			jobs.StatusPauseRequested,
		)
		var nNotYetPaused int
		if err = row.Scan(&nNotYetPaused); err != nil {
			t.Fatal(err)
		}
		if nNotYetPaused <= 0 {
			break
		}
		// Sleep a bit not to DOS the jobs table.
		time.Sleep(10 * time.Second)
		t.l.Printf("Waiting for %d jobs to pause", nNotYetPaused)
	}

	t.l.Printf("Waiting for jobs to complete...")
	q := "SHOW JOBS WHEN COMPLETE (SELECT job_id FROM [SHOW JOBS]);"
	_, err := db.ExecContext(ctx, q)
	if err != nil {
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
		// The workload has to run on one of the nodes of the cluster.
		err := u.c.RunE(ctx, u.c.Node(1), cmd...)
		if ctx.Err() != nil {
			// If the context is canceled, that's probably why the workload  returned
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
			`PAUSE JOBS (SELECT job_id FROM [SHOW JOBS] WHERE status = $1);`,
			jobs.StatusRunning,
		)
		if err != nil {
			t.Fatal(err)
		}

		row := db.QueryRow("SELECT count(*) FROM [SHOW JOBS] WHERE status LIKE 'pause%'")
		var nPaused int
		if err := row.Scan(&nPaused); err != nil {
			t.Fatal(err)
		}
		t.l.Printf("Paused %d jobs", nPaused)
		time.Sleep(time.Second)
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
			`RESUME JOBS (SELECT job_id FROM [SHOW JOBS] WHERE status = $1);`,
			jobs.StatusPaused,
		)
		if err != nil {
			t.Fatal(err)
		}

		row := db.QueryRow(
			"SELECT count(*) FROM [SHOW JOBS] WHERE status = $1",
			jobs.StatusRunning,
		)
		var nRunning int
		if err := row.Scan(&nRunning); err != nil {
			t.Fatal(err)
		}
		t.l.Printf("Resumed %d jobs", nRunning)
		time.Sleep(d)
	}
}

func checkForFailedJobsStep(ctx context.Context, t *test, u *versionUpgradeTest) {
	t.l.Printf("Checking for failed jobs.")

	db := u.conn(ctx, t, 1)
	rows, err := db.Query(`
SELECT job_id, job_type, description, status, error, coordinator_id
FROM [SHOW JOBS] WHERE status = $1 OR status = $2`,
		jobs.StatusFailed, jobs.StatusReverting,
	)
	if err != nil {
		t.Fatal(err)
	}
	var jobType, desc, status, jobError string
	var jobID, coordinatorID int64
	var errMsg string
	for rows.Next() {
		err := rows.Scan(&jobID, &jobType, &desc, &status, &jobError, &coordinatorID)
		if err != nil {
			t.Fatal(err)
		}
		// Concatenate all unsuccessful jobs info.
		errMsg = fmt.Sprintf(
			"%sUnsuccessful job %d of type %s, description %s, status %s, error %s, coordinator %d\n",
			errMsg, jobID, jobType, desc, status, jobError, coordinatorID,
		)
	}
	if errMsg != "" {
		nodeInfo := "Cluster info\n"
		for i := range u.c.All() {
			nodeInfo = fmt.Sprintf(
				"%sNode %d: %s\n", nodeInfo, i+1, u.binaryVersion(ctx, t, i+1))
		}
		t.Fatalf("%s\n%s", nodeInfo, errMsg)
	}
}

func runJobsMixedVersions(
	ctx context.Context, t *test, c *cluster, warehouses int, predecessorVersion string,
) {
	// An empty string means that the cockroach binary specified by flag
	// `cockroach` will be used.
	const mainVersion = ""
	roachNodes := c.All()
	backgroundTPCC := backgroundTPCCWorkload(warehouses, "tpcc")
	resumeAllJobsAndWaitStep := makeResumeAllJobsAndWaitStep(10 * time.Second)
	c.Put(ctx, workload, "./workload", c.Node(1))

	u := newVersionUpgradeTest(c,
		uploadAndStartFromCheckpointFixture(roachNodes, predecessorVersion),
		waitForUpgradeStep(roachNodes),
		preventAutoUpgradeStep(1),

		backgroundTPCC.launch,
		func(ctx context.Context, _ *test, u *versionUpgradeTest) {
			time.Sleep(10 * time.Second)
		},
		checkForFailedJobsStep,
		pauseAllJobsStep(),

		// Roll the nodes into the new version one by one, while repeatedly pausing
		// and resuming all jobs.
		binaryUpgradeStep(c.Node(3), mainVersion),
		resumeAllJobsAndWaitStep,
		checkForFailedJobsStep,
		pauseAllJobsStep(),

		binaryUpgradeStep(c.Node(2), mainVersion),
		resumeAllJobsAndWaitStep,
		checkForFailedJobsStep,
		pauseAllJobsStep(),

		binaryUpgradeStep(c.Node(1), mainVersion),
		resumeAllJobsAndWaitStep,
		checkForFailedJobsStep,
		pauseAllJobsStep(),

		binaryUpgradeStep(c.Node(4), mainVersion),
		resumeAllJobsAndWaitStep,
		checkForFailedJobsStep,
		pauseAllJobsStep(),

		// Roll back again, which ought to be fine because the cluster upgrade was
		// not finalized.
		binaryUpgradeStep(c.Node(2), predecessorVersion),
		resumeAllJobsAndWaitStep,
		checkForFailedJobsStep,
		pauseAllJobsStep(),

		binaryUpgradeStep(c.Node(4), predecessorVersion),
		resumeAllJobsAndWaitStep,
		checkForFailedJobsStep,
		pauseAllJobsStep(),

		binaryUpgradeStep(c.Node(3), predecessorVersion),
		resumeAllJobsAndWaitStep,
		checkForFailedJobsStep,
		pauseAllJobsStep(),

		binaryUpgradeStep(c.Node(1), predecessorVersion),
		resumeAllJobsAndWaitStep,
		checkForFailedJobsStep,
		pauseAllJobsStep(),

		// Roll nodes forward and finalize upgrade.
		binaryUpgradeStep(c.Node(4), mainVersion),
		resumeAllJobsAndWaitStep,
		checkForFailedJobsStep,
		pauseAllJobsStep(),

		binaryUpgradeStep(c.Node(3), mainVersion),
		resumeAllJobsAndWaitStep,
		checkForFailedJobsStep,
		pauseAllJobsStep(),

		binaryUpgradeStep(c.Node(1), mainVersion),
		resumeAllJobsAndWaitStep,
		checkForFailedJobsStep,
		pauseAllJobsStep(),

		binaryUpgradeStep(c.Node(2), mainVersion),
		resumeAllJobsAndWaitStep,
		checkForFailedJobsStep,
		pauseAllJobsStep(),

		allowAutoUpgradeStep(1),
		waitForUpgradeStep(roachNodes),
		resumeAllJobsAndWaitStep,
		backgroundTPCC.stop,
		checkForFailedJobsStep,
	)
	u.run(ctx, t)
}

func registerJobsMixedVersions(r *testRegistry) {
	r.Add(testSpec{
		Name:  "jobs/mixed-versions",
		Owner: OwnerBulkIO,
		// Jobs infrastructure was unstable prior to 20.1 in terms of the behavior
		// of `PAUSE/CANCEL JOB` commands which were best effort and relied on the
		// job itself to detect the request. These were fixed by introducing new job
		// state machine states `Status{Pause,Cancel}Requested`. This test purpose
		// is to to test the state transitions of jobs from paused to resumed and
		// vice versa in order to detect regressions in the work done for 20.1.
		MinVersion: "v20.1.0",
		Cluster:    makeClusterSpec(4),
		Run: func(ctx context.Context, t *test, c *cluster) {
			predV, err := PredecessorVersion(r.buildVersion)
			if err != nil {
				t.Fatal(err)
			}
			warehouses := 200
			if local {
				warehouses = 20
			}
			runJobsMixedVersions(ctx, t, c, warehouses, predV)
		},
	})
}
