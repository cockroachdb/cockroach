// Copyright 2021 The Cockroach Authors.
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
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type jobStarter func(c cluster.Cluster) (string, error)

// jobSurvivesNodeShutdown is a helper that tests that a given job,
// running on the specified gatewayNode will still complete successfully
// if nodeToShutdown is shutdown partway through execution.
// This helper assumes:
// - That the job is long running and will take a least a minute to complete.
// - That the necessary setup is done (e.g. any data that the job relies on is
// already loaded) so that `query` can be run on its own to kick off the job.
// - That the statement running the job is a detached statement, and does not
// block until the job completes.
func jobSurvivesNodeShutdown(
	ctx context.Context, t test.Test, c cluster.Cluster, nodeToShutdown int, startJob jobStarter,
) {
	watcherNode := 1 + (nodeToShutdown)%c.Spec().NodeCount
	target := c.Node(nodeToShutdown)
	t.L().Printf("test has chosen shutdown target node %d, and watcher node %d",
		nodeToShutdown, watcherNode)

	jobIDCh := make(chan string, 1)

	m := c.NewMonitor(ctx)
	m.Go(func(ctx context.Context) error {
		defer close(jobIDCh)
		t.Status(`running job`)
		var jobID string
		jobID, err := startJob(c)
		if err != nil {
			return errors.Wrap(err, "starting the job")
		}
		t.L().Printf("started running job with ID %s", jobID)
		jobIDCh <- jobID

		pollInterval := 5 * time.Second
		ticker := time.NewTicker(pollInterval)

		watcherDB := c.Conn(ctx, watcherNode)
		defer watcherDB.Close()

		var status string
		for {
			select {
			case <-ticker.C:
				err := watcherDB.QueryRowContext(ctx, `SELECT status FROM [SHOW JOBS] WHERE job_id=$1`, jobID).Scan(&status)
				if err != nil {
					return errors.Wrap(err, "getting the job status")
				}
				jobStatus := jobs.Status(status)
				switch jobStatus {
				case jobs.StatusSucceeded:
					t.Status("job completed")
					return nil
				case jobs.StatusRunning:
					t.L().Printf("job %s still running, waiting to succeed", jobID)
				default:
					// Waiting for job to complete.
					return errors.Newf("unexpectedly found job %s in state %s", jobID, status)
				}
			case <-ctx.Done():
				return errors.Wrap(ctx.Err(), "context canceled while waiting for job to finish")
			}
		}
	})

	m.Go(func(ctx context.Context) error {
		jobID, ok := <-jobIDCh
		if !ok {
			return errors.New("job never created")
		}

		// Shutdown a node after a bit, and keep it shutdown for the remainder
		// of the job.
		timeToWait := 10 * time.Second
		timer := timeutil.Timer{}
		timer.Reset(timeToWait)
		select {
		case <-ctx.Done():
			return errors.Wrapf(ctx.Err(), "stopping test, did not shutdown node")
		case <-timer.C:
			timer.Read = true
		}

		// Sanity check that the job is still running.
		watcherDB := c.Conn(ctx, watcherNode)
		defer watcherDB.Close()

		var status string
		err := watcherDB.QueryRowContext(ctx, `SELECT status FROM [SHOW JOBS] WHERE job_id=$1`, jobID).Scan(&status)
		if err != nil {
			return errors.Wrap(err, "getting the job status")
		}
		jobStatus := jobs.Status(status)
		if jobStatus != jobs.StatusRunning {
			return errors.Newf("job too fast! job got to state %s before the target node could be shutdown",
				status)
		}

		t.L().Printf(`stopping node %s`, target)
		if err := c.StopE(ctx, target); err != nil {
			return errors.Wrapf(err, "could not stop node %s", target)
		}
		t.L().Printf("stopped node %s", target)

		return nil
	})

	m.ExpectDeath()
	m.Wait()

	// NB: the roachtest harness checks that at the end of the test, all nodes
	// that have data also have a running process.
	t.Status(fmt.Sprintf("restarting %s (node restart test is done)\n", target))
	if err := c.StartE(ctx, target); err != nil {
		t.Fatal(errors.Wrapf(err, "could not restart node %s", target))
	}
}
