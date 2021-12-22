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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type jobStarter func(c cluster.Cluster, t test.Test) (string, error)

// jobSurvivesNodeShutdown is a helper that tests that a given job,
// running on the specified gatewayNode will still complete successfully
// if nodeToShutdown is shutdown partway through execution.
//
// This helper assumes:
// - That the job is will take at least 2 seconds to complete.
// - That the necessary setup is done (e.g. any data that the job relies on is
// already loaded) so that `query` can be run on its own to kick off the job.
// - That the statement running the job is a detached statement, and does not
// block until the job completes.
//
// The helper waits for 3x replication on existing ranges before
// running the provided jobStarter.
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

		watcherDB := c.Conn(ctx, t.L(), watcherNode)
		defer watcherDB.Close()

		// Wait for 3x replication to ensure that the cluster
		// is in a healthy state before we start bringing any
		// nodes down.
		t.Status("waiting for cluster to be 3x replicated")
		WaitFor3XReplication(t, watcherDB)

		t.Status("running job")
		var jobID string
		jobID, err := startJob(c, t)
		if err != nil {
			return errors.Wrap(err, "starting the job")
		}
		t.L().Printf("started running job with ID %s", jobID)
		jobIDCh <- jobID

		pollInterval := 5 * time.Second
		ticker := time.NewTicker(pollInterval)
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

		// Check once a second to see if the job has started running.
		watcherDB := c.Conn(ctx, t.L(), watcherNode)
		defer watcherDB.Close()
		timeToWait := time.Second
		timer := timeutil.Timer{}
		jobRunning := false
		for {
			var status string
			err := watcherDB.QueryRowContext(ctx, `SELECT status FROM [SHOW JOBS] WHERE job_id=$1`, jobID).Scan(&status)
			if err != nil {
				return errors.Wrap(err, "getting the job status")
			}
			switch jobs.Status(status) {
			case jobs.StatusPending:
			case jobs.StatusRunning:
				jobRunning = true
			default:
				return errors.Newf("job too fast! job got to state %s before the target node could be shutdown",
					status)
			}
			t.L().Printf(`status %s`, status)
			timer.Reset(timeToWait)
			select {
			case <-ctx.Done():
				return errors.Wrapf(ctx.Err(), "stopping test, did not shutdown node")
			case <-timer.C:
				timer.Read = true
			}
			// Break a second after confirming the job is running to ensure the node shutdown
			// "in the middle" of running the job, right after the job began running.
			if jobRunning {
				break
			}
		}

		t.L().Printf(`stopping node %s`, target)
		if err := c.StopE(ctx, t.L(), option.DefaultStopOpts(), target); err != nil {
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
	if err := c.StartE(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), target); err != nil {
		t.Fatal(errors.Wrapf(err, "could not restart node %s", target))
	}
}
