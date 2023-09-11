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
	gosql "database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type jobStarter func(c cluster.Cluster, l *logger.Logger) (jobspb.JobID, error)

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
	cfg := nodeShutdownConfig{
		shutdownNode:         nodeToShutdown,
		watcherNode:          1 + (nodeToShutdown)%c.Spec().NodeCount,
		crdbNodes:            c.All(),
		waitFor3XReplication: true,
		sleepBeforeShutdown:  30 * time.Second,
	}
	require.NoError(t, executeNodeShutdown(ctx, t, c, cfg, startJob))
}

type nodeShutdownConfig struct {
	shutdownNode         int
	watcherNode          int
	crdbNodes            option.NodeListOption
	restartSettings      []install.ClusterSettingOption
	waitFor3XReplication bool
	sleepBeforeShutdown  time.Duration
	rng                  *rand.Rand
}

// executeNodeShutdown executes a node shutdown and returns all errors back to the caller.
//
// TODO(msbutler): ideally, t.L() is only passed to this function instead of t,
// but WaitFor3xReplication requires t. Once this function only has a logger, we
// can guarantee that all errors return to the caller.
func executeNodeShutdown(
	ctx context.Context, t test.Test, c cluster.Cluster, cfg nodeShutdownConfig, startJob jobStarter,
) error {
	target := c.Node(cfg.shutdownNode)
	t.L().Printf("test has chosen shutdown target node %d, and watcher node %d",
		cfg.shutdownNode, cfg.watcherNode)

	watcherDB := c.Conn(ctx, t.L(), cfg.watcherNode)
	defer watcherDB.Close()

	if cfg.waitFor3XReplication {
		// Wait for 3x replication to ensure that the cluster
		// is in a healthy state before we start bringing any
		// nodes down.
		t.Status("waiting for cluster to be 3x replicated")
		err := WaitFor3XReplication(ctx, t, watcherDB)
		if err != nil {
			return err
		}
	}

	t.Status("running job")
	jobID, err := startJob(c, t.L())
	if err != nil {
		return err
	}
	t.L().Printf("started running job with ID %s", jobID)
	if err := WaitForRunning(ctx, watcherDB, jobID, time.Minute); err != nil {
		return err
	}

	m := c.NewMonitor(ctx, cfg.crdbNodes)
	m.ExpectDeath()
	m.Go(func(ctx context.Context) error {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
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
	time.Sleep(cfg.sleepBeforeShutdown)
	if cfg.rng == nil {
		rng, _ := randutil.NewTestRand()
		cfg.rng = rng
	}
	shouldUseSigKill := cfg.rng.Float64() > 0.5
	if shouldUseSigKill {
		t.L().Printf(`stopping node (using SIGKILL) %s`, target)
		if err := c.StopE(ctx, t.L(), option.DefaultStopOpts(), target); err != nil {
			return errors.Wrapf(err, "could not stop node %s", target)
		}
	} else {
		t.L().Printf(`stopping node gracefully %s`, target)
		if err := c.StopCockroachGracefullyOnNode(ctx, t.L(), cfg.shutdownNode); err != nil {
			return errors.Wrapf(err, "could not stop node %s", target)
		}
	}
	t.L().Printf("stopped node %s", target)

	if err := m.WaitE(); err != nil {
		return err
	}
	// NB: the roachtest harness checks that at the end of the test, all nodes
	// that have data also have a running process.
	t.Status(fmt.Sprintf("restarting %s (node restart test is done)\n", target))
	// Don't begin another backup schedule, as the parent test driver has already
	// set or disallowed the automatic backup schedule.
	if err := c.StartE(ctx, t.L(), option.DefaultStartOptsNoBackups(),
		install.MakeClusterSettings(cfg.restartSettings...), target); err != nil {
		return errors.Wrapf(err, "could not restart node %s", target)
	}
	return nil
}

func WaitForRunning(
	ctx context.Context, db *gosql.DB, jobID jobspb.JobID, maxWait time.Duration,
) error {
	return testutils.SucceedsWithinError(func() error {
		var status jobs.Status
		if err := db.QueryRowContext(ctx, "SELECT status FROM crdb_internal.system_jobs WHERE id = $1", jobID).Scan(&status); err != nil {
			return err
		}
		switch status {
		case jobs.StatusPending:
		case jobs.StatusRunning:
		default:
			return errors.Newf("job too fast! job got to state %s before the target node could be shutdown",
				status)
		}
		return nil
	}, maxWait)
}

func getJobProgress(t test.Test, db *sqlutils.SQLRunner, jobID jobspb.JobID) *jobspb.Progress {
	ret := &jobspb.Progress{}
	var buf []byte
	db.QueryRow(t, `SELECT progress FROM crdb_internal.system_jobs WHERE id = $1`, jobID).Scan(&buf)
	if err := protoutil.Unmarshal(buf, ret); err != nil {
		t.Fatal(err)
	}
	return ret
}
