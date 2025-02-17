// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	require.NoError(t, executeNodeShutdown(ctx, t, c, defaultNodeShutdownConfig(c, nodeToShutdown), startJob))
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

func defaultNodeShutdownConfig(c cluster.Cluster, nodeToShutdown int) nodeShutdownConfig {
	return nodeShutdownConfig{
		shutdownNode:         nodeToShutdown,
		watcherNode:          1 + (nodeToShutdown)%c.Spec().NodeCount,
		crdbNodes:            c.All(),
		waitFor3XReplication: true,
		sleepBeforeShutdown:  30 * time.Second,
	}
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
		err := roachtestutil.WaitFor3XReplication(ctx, t.L(), watcherDB)
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
				err := watcherDB.QueryRowContext(ctx, `SELECT status FROM [SHOW JOB $1]`, jobID).Scan(&status)
				if err != nil {
					return errors.Wrap(err, "getting the job status")
				}
				jobStatus := jobs.State(status)
				switch jobStatus {
				case jobs.StateSucceeded:
					t.Status("job completed")
					return nil
				case jobs.StateRunning:
					t.L().Printf("job %d still running, waiting to succeed", jobID)
				default:
					// Waiting for job to complete.
					return errors.Newf("unexpectedly found job %d in state %s", jobID, status)
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
		if err := c.StopE(
			ctx, t.L(), option.NewStopOpts(option.Graceful(shutdownGracePeriod)), c.Node(cfg.shutdownNode),
		); err != nil {
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
	if err := c.StartE(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule),
		install.MakeClusterSettings(cfg.restartSettings...), target); err != nil {
		return errors.Wrapf(err, "could not restart node %s", target)
	}
	return nil
}

type checkStatusFunc func(status jobs.State) (success bool, unexpected bool)

func WaitForState(
	ctx context.Context,
	db *gosql.DB,
	jobID jobspb.JobID,
	check checkStatusFunc,
	maxWait time.Duration,
) error {
	startTime := timeutil.Now()
	ticker := time.NewTicker(time.Microsecond)
	defer ticker.Stop()
	var state string
	for {
		select {
		case <-ticker.C:
			err := db.QueryRowContext(ctx, `SELECT status FROM [SHOW JOB $1]`, jobID).Scan(&state)
			if err != nil {
				return errors.Wrapf(err, "getting the job state %s", state)
			}
			success, unexpected := check(jobs.State(state))
			if unexpected {
				return errors.Newf("unexpectedly found job %d in state %s", jobID, state)
			}
			if success {
				return nil
			}
			if timeutil.Since(startTime) > maxWait {
				return errors.Newf("job %d did not reach state %s after %s", jobID, state, maxWait)
			}
			ticker.Reset(5 * time.Second)
		case <-ctx.Done():
			return errors.Wrapf(ctx.Err(), "context canceled while waiting for job to reach state %s", state)
		}
	}
}

func WaitForRunning(
	ctx context.Context, db *gosql.DB, jobID jobspb.JobID, maxWait time.Duration,
) error {
	return WaitForState(ctx, db, jobID,
		func(status jobs.State) (success bool, unexpected bool) {
			switch status {
			case jobs.StateRunning:
				return true, false
			case jobs.StatePending:
				return false, false
			default:
				return false, true
			}
		}, maxWait)
}

func WaitForSucceeded(
	ctx context.Context, db *gosql.DB, jobID jobspb.JobID, maxWait time.Duration,
) error {
	return WaitForState(ctx, db, jobID,
		func(status jobs.State) (success bool, unexpected bool) {
			switch status {
			case jobs.StateSucceeded:
				return true, false
			case jobs.StateRunning:
				return false, false
			default:
				return false, true
			}
		}, maxWait)
}

type jobRecord struct {
	status   string
	payload  jobspb.Payload
	progress jobspb.Progress
}

func (jr *jobRecord) GetHighWater() time.Time {
	var highwaterTime time.Time
	highwater := jr.progress.GetHighWater()
	if highwater != nil {
		highwaterTime = highwater.GoTime()
	}
	return highwaterTime
}
func (jr *jobRecord) GetFinishedTime() time.Time { return time.UnixMicro(jr.payload.FinishedMicros) }
func (jr *jobRecord) GetStatus() string          { return jr.status }
func (jr *jobRecord) GetError() string           { return jr.payload.Error }

func getJobRecord(db *gosql.DB, jobID int) (*jobRecord, error) {
	var (
		jr            jobRecord
		payloadBytes  []byte
		progressBytes []byte
	)
	if err := db.QueryRow(
		`SELECT status, payload, progress FROM crdb_internal.system_jobs WHERE id = $1`, jobID,
	).Scan(&jr.status, &payloadBytes, &progressBytes); err != nil {
		return nil, err
	}

	if err := protoutil.Unmarshal(payloadBytes, &jr.payload); err != nil {
		return nil, err
	}
	if err := protoutil.Unmarshal(progressBytes, &jr.progress); err != nil {
		return nil, err
	}
	return &jr, nil
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

func AssertReasonableFractionCompleted(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, jobID jobspb.JobID, nodeToQuery int,
) error {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	fractionsRecorded := make([]float64, 0)

	for {
		select {
		case <-ticker.C:
			fractionCompleted, err := getFractionProgressed(ctx, l, c, jobID, nodeToQuery)
			if err != nil {
				return err
			}
			fractionsRecorded = append(fractionsRecorded, fractionCompleted)
			if fractionCompleted == 1 {
				count := len(fractionsRecorded)
				if count > 5 && fractionsRecorded[count/2] < 0.2 && fractionsRecorded[count/2] > 0.8 {
					return errors.Newf("the median fraction completed was %.2f, which is outside (0.2,0.8)", fractionsRecorded[count/2])
				}
				l.Printf("not enough 'fractionCompleted' recorded to assert progress looks sane")
				return nil
			}
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "context canceled while waiting for job to finish")
		}
	}
}

func getFractionProgressed(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, jobID jobspb.JobID, nodeToQuery int,
) (float64, error) {
	var status string
	var fractionCompleted float64
	conn := c.Conn(ctx, l, nodeToQuery)
	defer conn.Close()
	err := conn.QueryRowContext(ctx, `SELECT status, fraction_completed FROM [SHOW JOB $1]`, jobID).Scan(&status, &fractionCompleted)
	if err != nil {
		return 0, errors.Wrap(err, "getting the job status and fraction completed")
	}
	jobStatus := jobs.State(status)
	switch jobStatus {
	case jobs.StateSucceeded:
		if fractionCompleted != 1 {
			return 0, errors.Newf("job completed but fraction completed is %.2f", fractionCompleted)
		}
		return fractionCompleted, nil
	case jobs.StateRunning:
		l.Printf("job %d still running, %.2f completed, waiting to succeed", jobID, fractionCompleted)
		return fractionCompleted, nil
	default:
		return 0, errors.Newf("unexpectedly found job %s in state %s", jobID, status)
	}
}
