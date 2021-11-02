// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamproducer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type coordinatedTimer struct {
	timer timeutil.TimerI
	in    <-chan struct{}
	out   chan<- struct{}
}

// Reset resets the timer and waits for a new time to be
// assigned to the underlying manual time source.
func (s *coordinatedTimer) Reset(duration time.Duration) {
	s.timer.Reset(duration)
	s.out <- struct{}{}
}

// Stop behaves the same as the normal timer.
func (s *coordinatedTimer) Stop() bool {
	return s.timer.Stop()
}

// Ch returns next timer event after a new time is assigned to
// the underlying manual time source.
func (s *coordinatedTimer) Ch() <-chan time.Time {
	<-s.in
	return s.timer.Ch()
}

// MarkRead behaves the same as the normal timer.
func (s *coordinatedTimer) MarkRead() {
	s.timer.MarkRead()
}

func makeCoordinatedTimer(
	i timeutil.TimerI, in <-chan struct{}, out chan<- struct{},
) timeutil.TimerI {
	return &coordinatedTimer{
		timer: i,
		in:    in,
		out:   out,
	}
}

type coordinatedResumer struct {
	resumer            jobs.Resumer
	revertingConfirmed chan<- struct{}
}

// Resume behaves the same as the normal resumer.
func (c coordinatedResumer) Resume(ctx context.Context, execCtx interface{}) error {
	return c.resumer.Resume(ctx, execCtx)
}

// OnFailOrCancel is called after the job reaches 'reverting' status
// and notifies watcher after it finishes.
func (c coordinatedResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	err := c.resumer.OnFailOrCancel(ctx, execCtx)
	c.revertingConfirmed <- struct{}{}
	return err
}

func TestStreamReplicationProducerJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
		},
	}
	tc := testcluster.StartTestCluster(t, 1, clusterArgs)
	defer tc.Stopper().Stop(ctx)

	source := tc.Server(0)
	sql := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	// Shorten the tracking frequency to make timer easy to be triggerred.
	sql.Exec(t, "SET CLUSTER SETTING stream_replication.stream_liveness_track_frequency = '1ms'")
	registry := source.JobRegistry().(*jobs.Registry)

	resetConstructor := func() {
		jobs.RegisterConstructor(
			jobspb.TypeStreamReplication,
			func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
				ts := timeutil.DefaultTimeSource{}
				return &producerJobResumer{
					job:        job,
					timeSource: ts,
					timer:      ts.NewTimer(),
				}
			},
		)
	}

	registerConstructor := func(initialTime time.Time) (*timeutil.ManualTime, func(), func(), func()) {
		mt := timeutil.NewManualTime(initialTime)
		waitUntilReverting := make(chan struct{})
		in, out := make(chan struct{}, 1), make(chan struct{}, 1)
		jobs.RegisterConstructor(jobspb.TypeStreamReplication, func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			r := &producerJobResumer{
				job:        job,
				timeSource: mt,
				timer:      makeCoordinatedTimer(mt.NewTimer(), in, out),
			}
			return coordinatedResumer{
				resumer:            r,
				revertingConfirmed: waitUntilReverting,
			}
		})
		return mt,
			func() {
				in <- struct{}{} // Signals the timer that a new time is assigned to time source
			},
			func() {
				<-out // Waits until caller starts waiting for a new timer event
			},
			func() {
				<-waitUntilReverting // Waits until job reaches 'reverting' status
			}
	}

	startJob := func(jobID jobspb.JobID, jr jobs.Record) error {
		return source.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			_, err := registry.CreateAdoptableJobWithTxn(ctx, jr, jobID, txn)
			return err
		})
	}

	timeout, username := 1*time.Second, security.MakeSQLUsernameFromPreNormalizedString("user")
	jobsQuery := func(jobID jobspb.JobID) string {
		return fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %d", jobID)
	}
	expirationTime := func(record jobs.Record) time.Time {
		return record.Progress.(jobspb.StreamReplicationProgress).Expiration
	}
	t.Run("producer-job", func(t *testing.T) {
		jobID, jr := makeProducerJobRecord(registry, 10, timeout, username)

		// Case 1: Resumer wakes up and finds the job timed out.
		_, _, _, waitUntilReverting := registerConstructor(expirationTime(jr).Add(1 * time.Millisecond))
		require.NoError(t, startJob(jobID, jr))

		waitUntilReverting()
		sql.SucceedsSoonDuration = 1 * time.Second
		sql.CheckQueryResultsRetry(t, jobsQuery(jobID), [][]string{{"failed"}})

		// Case 2: Resumer wakes up and find the job still active.
		jobID, jr = makeProducerJobRecord(registry, 20, timeout, username)
		mt, timeGiven, waitForTimeRequest, waitUntilReverting := registerConstructor(expirationTime(jr).Add(-5 * time.Millisecond))
		defer resetConstructor()

		require.NoError(t, startJob(jobID, jr))
		waitForTimeRequest()
		sql.CheckQueryResults(t, jobsQuery(jobID), [][]string{{"running"}})

		// Reset the time to be after the timeout
		mt.AdvanceTo(expirationTime(jr).Add(2 * time.Millisecond))
		timeGiven()
		waitUntilReverting()
		status := sql.QueryStr(t, jobsQuery(jobID))[0][0]
		require.True(t, status == "reverting" || status == "failed")
	})
}
