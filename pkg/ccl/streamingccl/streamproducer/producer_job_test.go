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
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streampb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	resumer           jobs.Resumer
	revertingFinished chan<- struct{}
}

// Resume behaves the same as the normal resumer.
func (c coordinatedResumer) Resume(ctx context.Context, execCtx interface{}) error {
	return c.resumer.Resume(ctx, execCtx)
}

// OnFailOrCancel is called after the job reaches 'reverting' status
// and notifies watcher after it finishes.
func (c coordinatedResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	err := c.resumer.OnFailOrCancel(ctx, execCtx)
	c.revertingFinished <- struct{}{}
	return err
}

func TestStreamReplicationProducerJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// Test fails with the SQL server defined. More investigation
			// is required. Tracked with #76378.
			DisableDefaultSQLServer: true,
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
	ptp := source.DistSQLServer().(*distsql.ServerImpl).ServerConfig.ProtectedTimestampProvider
	timeout, username := 1*time.Second, security.MakeSQLUsernameFromPreNormalizedString("user")

	registerConstructor := func(initialTime time.Time) (*timeutil.ManualTime, func(), func(), func()) {
		mt := timeutil.NewManualTime(initialTime)
		waitJobFinishReverting := make(chan struct{})
		in, out := make(chan struct{}, 1), make(chan struct{}, 1)
		jobs.RegisterConstructor(jobspb.TypeStreamReplication, func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			r := &producerJobResumer{
				job:        job,
				timeSource: mt,
				timer:      makeCoordinatedTimer(mt.NewTimer(), in, out),
			}
			return coordinatedResumer{
				resumer:           r,
				revertingFinished: waitJobFinishReverting,
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
				<-waitJobFinishReverting // Waits until job reaches 'reverting' status
			}
	}

	jobsQuery := func(jobID jobspb.JobID) string {
		return fmt.Sprintf("SELECT status FROM system.jobs WHERE id = %d", jobID)
	}
	expirationTime := func(record jobs.Record) time.Time {
		return record.Progress.(jobspb.StreamReplicationProgress).Expiration
	}
	runJobWithProtectedTimestamp := func(ptsID uuid.UUID, ts hlc.Timestamp, jr jobs.Record) error {
		return source.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			deprecatedTenantSpan := roachpb.Spans{*makeTenantSpan(30)}
			tenantTarget := ptpb.MakeTenantsTarget([]roachpb.TenantID{roachpb.MakeTenantID(30)})
			if err := ptp.Protect(ctx, txn,
				jobsprotectedts.MakeRecord(ptsID, int64(jr.JobID), ts,
					deprecatedTenantSpan, jobsprotectedts.Jobs, tenantTarget)); err != nil {
				return err
			}
			_, err := registry.CreateAdoptableJobWithTxn(ctx, jr, jr.JobID, txn)
			return err
		})
	}
	getPTSRecord := func(ptsID uuid.UUID) (r *ptpb.Record, err error) {
		err = source.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			r, err = ptp.GetRecord(ctx, txn, ptsID)
			return err
		})
		return r, err
	}

	t.Run("producer-job", func(t *testing.T) {
		{ // Job times out at the beginning
			ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
			ptsID := uuid.MakeV4()
			jr := makeProducerJobRecord(registry, 10, timeout, username, ptsID)
			defer jobs.ResetConstructors()()
			_, _, _, waitJobFinishReverting := registerConstructor(expirationTime(jr).Add(1 * time.Millisecond))

			require.NoError(t, runJobWithProtectedTimestamp(ptsID, ts, jr))

			waitJobFinishReverting()

			sql.CheckQueryResultsRetry(t, jobsQuery(jr.JobID), [][]string{{"failed"}})
			// Ensures the protected timestamp record is released.
			_, err := getPTSRecord(ptsID)
			require.Error(t, err, "protected timestamp record does not exist")

			var status streampb.StreamReplicationStatus
			require.NoError(t, source.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				status, err = updateReplicationStreamProgress(
					ctx, timeutil.Now(), ptp, registry, streaming.StreamID(jr.JobID),
					hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}, txn)
				return err
			}))
			require.Equal(t, streampb.StreamReplicationStatus_STREAM_INACTIVE, status.StreamStatus)
		}

		{ // Job starts running and eventually fails after it's timed out
			ptsTime := timeutil.Now()
			ts := hlc.Timestamp{WallTime: ptsTime.UnixNano()}
			ptsID := uuid.MakeV4()

			jr := makeProducerJobRecord(registry, 20, timeout, username, ptsID)
			defer jobs.ResetConstructors()()
			mt, timeGiven, waitForTimeRequest, waitJobFinishReverting := registerConstructor(expirationTime(jr).Add(-5 * time.Millisecond))

			require.NoError(t, runJobWithProtectedTimestamp(ptsID, ts, jr))
			waitForTimeRequest()
			sql.CheckQueryResults(t, jobsQuery(jr.JobID), [][]string{{"running"}})

			updatedFrontier := hlc.Timestamp{
				// Set up a larger frontier timestamp to trigger protected timestamp update
				WallTime: ptsTime.Add(1 * time.Second).UnixNano(),
			}

			// Set expiration to a new time in the future
			var streamStatus streampb.StreamReplicationStatus
			var err error
			expire := expirationTime(jr).Add(10 * time.Millisecond)
			require.NoError(t, source.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				streamStatus, err = updateReplicationStreamProgress(
					ctx, expire,
					ptp, registry, streaming.StreamID(jr.JobID), updatedFrontier, txn)
				return err
			}))
			require.Equal(t, streampb.StreamReplicationStatus_STREAM_ACTIVE, streamStatus.StreamStatus)
			require.Equal(t, updatedFrontier, *streamStatus.ProtectedTimestamp)

			r, err := getPTSRecord(ptsID)
			require.NoError(t, err)
			// Ensure the timestamp is updated on the PTS record
			require.Equal(t, updatedFrontier, r.Timestamp)

			// Reset the time to be after the timeout
			mt.AdvanceTo(expire.Add(12 * time.Millisecond))
			timeGiven()
			waitJobFinishReverting()

			status := sql.QueryStr(t, jobsQuery(jr.JobID))[0][0]
			require.True(t, status == "reverting" || status == "failed")
			// Ensures the protected timestamp record is released.
			_, err = getPTSRecord(ptsID)
			require.Error(t, err, "protected timestamp record does not exist")
		}
	})
}
