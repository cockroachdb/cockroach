// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package producer

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestStreamReplicationProducerJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
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
	usr := username.MakeSQLUsernameFromPreNormalizedString("user")
	tenantID := uint64(32)

	insqlDB := source.InternalDB().(isql.DB)
	runJobWithProtectedTimestamp := func(ptsID uuid.UUID, ts hlc.Timestamp, jr jobs.Record) error {
		return insqlDB.Txn(ctx, func(
			ctx context.Context, txn isql.Txn,
		) error {
			deprecatedTenantSpan := roachpb.Spans{makeTenantSpan(tenantID)}
			tenantTarget := ptpb.MakeTenantsTarget([]roachpb.TenantID{roachpb.MustMakeTenantID(tenantID)})
			record := jobsprotectedts.MakeRecord(
				ptsID, int64(jr.JobID), ts, deprecatedTenantSpan,
				jobsprotectedts.Jobs, tenantTarget,
			)
			if err := ptp.WithTxn(txn).Protect(ctx, record); err != nil {
				return err
			}
			_, err := registry.CreateAdoptableJobWithTxn(ctx, jr, jr.JobID, txn)
			return err
		})
	}
	getPTSRecord := func(ptsID uuid.UUID) (r *ptpb.Record, err error) {
		err = insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			r, err = ptp.WithTxn(txn).GetRecord(ctx, ptsID)
			return err
		})
		return r, err
	}

	t.Run("producer-job-times-out", func(t *testing.T) {
		// Job times out at the beginning
		ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		ptsID := uuid.MakeV4()
		ti := &mtinfopb.TenantInfo{
			SQLInfo: mtinfopb.SQLInfo{ID: 10},
		}
		jr := makeProducerJobRecord(registry, ti, time.Millisecond, usr, ptsID, false)

		require.NoError(t, runJobWithProtectedTimestamp(ptsID, ts, jr))

		jobutils.WaitForJobToFail(t, sql, jr.JobID)

		// Ensures the protected timestamp record is released.
		_, err := getPTSRecord(ptsID)
		require.True(t, testutils.IsError(err, "protected timestamp record does not exist"), err)

		var status streampb.StreamReplicationStatus
		require.NoError(t, insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			status, err = updateReplicationStreamProgress(
				ctx, timeutil.Now(), ptp, registry, streampb.StreamID(jr.JobID),
				hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}, txn)
			return err
		}))
		require.Equal(t, streampb.StreamReplicationStatus_STREAM_INACTIVE, status.StreamStatus)
	})

	t.Run("producer job makes progress", func(t *testing.T) {
		ptsTime := timeutil.Now()
		ti := &mtinfopb.TenantInfo{
			SQLInfo: mtinfopb.SQLInfo{ID: 20},
		}
		ts := hlc.Timestamp{WallTime: ptsTime.UnixNano()}
		ptsID := uuid.MakeV4()
		expirationWindow := time.Hour
		jr := makeProducerJobRecord(registry, ti, expirationWindow, usr, ptsID, false)

		require.NoError(t, runJobWithProtectedTimestamp(ptsID, ts, jr))

		// Coordinate the job to get a new time.
		jobutils.WaitForJobToRun(t, sql, jr.JobID)
		updatedFrontier := hlc.Timestamp{
			// Set up a larger frontier timestamp to trigger protected timestamp update
			WallTime: ptsTime.Add(1 * time.Second).UnixNano(),
		}

		// Push the expiration to a new time in the future by passing a begin timestamp in the future.
		var streamStatus streampb.StreamReplicationStatus
		var err error
		newExpiration := timeutil.Now().Add(expirationWindow)
		require.NoError(t, insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			streamStatus, err = updateReplicationStreamProgress(
				ctx, newExpiration, ptp, registry, streampb.StreamID(jr.JobID), updatedFrontier, txn)
			return err
		}))
		require.Equal(t, streampb.StreamReplicationStatus_STREAM_ACTIVE, streamStatus.StreamStatus)
		require.Equal(t, updatedFrontier, *streamStatus.ProtectedTimestamp)

		r, err := getPTSRecord(ptsID)
		require.NoError(t, err)
		// Ensure the timestamp is updated on the PTS record
		require.Equal(t, updatedFrontier, r.Timestamp)
	})
}
