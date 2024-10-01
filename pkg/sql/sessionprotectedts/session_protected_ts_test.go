// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sessionprotectedts_test

import (
	"context"
	gosql "database/sql"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptreconcile"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionprotectedts"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func createSessionAndProtect(
	ctx context.Context,
	t *testing.T,
	s0 serverutils.ApplicationLayerInterface,
	sqlDB *sqlutils.SQLRunner,
	pts protectedts.Storage,
) (*gosql.DB, *ptpb.Record) {
	t.Helper()

	// Start the first SQL session that will lay down a PTS record.
	targetDB := s0.SQLConn(t, serverutils.DBName("defaultdb"))
	targetRunner := sqlutils.MakeSQLRunner(targetDB)
	targetRunner.Exec(t, `SELECT 1`)

	// Fetch the IDLE session's ID.
	var idleSessionID string
	targetRunner.QueryRow(t, `SHOW session_id`).Scan(&idleSessionID)
	recordID := uuid.MakeV4()
	rec := sessionprotectedts.MakeRecord(recordID, []byte(idleSessionID), s0.Clock().Now(), ptpb.MakeClusterTarget())
	require.NoError(t, pts.Protect(ctx, rec))

	return targetDB, rec
}

func TestSessionProtectedTimestampReconciler(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderRace(t, "May cause a conn executor race fixed by #114783")

	ctx := context.Background()
	defer log.Scope(t).Close(t)

	testCluster := serverutils.StartCluster(t, 1, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(ctx)
	s0 := testCluster.Server(0).ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(s0.SQLConn(t, serverutils.DBName("defaultdb")))

	insqlDB := s0.InternalDB().(isql.DB)
	ptp := s0.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
	pts := ptstorage.WithDatabase(ptp, insqlDB)

	settings := cluster.MakeTestingClusterSettings()
	r := ptreconcile.New(settings, insqlDB, ptp,
		ptreconcile.StatusFuncs{
			sessionprotectedts.SessionMetaType: sessionprotectedts.MakeStatusFunc(),
		})
	require.NoError(t, r.StartReconciler(ctx, s0.AppStopper()))

	// Create two sessions, these will be IDLE as they aren't actively running a
	// query.
	s1, rec1 := createSessionAndProtect(ctx, t, s0, sqlDB, pts)
	s2, rec2 := createSessionAndProtect(ctx, t, s0, sqlDB, pts)

	state, err := pts.GetState(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, len(state.Records))

	t.Run("reconcile", func(t *testing.T) {
		ptreconcile.ReconcileInterval.Override(ctx, &settings.SV, time.Millisecond)
		// Close one of the sessions, and ensure that the reconciler removes the
		// associated PTS record.
		require.NoError(t, s1.Close())
		testutils.SucceedsSoon(t, func() error {
			require.Equal(t, int64(0), r.Metrics().ReconciliationErrors.Count())
			if removed := r.Metrics().RecordsRemoved.Count(); removed != 1 {
				return errors.Newf("expected processed to be 1, got %d", removed)
			}
			return nil
		})
		_, err := pts.GetRecord(ctx, rec1.ID.GetUUID())
		require.Regexp(t, protectedts.ErrNotExists, err)

		// Verify the second record is still around.
		_, err = pts.GetRecord(ctx, rec2.ID.GetUUID())
		require.NoError(t, err)
		require.NoError(t, s2.Close())

		// Open one last session and keep it active.
		s3, rec3 := createSessionAndProtect(ctx, t, s0, sqlDB, pts)
		defer func() {
			_ = s3.Close()
		}()
		var wg sync.WaitGroup
		wg.Add(1)
		aboutToSleep := make(chan struct{})
		defer close(aboutToSleep)
		go func() {
			defer wg.Done()
			aboutToSleep <- struct{}{}
			_, err := s3.Exec(`SELECT generate_series(1, 5000000)`)
			require.NoError(t, err)
		}()
		<-aboutToSleep

		// Finally, check that the reconciler removes s2's record but leaves the
		// ACTIVE session's record alone.
		testutils.SucceedsSoon(t, func() error {
			require.Equal(t, int64(0), r.Metrics().ReconciliationErrors.Count())
			if removed := r.Metrics().RecordsRemoved.Count(); removed != 2 {
				return errors.Newf("expected processed to be 2, got %d", removed)
			}
			return nil
		})

		_, err = pts.GetRecord(ctx, rec2.ID.GetUUID())
		require.Regexp(t, protectedts.ErrNotExists, err)
		_, err = pts.GetRecord(ctx, rec3.ID.GetUUID())
		require.NoError(t, err)

		wg.Wait()
	})
}
