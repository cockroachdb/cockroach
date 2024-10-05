// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ptreconcile_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptreconcile"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestReconciler(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	testutils.RunTrueAndFalse(t, "reconciler", func(t *testing.T, withDeprecatedSpans bool) {
		defer log.Scope(t).Close(t)

		srv := serverutils.StartServerOnly(t, base.TestServerArgs{
			Knobs: base.TestingKnobs{
				ProtectedTS: &protectedts.TestingKnobs{DisableProtectedTimestampForMultiTenant: withDeprecatedSpans,
					UseMetaTable: withDeprecatedSpans},
			},
		})
		defer srv.Stopper().Stop(ctx)
		s0 := srv.ApplicationLayer()

		// Now I want to create some artifacts that should get reconciled away and
		// then make sure that they do and others which should not do not.
		insqlDB := s0.InternalDB().(isql.DB)
		ptp := s0.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
		pts := ptstorage.WithDatabase(ptp, insqlDB)

		settings := cluster.MakeTestingClusterSettings()
		const testTaskType = "foo"
		var state = struct {
			mu       syncutil.Mutex
			toRemove map[string]struct{}
		}{}
		state.toRemove = map[string]struct{}{}

		r := ptreconcile.New(settings, insqlDB, ptp,
			ptreconcile.StatusFuncs{
				testTaskType: func(
					ctx context.Context, txn isql.Txn, meta []byte,
				) (shouldRemove bool, err error) {
					state.mu.Lock()
					defer state.mu.Unlock()
					_, shouldRemove = state.toRemove[string(meta)]
					return shouldRemove, nil
				},
			})
		require.NoError(t, r.StartReconciler(ctx, s0.AppStopper()))
		recMeta := "a"
		rec1 := ptpb.Record{
			ID:        uuid.MakeV4().GetBytes(),
			Timestamp: s0.Clock().Now(),
			Mode:      ptpb.PROTECT_AFTER,
			MetaType:  testTaskType,
			Meta:      []byte(recMeta),
		}
		if withDeprecatedSpans {
			rec1.DeprecatedSpans = []roachpb.Span{{Key: keys.MinKey, EndKey: keys.MaxKey}}
		} else {
			rec1.Target = ptpb.MakeClusterTarget()
		}
		require.NoError(t, pts.Protect(ctx, &rec1))

		t.Run("update settings", func(t *testing.T) {
			ptreconcile.ReconcileInterval.Override(ctx, &settings.SV, time.Millisecond)
			testutils.SucceedsSoon(t, func() error {
				require.Equal(t, int64(0), r.Metrics().RecordsRemoved.Count())
				require.Equal(t, int64(0), r.Metrics().ReconciliationErrors.Count())
				if processed := r.Metrics().RecordsProcessed.Count(); processed < 1 {
					return errors.Errorf("expected processed to be at least 1, got %d", processed)
				}
				return nil
			})
		})
		t.Run("reconcile", func(t *testing.T) {
			state.mu.Lock()
			state.toRemove[recMeta] = struct{}{}
			state.mu.Unlock()

			ptreconcile.ReconcileInterval.Override(ctx, &settings.SV, time.Millisecond)
			testutils.SucceedsSoon(t, func() error {
				require.Equal(t, int64(0), r.Metrics().ReconciliationErrors.Count())
				if removed := r.Metrics().RecordsRemoved.Count(); removed != 1 {
					return errors.Errorf("expected processed to be 1, got %d", removed)
				}
				return nil
			})
			_, err := pts.GetRecord(ctx, rec1.ID.GetUUID())
			require.Regexp(t, protectedts.ErrNotExists, err)
		})
	})
}
