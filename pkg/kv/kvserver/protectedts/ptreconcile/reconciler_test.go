// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ptreconcile_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptreconcile"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestReconciler(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	// Now I want to create some artifacts that should get reconciled away and
	// then make sure that they do and others which should not do not.
	s0 := tc.Server(0)
	ptp := s0.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider

	settings := cluster.MakeTestingClusterSettings()
	const testTaskType = "foo"
	var state = struct {
		mu       syncutil.Mutex
		toRemove map[string]struct{}
	}{}
	state.toRemove = map[string]struct{}{}
	cfg := ptreconcile.Config{
		Settings: settings,
		Stores:   s0.GetStores().(*kvserver.Stores),
		DB:       s0.DB(),
		Storage:  ptp,
		Cache:    ptp,
		StatusFuncs: ptreconcile.StatusFuncs{
			testTaskType: func(
				ctx context.Context, txn *kv.Txn, meta []byte,
			) (shouldRemove bool, err error) {
				state.mu.Lock()
				defer state.mu.Unlock()
				_, shouldRemove = state.toRemove[string(meta)]
				return shouldRemove, nil
			},
		},
	}
	r := ptreconcile.NewReconciler(cfg)
	require.NoError(t, r.Start(ctx, tc.Stopper()))
	recMeta := "a"
	rec1 := ptpb.Record{
		ID:        uuid.MakeV4(),
		Timestamp: s0.Clock().Now(),
		Mode:      ptpb.PROTECT_AFTER,
		MetaType:  testTaskType,
		Meta:      []byte(recMeta),
		Spans: []roachpb.Span{
			{Key: keys.MinKey, EndKey: keys.MaxKey},
		},
	}
	require.NoError(t, s0.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return ptp.Protect(ctx, txn, &rec1)
	}))

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
		require.Regexp(t, protectedts.ErrNotExists, s0.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			_, err := ptp.GetRecord(ctx, txn, rec1.ID)
			return err
		}))
	})
}
