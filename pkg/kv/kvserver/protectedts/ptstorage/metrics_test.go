// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ptstorage_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestStorageMetricsProtect verifies that Protect operations update metrics
// correctly.
func TestStorageMetricsProtect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(0)
	manager := ptstorage.New(s.ClusterSettings(), nil)
	metrics := manager.Metrics()

	// Test successful Protect.
	protectSuccess := metrics.ProtectSuccess.Count()
	protectFailed := metrics.ProtectFailed.Count()

	id := uuid.MakeV4()
	err := s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		rec := &ptpb.Record{
			ID:        id.GetBytesMut(),
			Timestamp: hlc.Timestamp{WallTime: 1},
			MetaType:  "test",
			Meta:      []byte("test"),
			Target:    ptpb.MakeSchemaObjectsTarget(descpb.IDs{1}),
		}
		return manager.WithTxn(txn).Protect(ctx, rec)
	})
	require.NoError(t, err)
	require.Equal(t, protectSuccess+1, metrics.ProtectSuccess.Count())
	require.Equal(t, protectFailed, metrics.ProtectFailed.Count())

	// Test failed Protect (ErrExists).
	protectSuccess = metrics.ProtectSuccess.Count()
	protectFailed = metrics.ProtectFailed.Count()

	err = s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		rec := &ptpb.Record{
			ID:        id.GetBytesMut(),
			Timestamp: hlc.Timestamp{WallTime: 2},
			MetaType:  "test",
			Meta:      []byte("test"),
			Target:    ptpb.MakeSchemaObjectsTarget(descpb.IDs{1}),
		}
		return manager.WithTxn(txn).Protect(ctx, rec)
	})
	require.ErrorIs(t, err, protectedts.ErrExists)
	require.Equal(t, protectSuccess, metrics.ProtectSuccess.Count())
	require.Equal(t, protectFailed+1, metrics.ProtectFailed.Count())
}

// TestStorageMetricsRelease verifies that Release operations update metrics
// correctly.
func TestStorageMetricsRelease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(0)
	manager := ptstorage.New(s.ClusterSettings(), nil)
	metrics := manager.Metrics()

	// Create a record to release.
	id := uuid.MakeV4()
	err := s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		rec := &ptpb.Record{
			ID:        id.GetBytesMut(),
			Timestamp: hlc.Timestamp{WallTime: 1},
			MetaType:  "test",
			Meta:      []byte("test"),
			Target:    ptpb.MakeSchemaObjectsTarget(descpb.IDs{1}),
		}
		return manager.WithTxn(txn).Protect(ctx, rec)
	})
	require.NoError(t, err)

	// Test successful Release.
	releaseSuccess := metrics.ReleaseSuccess.Count()
	releaseFailed := metrics.ReleaseFailed.Count()

	err = s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return manager.WithTxn(txn).Release(ctx, id)
	})
	require.NoError(t, err)
	require.Equal(t, releaseSuccess+1, metrics.ReleaseSuccess.Count())
	require.Equal(t, releaseFailed, metrics.ReleaseFailed.Count())

	// Test failed Release (ErrNotExists).
	releaseSuccess = metrics.ReleaseSuccess.Count()
	releaseFailed = metrics.ReleaseFailed.Count()

	err = s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return manager.WithTxn(txn).Release(ctx, id)
	})
	require.ErrorIs(t, err, protectedts.ErrNotExists)
	require.Equal(t, releaseSuccess, metrics.ReleaseSuccess.Count())
	require.Equal(t, releaseFailed+1, metrics.ReleaseFailed.Count())
}

// TestStorageMetricsGetRecord verifies that GetRecord operations update
// metrics correctly.
func TestStorageMetricsGetRecord(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(0)
	manager := ptstorage.New(s.ClusterSettings(), nil)
	metrics := manager.Metrics()

	// Create a record to get.
	id := uuid.MakeV4()
	err := s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		rec := &ptpb.Record{
			ID:        id.GetBytesMut(),
			Timestamp: hlc.Timestamp{WallTime: 1},
			MetaType:  "test",
			Meta:      []byte("test"),
			Target:    ptpb.MakeSchemaObjectsTarget(descpb.IDs{1}),
		}
		return manager.WithTxn(txn).Protect(ctx, rec)
	})
	require.NoError(t, err)

	// Test successful GetRecord.
	getRecordSuccess := metrics.GetRecordSuccess.Count()
	getRecordFailed := metrics.GetRecordFailed.Count()

	err = s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := manager.WithTxn(txn).GetRecord(ctx, id)
		return err
	})
	require.NoError(t, err)
	require.Equal(t, getRecordSuccess+1, metrics.GetRecordSuccess.Count())
	require.Equal(t, getRecordFailed, metrics.GetRecordFailed.Count())

	// Test failed GetRecord (ErrNotExists).
	getRecordSuccess = metrics.GetRecordSuccess.Count()
	getRecordFailed = metrics.GetRecordFailed.Count()

	nonExistentID := uuid.MakeV4()
	err = s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := manager.WithTxn(txn).GetRecord(ctx, nonExistentID)
		return err
	})
	require.ErrorIs(t, err, protectedts.ErrNotExists)
	require.Equal(t, getRecordSuccess, metrics.GetRecordSuccess.Count())
	require.Equal(t, getRecordFailed+1, metrics.GetRecordFailed.Count())
}
