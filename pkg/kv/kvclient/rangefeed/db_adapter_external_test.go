// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func startMonitorWithBudget(budget int64) *mon.BytesMonitor {
	mm := mon.NewMonitorWithLimit(
		"test-mm", mon.MemoryResource, budget,
		nil, nil,
		128 /* small allocation increment */, 100,
		cluster.MakeTestingClusterSettings())
	mm.Start(context.Background(), nil, mon.MakeStandaloneBudget(budget))
	return mm
}

// TestDBClientScan tests that the logic in Scan on the dbAdapter is sane.
// The rangefeed logic is a literal passthrough so it's not getting a lot of
// testing directly.
func TestDBClientScan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	db := tc.Server(0).DB()
	beforeAny := db.Clock().Now()
	scratchKey := tc.ScratchRange(t)
	mkKey := func(k string) roachpb.Key {
		return encoding.EncodeStringAscending(scratchKey, k)
	}
	require.NoError(t, db.Put(ctx, mkKey("a"), 1))
	require.NoError(t, db.Put(ctx, mkKey("b"), 2))
	afterB := db.Clock().Now()
	require.NoError(t, db.Put(ctx, mkKey("c"), 3))

	dba, err := rangefeed.NewDBAdapter(db)
	require.NoError(t, err)
	sp := roachpb.Span{
		Key:    scratchKey,
		EndKey: scratchKey.PrefixEnd(),
	}

	// Ensure that the timestamps are properly respected by not observing any
	// values at the timestamp preceding writes.
	t.Run("scan respects time bounds", func(t *testing.T) {
		var responses []roachpb.KeyValue
		require.NoError(t, dba.ScanWithOptions(
			ctx, []roachpb.Span{sp}, beforeAny, func(value roachpb.KeyValue) {
				responses = append(responses, value)
			}))
		require.Len(t, responses, 0)
	})

	// Ensure that expected values are seen at the intermediate timestamp.
	t.Run("scan sees values at intermediate ts", func(t *testing.T) {
		var responses []roachpb.KeyValue
		require.NoError(t, dba.ScanWithOptions(
			ctx, []roachpb.Span{sp}, afterB, func(value roachpb.KeyValue) {
				responses = append(responses, value)
			}))
		require.Len(t, responses, 2)
		require.Equal(t, mkKey("a"), responses[0].Key)
		va, err := responses[0].Value.GetInt()
		require.NoError(t, err)
		require.Equal(t, int64(1), va)
	})

	// Ensure that pagination doesn't break anything.
	t.Run("scan pagination works", func(t *testing.T) {
		var responses []roachpb.KeyValue
		require.NoError(t, dba.ScanWithOptions(ctx, []roachpb.Span{sp}, db.Clock().Now(),
			func(value roachpb.KeyValue) {
				responses = append(responses, value)
			},
			rangefeed.WithTargetScanBytes(1)))
		require.Len(t, responses, 3)
	})

	// Ensure scan respects memory limits.
	t.Run("scan respects memory limits", func(t *testing.T) {
		const memLimit = 4096
		mm := startMonitorWithBudget(memLimit)
		defer mm.Stop(ctx)

		require.Regexp(t, "memory budget exceeded",
			dba.ScanWithOptions(ctx, []roachpb.Span{sp}, db.Clock().Now(),
				func(value roachpb.KeyValue) {},
				rangefeed.WithTargetScanBytes(2*memLimit),
				rangefeed.WithMemoryMonitor(mm),
			))
	})

	// Verify parallel scan operations.
	t.Run("parallel scan requests", func(t *testing.T) {
		sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
		sqlDB.Exec(t, `CREATE TABLE foo (key INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo (key) SELECT * FROM generate_series(1, 1000)`)
		sqlDB.Exec(t, "ALTER TABLE foo SPLIT AT VALUES (250), (500), (750)")

		fooDesc := catalogkv.TestingGetTableDescriptor(
			db, keys.SystemSQLCodec, "defaultdb", "foo")
		fooSpan := fooDesc.PrimaryIndexSpan(keys.SystemSQLCodec)

		// We expect 4 splits -- we'll start the scan with parallelism set to 3.
		// We will block these scans from completion until we know that we have 3
		// concurrently running scan requests.
		var parallelism = 3
		var barrier int32 = 0
		proceed := make(chan struct{})

		g := ctxgroup.WithContext(context.Background())
		g.GoCtx(func(ctx context.Context) error {
			return dba.ScanWithOptions(ctx, []roachpb.Span{fooSpan}, db.Clock().Now(),
				func(value roachpb.KeyValue) {},
				rangefeed.WithInitialScanParallelismFn(func() int { return parallelism }),
				rangefeed.WithOnSpanScanCompleted(func(ctx context.Context, sp roachpb.Span) {
					atomic.AddInt32(&barrier, 1)
					<-proceed
				}),
			)
		})

		testutils.SucceedsSoon(t, func() error {
			if atomic.LoadInt32(&barrier) == int32(parallelism) {
				return nil
			}
			return errors.New("still  waiting for barrier")
		})
		close(proceed)
		require.NoError(t, g.Wait())
	})
}
