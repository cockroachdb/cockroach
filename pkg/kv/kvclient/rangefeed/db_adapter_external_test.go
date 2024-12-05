// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed_test

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func startMonitorWithBudget(budget int64) *mon.BytesMonitor {
	mm := mon.NewMonitor(mon.Options{
		Name:      mon.MakeMonitorName("test-mm"),
		Limit:     budget,
		Increment: 128, /* small allocation increment */
		Settings:  cluster.MakeTestingClusterSettings(),
	})
	mm.Start(context.Background(), nil, mon.NewStandaloneBudget(budget))
	return mm
}

// TestDBClientScan tests that the logic in Scan on the dbAdapter is sane.
// The rangefeed logic is a literal passthrough so it's not getting a lot of
// testing directly.
func TestDBClientScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()

	beforeAny := db.Clock().Now()

	scratchKey := slices.Clip(append(ts.Codec().TenantPrefix(), keys.ScratchRangeMin...))
	_, _, err := srv.StorageLayer().SplitRange(scratchKey)
	require.NoError(t, err)

	mkKey := func(k string) roachpb.Key {
		return encoding.EncodeStringAscending(scratchKey, k)
	}
	require.NoError(t, db.Put(ctx, mkKey("a"), 1))
	require.NoError(t, db.Put(ctx, mkKey("b"), 2))
	afterB := db.Clock().Now()
	require.NoError(t, db.Put(ctx, mkKey("c"), 3))

	dba, err := rangefeed.NewDBAdapter(db, ts.ClusterSettings())
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
		sqlDB := sqlutils.MakeSQLRunner(sqlDB)
		sqlDB.Exec(t, `CREATE TABLE foo (key INT PRIMARY KEY)`)
		defer func() {
			sqlDB.Exec(t, `DROP TABLE foo`)
		}()

		sqlDB.Exec(t, `INSERT INTO foo (key) SELECT * FROM generate_series(1, 1000)`)
		sqlDB.Exec(t, "ALTER TABLE foo SPLIT AT VALUES (250), (500), (750)")

		fooDesc := desctestutils.TestingGetPublicTableDescriptor(
			db, ts.Codec(), "defaultdb", "foo")
		fooSpan := fooDesc.PrimaryIndexSpan(ts.Codec())

		// Ensure the splits make it into the meta ranges and range cache. Simply
		// running a DistSender scan does not appear sufficient in rare cases.
		//
		// ScanWithOptions will split the scan requests itself based on the range
		// cache and assert on that split, before sending them to the DistSender.
		ds := ts.DistSenderI().(*kvcoord.DistSender)
		testutils.SucceedsSoon(t, func() error {
			// Flush the cache, and repopulate it via a scan. This looks at the
			// canonical range descriptors rather than the possibly stale meta ranges.
			ds.RangeDescriptorCache().Clear()
			_, err := db.Scan(ctx, fooSpan.Key, fooSpan.EndKey, 0)
			require.NoError(t, err)

			var descs []roachpb.RangeDescriptor
			iter := kvcoord.MakeRangeIterator(ds)
			for iter.Seek(ctx, roachpb.RKey(fooSpan.Key), kvcoord.Ascending); iter.Valid(); iter.Next(ctx) {
				desc := iter.Desc()
				if !fooSpan.Overlaps(desc.KeySpan().AsRawSpanWithNoLocals()) {
					break
				}
				descs = append(descs, *desc)
			}
			if len(descs) == 4 {
				t.Logf("range cache has 4 ranges: %v", descs)
				return nil
			}
			return errors.Errorf("range cache has %d ranges: %v", len(descs), descs)
		})

		// We have 4 ranges -- we'll start the scan with parallelism set to 3.
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
				rangefeed.WithOnScanCompleted(func(ctx context.Context, sp roachpb.Span) error {
					t.Logf("completed scan for %s", sp)
					atomic.AddInt32(&barrier, 1)
					<-proceed
					return nil
				}),
			)
		})

		testutils.SucceedsSoon(t, func() error {
			if b := atomic.LoadInt32(&barrier); b != int32(parallelism) {
				return errors.Errorf("still waiting for barrier (%d/%d)", b, parallelism)
			}
			return nil
		})
		close(proceed)
		require.NoError(t, g.Wait())
	})

	// Verify when errors occur during scan, only the failed spans are retried.
	t.Run("scan retries failed spans", func(t *testing.T) {
		sqlDB := sqlutils.MakeSQLRunner(sqlDB)
		sqlDB.Exec(t, `CREATE TABLE foo (key INT PRIMARY KEY)`)
		defer func() {
			sqlDB.Exec(t, `DROP TABLE foo`)
		}()

		sqlDB.Exec(t, `INSERT INTO foo (key) SELECT * FROM generate_series(1, 1000)`)
		sqlDB.Exec(t, "ALTER TABLE foo SPLIT AT (SELECT * FROM generate_series(100, 900, 100))")

		fooDesc := desctestutils.TestingGetPublicTableDescriptor(
			db, ts.Codec(), "defaultdb", "foo")
		fooSpan := fooDesc.PrimaryIndexSpan(ts.Codec())

		scanData := struct {
			syncutil.Mutex
			numSucceeded int
			failedSpan   roachpb.Span
			succeeded    roachpb.SpanGroup
		}{}

		// We expect 11 splits.
		// One span will fail.  Verify we retry only the spans that we have not attempted before.
		var parallelism = 6
		f := rangefeed.NewFactoryWithDB(ts.AppStopper(), dba, nil /* knobs */)
		scanComplete := make(chan struct{})
		scanErr := make(chan error, 1)
		retryScanErr := errors.New("retry scan")

		feed, err := f.RangeFeed(ctx, "foo-feed", []roachpb.Span{fooSpan}, db.Clock().Now(),
			func(ctx context.Context, value *kvpb.RangeFeedValue) {},

			rangefeed.WithInitialScanParallelismFn(func() int { return parallelism }),

			rangefeed.WithInitialScan(func(ctx context.Context) {
				close(scanComplete)
			}),

			rangefeed.WithOnInitialScanError(func(ctx context.Context, err error) (shouldFail bool) {
				if errors.Is(err, retryScanErr) {
					// If we see retry marker -- then retry.
					shouldFail = false
				} else {
					// Otherwise, fail the scan.
					shouldFail = true
					select {
					case scanErr <- err:
					default:
					}
				}
				return shouldFail
			}),

			rangefeed.WithOnScanCompleted(func(ctx context.Context, sp roachpb.Span) error {
				scanData.Lock()
				defer scanData.Unlock()

				// We expect 11 ranges to generate scan request, so we want to fail some range after
				// we have done scanning some spans.
				const numSpansToSucceedBeforeFail = 7
				if scanData.failedSpan.Key.Equal(scanData.failedSpan.EndKey) &&
					scanData.numSucceeded == numSpansToSucceedBeforeFail {
					scanData.failedSpan = sp
					return retryScanErr
				} else {
					// Verify we do not retry spans we've seen before.
					if scanData.succeeded.Contains(sp.Key) {
						return errors.Newf("span %s already scanned", sp)
					} else {
						scanData.succeeded.Add(sp)
						scanData.numSucceeded++
					}
				}
				return nil
			}),
		)

		require.NoError(t, err)
		defer feed.Close()

		select {
		case <-scanComplete:
		case err := <-scanErr:
			t.Fatalf("scan terminated in error: %v", err)
		}

		// Verify we have scanned entire table.
		require.Equal(t, 1, scanData.succeeded.Len(),
			"scanned spans: %v failed: %s", scanData.succeeded.Slice(), scanData.failedSpan)
		require.True(t, fooSpan.Equal(scanData.succeeded.Slice()[0]),
			"table=%s slice=%s", fooSpan, scanData.succeeded.Slice()[0])
	})
}
