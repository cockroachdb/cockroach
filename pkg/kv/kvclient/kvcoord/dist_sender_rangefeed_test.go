// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord_test

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/sasha-s/go-deadlock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type testRangefeedClient struct {
	rpc.RestrictedInternalClient
	count func()
}

func (c *testRangefeedClient) MuxRangeFeed(
	ctx context.Context, opts ...grpc.CallOption,
) (kvpb.Internal_MuxRangeFeedClient, error) {
	defer c.count()
	return c.RestrictedInternalClient.MuxRangeFeed(ctx, opts...)
}

type internalClientCounts struct {
	syncutil.Mutex
	counts map[rpc.RestrictedInternalClient]int
}

func (c *internalClientCounts) Inc(ic rpc.RestrictedInternalClient) {
	if c == nil {
		return
	}
	c.Lock()
	defer c.Unlock()
	c.counts[ic]++
}

type countConnectionsTransport struct {
	kvcoord.Transport
	counts *internalClientCounts
}

var _ kvcoord.Transport = (*countConnectionsTransport)(nil)

type testFeedCtxKey struct{}

func (c *countConnectionsTransport) NextInternalClient(
	ctx context.Context,
) (rpc.RestrictedInternalClient, error) {
	client, err := c.Transport.NextInternalClient(ctx)
	if err != nil {
		return nil, err
	}

	// Use regular client if we're not running this tests rangefeed.
	if ctx.Value(testFeedCtxKey{}) == nil {
		return client, nil
	}

	tc := &testRangefeedClient{
		RestrictedInternalClient: client,
	}

	tc.count = func() {
		if c.counts != nil {
			c.counts.Inc(tc)
		}
	}

	return tc, nil
}

func makeTransportFactory(
	counts *internalClientCounts,
) func(kvcoord.TransportFactory) kvcoord.TransportFactory {
	return func(factory kvcoord.TransportFactory) kvcoord.TransportFactory {
		return func(options kvcoord.SendOptions, slice kvcoord.ReplicaSlice) kvcoord.Transport {
			transport := factory(options, slice)
			countingTransport := &countConnectionsTransport{
				Transport: transport,
				counts:    counts,
			}
			return countingTransport
		}
	}
}

// rangeFeed is a helper to execute rangefeed.  We are not using rangefeed library
// here because of circular dependencies.
// Returns cleanup function, which is safe to call multiple times, that shuts down
// and waits for the rangefeed to terminate.
func rangeFeed(
	dsI interface{},
	sp roachpb.Span,
	startFrom hlc.Timestamp,
	onValue func(event kvcoord.RangeFeedMessage),
	opts ...kvcoord.RangeFeedOption,
) func() {
	ds := dsI.(*kvcoord.DistSender)
	events := make(chan kvcoord.RangeFeedMessage)
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), testFeedCtxKey{}, struct{}{}))

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) (err error) {
		return ds.RangeFeed(ctx, []kvcoord.SpanTimePair{{Span: sp, StartAfter: startFrom}}, events, opts...)
	})
	g.GoCtx(func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ev := <-events:
				onValue(ev)
			}
		}
	})

	return func() {
		cancel()
		_ = g.Wait()
	}
}

// observeNValues returns on value handler which expects to see N rangefeed values,
// along with the channel which gets closed when requisite count of events has been seen.
func observeNValues(n int) (chan struct{}, func(ev kvcoord.RangeFeedMessage)) {
	var count = struct {
		syncutil.Mutex
		c int
	}{}
	allSeen := make(chan struct{})
	return allSeen, func(ev kvcoord.RangeFeedMessage) {
		if ev.Val != nil {
			count.Lock()
			defer count.Unlock()
			count.c++
			log.Infof(context.Background(), "Waiting N values: saw %d, want %d; current=%s", count.c, n, ev.Val.Key)
			if count.c == n {
				close(allSeen)
			}
		}
	}
}

func channelWaitWithTimeout(t *testing.T, ch chan struct{}) {
	t.Helper()
	timeOut := 30 * time.Second
	if util.RaceEnabled {
		timeOut *= 10
	}
	if syncutil.DeadlockEnabled {
		timeOut = 2 * deadlock.Opts.DeadlockTimeout
	}
	select {
	case <-ch:
	case <-time.After(timeOut):
		t.Fatal("test timed out")
	}
}

func TestMuxRangeFeedConnectsToNodeOnce(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	connCounts := &internalClientCounts{counts: make(map[rpc.RestrictedInternalClient]int)}
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // Turn off replication queues.

		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				KVClient: &kvcoord.ClientTestingKnobs{
					TransportFactory: makeTransportFactory(connCounts),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	ts := tc.Server(0)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	startTime := ts.Clock().Now()

	// Create a table, and split it so that we have multiple ranges, distributed across
	// test cluster nodes.
	sqlDB.ExecMultiple(t,
		`SET CLUSTER SETTING kv.rangefeed.enabled = true`,
		`ALTER DATABASE defaultdb  CONFIGURE ZONE USING num_replicas = 1`,
		`CREATE TABLE foo (key INT PRIMARY KEY)`,
		`INSERT INTO foo (key) SELECT * FROM generate_series(1, 1000)`,
		`ALTER TABLE foo SPLIT AT (SELECT * FROM generate_series(100, 900, 100))`,
	)

	for i := 100; i <= 900; i += 100 {
		storeID := 1 + i%3
		rowID := i
		testutils.SucceedsSoon(t, func() error {
			_, err := sqlDB.DB.ExecContext(context.Background(),
				"ALTER TABLE foo EXPERIMENTAL_RELOCATE VALUES (ARRAY[$1], $2)", storeID, rowID)
			return err
		})
	}

	fooDesc := desctestutils.TestingGetPublicTableDescriptor(
		ts.DB(), keys.SystemSQLCodec, "defaultdb", "foo")
	fooSpan := fooDesc.PrimaryIndexSpan(keys.SystemSQLCodec)

	allSeen, onValue := observeNValues(1000)
	closeFeed := rangeFeed(ts.DistSenderI(), fooSpan, startTime, onValue)
	defer closeFeed()
	channelWaitWithTimeout(t, allSeen)
	closeFeed() // Explicitly shutdown the feed to make sure counters no longer change.

	// Verify we connected to each node once.
	connCounts.Lock()
	defer connCounts.Unlock()
	for _, c := range connCounts.counts {
		require.Equal(t, 1, c)
	}
}

func TestMuxRangeCatchupScanQuotaReleased(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	ts := tc.Server(0)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	startTime := ts.Clock().Now()

	// Initial setup: only single catchup scan allowed.
	sqlDB.ExecMultiple(t,
		`SET CLUSTER SETTING kv.rangefeed.enabled = true`,
		`ALTER DATABASE defaultdb  CONFIGURE ZONE USING num_replicas = 1`,
		`CREATE TABLE foo (key INT PRIMARY KEY)`,
		`INSERT INTO foo (key) SELECT * FROM generate_series(1, 1000)`,
	)

	fooDesc := desctestutils.TestingGetPublicTableDescriptor(
		ts.DB(), keys.SystemSQLCodec, "defaultdb", "foo")
	fooSpan := fooDesc.PrimaryIndexSpan(keys.SystemSQLCodec)

	// This error causes rangefeed to restart after re-resolving spans, and causes
	// catchup scan quota acquisition.
	transientErrEvent := kvpb.RangeFeedEvent{
		Error: &kvpb.RangeFeedError{
			Error: *kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_RANGE_SPLIT)),
		}}
	noValuesExpected := func(event kvcoord.RangeFeedMessage) {
		panic("received value when none expected")
	}
	const numErrsToReturn = 100
	var numErrors atomic.Int32
	enoughErrors := make(chan struct{})
	closeFeed := rangeFeed(ts.DistSenderI(), fooSpan, startTime, noValuesExpected,
		kvcoord.TestingWithOnRangefeedEvent(
			func(_ context.Context, _ roachpb.Span, _ int64, event *kvpb.RangeFeedEvent) (skip bool, _ error) {
				*event = transientErrEvent
				if numErrors.Add(1) == numErrsToReturn {
					close(enoughErrors)
				}
				return false, nil
			}))
	defer closeFeed()
	channelWaitWithTimeout(t, enoughErrors)
}

// TestMuxRangeFeedDoesNotStallOnError tests that the mux rangefeed
// client does not stall forever when all replicas return errors for
// the initial MuxRangeFeed call.
func TestMuxRangeFeedDoesNotStallOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDuress(t, "slow test")

	ctx := context.Background()

	var (
		rfMethod = "/cockroach.roachpb.Internal/MuxRangeFeed"

		maxErrors   = 10
		shouldError atomic.Bool
		errCount    int
	)

	streamInterceptor := func(target string, class rpc.ConnectionClass) grpc.StreamClientInterceptor {
		return func(
			ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
			method string, streamer grpc.Streamer, opts ...grpc.CallOption,
		) (grpc.ClientStream, error) {
			if method == rfMethod {
				if shouldError.Load() && errCount <= maxErrors {
					errCount++
					return nil, errors.Newf("test error %d", errCount)
				}
			}
			return streamer(ctx, desc, cc, method, opts...)
		}
	}
	const numServers int = 3
	serverArgs := base.TestServerArgs{
		RetryOptions: retry.Options{
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     10 * time.Millisecond,
		},
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				ContextTestingKnobs: rpc.ContextTestingKnobs{
					StreamClientInterceptor: streamInterceptor,
				},
			},
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
		// Scan like a bat out of hell to ensure down-replication happens quickly.
		ScanInterval: 50 * time.Millisecond,
	}

	tc := testcluster.StartTestCluster(t, numServers, base.TestClusterArgs{ServerArgs: serverArgs})
	defer tc.Stopper().Stop(ctx)
	ts := tc.ApplicationLayer(0)

	startFrom := ts.Clock().Now()

	sqlDB := sqlutils.MakeSQLRunner(ts.SQLConn(t))

	// The goal here is to try make sure that the rangefeed is forced to
	// connect to a remote node. Local connections don't go through the
	// server interceptors set up above.
	sqlDB.ExecMultiple(t,
		`SET CLUSTER SETTING kv.rangefeed.enabled = true`,
		`SET CLUSTER SETTING kv.closed_timestamp.target_duration='100ms'`,
		`SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '50ms'`,
		`SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '50ms'`,
		`ALTER DATABASE defaultdb CONFIGURE ZONE USING num_replicas = 1`,
		`CREATE TABLE foo (key INT PRIMARY KEY)`,
	)

	// Waiting for the initial range to go start removing replicas makes the wait
	// below substantially shorter.
	testutils.SucceedsSoon(t, func() error {
		var replicaCount int
		sqlDB.QueryRow(t,
			"SELECT sum(cardinality(replicas)) FROM [SHOW RANGES FROM TABLE foo WITH DETAILS]").
			Scan(&replicaCount)
		if replicaCount != 1 {
			return errors.Newf("too many replicas: %d", replicaCount)
		}
		return nil
	})

	sqlDB.ExecMultiple(t,
		`INSERT INTO foo (key) SELECT * FROM generate_series(1, 100)`,
		`ALTER TABLE foo SPLIT AT (SELECT * FROM generate_series(10, 90, 10))`,
	)

	// We scatter and wait until we have at least one range without replicas on n1.
	testutils.SucceedsSoon(t, func() error {
		sqlDB.Exec(t, `ALTER TABLE foo SCATTER`)
		var nonLocalCount int
		sqlDB.QueryRow(t,
			"SELECT count(1) FROM [SHOW RANGES FROM TABLE foo WITH DETAILS] WHERE array_position(replicas, 1) IS NULL").
			Scan(&nonLocalCount)
		if nonLocalCount == 0 {
			return errors.New("at least one non-local range required for test")
		}
		return nil
	})

	fooDesc := desctestutils.TestingGetPublicTableDescriptor(
		ts.DB(), ts.Codec(), "defaultdb", "foo")
	fooSpan := fooDesc.PrimaryIndexSpan(ts.Codec())

	shouldError.Store(true)
	allSeen, onValue := observeNValues(100)
	closeFeed := rangeFeed(ts.DistSenderI(), fooSpan, startFrom, onValue)
	defer closeFeed()
	channelWaitWithTimeout(t, allSeen)
}

// Test to make sure the various metrics used by rangefeed are correctly
// updated during the lifetime of the rangefeed and when the rangefeed completes.
func TestRangeFeedMetricsManagement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	ts := tc.Server(0)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	startTime := ts.Clock().Now()

	// Insert 1000 rows, and split them into 10 ranges.
	const numRanges = 10
	sqlDB.ExecMultiple(t,
		`ALTER DATABASE defaultdb CONFIGURE ZONE USING num_replicas = 1`,
		`CREATE TABLE foo (key INT PRIMARY KEY)`,
		`INSERT INTO foo (key) SELECT * FROM generate_series(1, 1000)`,
		`ALTER TABLE foo SPLIT AT (SELECT * FROM generate_series(100, 900, 100))`,
	)

	fooDesc := desctestutils.TestingGetPublicTableDescriptor(
		ts.DB(), keys.SystemSQLCodec, "defaultdb", "foo")
	fooSpan := fooDesc.PrimaryIndexSpan(keys.SystemSQLCodec)

	metrics := kvcoord.TestingMakeRangeFeedMetrics()

	// Number of ranges for which we'll issue transient error.
	const numRangesToRetry int64 = 3
	// Number of ranges which we will block from completion.
	const numCatchupToBlock int64 = 2

	// Upon shutdown, make sure the metrics have correct values.
	defer func() {
		require.EqualValues(t, 0, metrics.RangefeedRanges.Value())
		require.EqualValues(t, 0, metrics.RangefeedLocalRanges.Value())

		// We injected numRangesToRetry transient errors during catchup scan.
		// It is possible however, that we will observe key-mismatch error when restarting
		// due to how we split the ranges above (i.e. there is a version of the range
		// that goes from e.g. 800-Max, and then there is correct version 800-900).
		// When iterating through the entire table span, we pick up correct version.
		// However, if we attempt to re-resolve single range, we may get incorrect/old
		// version that was cached.  Thus, we occasionally see additional transient restarts.
		require.GreaterOrEqual(t, metrics.Errors.RangefeedErrorCatchup.Count(), numRangesToRetry)
		require.GreaterOrEqual(t, metrics.Errors.RangefeedRestartRanges.Count(), numRangesToRetry)

		// Even though numCatchupToBlock ranges were blocked in the catchup scan phase,
		// the counter should be 0 once rangefeed is done.
		require.EqualValues(t, 0, metrics.RangefeedCatchupRanges.Value())

	}()

	frontier, err := span.MakeFrontier(fooSpan)
	require.NoError(t, err)
	frontier = span.MakeConcurrentFrontier(frontier)

	// This error causes rangefeed to restart.
	transientErrEvent := kvpb.RangeFeedEvent{
		Error: &kvpb.RangeFeedError{Error: *kvpb.NewError(&kvpb.StoreNotFoundError{})},
	}

	var numRetried atomic.Int64
	var numCatchupBlocked atomic.Int64
	skipSet := struct {
		syncutil.Mutex
		stuck roachpb.SpanGroup // Spans that are stuck in catchup scan.
		retry roachpb.SpanGroup // Spans we issued retry for.
	}{}
	const kindRetry = true
	const kindStuck = false
	shouldSkip := func(k roachpb.Key, kind bool) bool {
		skipSet.Lock()
		defer skipSet.Unlock()
		if kind == kindRetry {
			return skipSet.retry.Contains(k)
		}
		return skipSet.stuck.Contains(k)
	}

	ignoreValues := func(event kvcoord.RangeFeedMessage) {}
	closeFeed := rangeFeed(ts.DistSenderI(), fooSpan, startTime, ignoreValues,
		kvcoord.TestingWithRangeFeedMetrics(&metrics),
		kvcoord.TestingWithOnRangefeedEvent(
			func(ctx context.Context, s roachpb.Span, _ int64, event *kvpb.RangeFeedEvent) (skip bool, _ error) {
				switch t := event.GetValue().(type) {
				case *kvpb.RangeFeedValue:
					// If we previously arranged for the range to be skipped (stuck catchup scan),
					// then skip any value that belongs to the skipped range.
					// This is only needed for mux rangefeed, since regular rangefeed just blocks.
					return shouldSkip(t.Key, kindStuck), nil
				case *kvpb.RangeFeedCheckpoint:
					if checkpoint := t; checkpoint.Span.Contains(s) {
						if checkpoint.ResolvedTS.IsEmpty() {
							return true, nil
						}

						// Skip any subsequent checkpoint if we previously arranged for
						// range to be skipped.
						if shouldSkip(checkpoint.Span.Key, kindStuck) {
							return true, nil
						}

						if !shouldSkip(checkpoint.Span.Key, kindRetry) && numRetried.Add(1) <= numRangesToRetry {
							// Return transient error for this range, but do this only once per range.
							skipSet.Lock()
							skipSet.retry.Add(checkpoint.Span)
							skipSet.Unlock()
							log.Infof(ctx, "skipping span %s", checkpoint.Span)
							*event = transientErrEvent
							return false, nil
						}

						_, err := frontier.Forward(checkpoint.Span, checkpoint.ResolvedTS)
						if err != nil {
							return false, err
						}

						if numCatchupBlocked.Add(1) <= numCatchupToBlock {
							// Mux rangefeed can't block single range, so just skip this event
							// and arrange for other events belonging to this range to be skipped as well.
							skipSet.Lock()
							skipSet.stuck.Add(checkpoint.Span)
							skipSet.Unlock()
							log.Infof(ctx, "skipping stuck span %s", checkpoint.Span)
							return true /* skip */, nil
						}
					}
				}

				return false, nil
			}))
	defer closeFeed()

	// Wait for the test frontier to advance.  Once it advances,
	// we know the rangefeed is started, all ranges are running (even if some of them are blocked).
	testutils.SucceedsWithin(t, func() error {
		if frontier.Frontier().IsEmpty() {
			return errors.Newf("waiting for frontier advance: %s", frontier.String())
		}
		return nil
	}, 10*time.Second)

	// At this point, we know the rangefeed for all ranges are running.
	require.EqualValues(t, numRanges, metrics.RangefeedRanges.Value(), frontier.String())

	// All ranges expected to be local.
	require.EqualValues(t, numRanges, metrics.RangefeedLocalRanges.Value(), frontier.String())

	// We also know that we have blocked numCatchupToBlock ranges in their catchup scan.
	require.EqualValues(t, numCatchupToBlock, metrics.RangefeedCatchupRanges.Value())
}

// TestRangefeedRangeObserver ensures the kvcoord.WithRangeObserver option
// works correctly.
func TestRangefeedRangeObserver(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	ts := tc.Server(0)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	kvserver.RangefeedEnabled.Override(
		context.Background(), &ts.ClusterSettings().SV, true)

	sqlDB.ExecMultiple(t,
		`CREATE TABLE foo (key INT PRIMARY KEY)`,
		`INSERT INTO foo (key) SELECT * FROM generate_series(1, 4)`,
		`ALTER TABLE foo SPLIT AT (SELECT * FROM generate_series(1, 4, 1))`,
	)
	defer func() {
		sqlDB.Exec(t, `DROP TABLE foo`)
	}()

	fooDesc := desctestutils.TestingGetPublicTableDescriptor(
		ts.DB(), keys.SystemSQLCodec, "defaultdb", "foo")
	fooSpan := fooDesc.PrimaryIndexSpan(keys.SystemSQLCodec)

	ignoreValues := func(event kvcoord.RangeFeedMessage) {}

	// Set up an observer to continuously poll for the list of ranges
	// being watched.
	var observedRangesMu syncutil.Mutex
	observedRanges := make(map[string]struct{})
	ctx2, cancel := context.WithCancel(context.Background())
	g := ctxgroup.WithContext(ctx2)
	defer func() {
		cancel()
		err := g.Wait()
		// Ensure the observer goroutine terminates gracefully via context cancellation.
		require.True(t, testutils.IsError(err, "context canceled"))
	}()
	observer := func(fn kvcoord.ForEachRangeFn) {
		g.GoCtx(func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(200 * time.Millisecond):
				}
				observedRangesMu.Lock()
				observedRanges = make(map[string]struct{})
				err := fn(func(rfCtx kvcoord.RangeFeedContext, feed kvcoord.PartialRangeFeed) error {
					observedRanges[feed.Span.String()] = struct{}{}
					return nil
				})
				observedRangesMu.Unlock()
				if err != nil {
					return err
				}
			}
		})
	}

	closeFeed := rangeFeed(ts.DistSenderI(), fooSpan, ts.Clock().Now(), ignoreValues,
		kvcoord.WithRangeObserver(observer))
	defer closeFeed()

	makeSpan := func(suffix string) string {
		return fmt.Sprintf("/Table/%d/%s", fooDesc.GetID(), suffix)
	}

	// The initial set of ranges we expect to observe.
	expectedRanges := map[string]struct{}{
		makeSpan("1{-/1}"):  {},
		makeSpan("1/{1-2}"): {},
		makeSpan("1/{2-3}"): {},
		makeSpan("1/{3-4}"): {},
		makeSpan("{1/4-2}"): {},
	}
	checkExpectedRanges := func() {
		testutils.SucceedsWithin(t, func() error {
			observedRangesMu.Lock()
			defer observedRangesMu.Unlock()
			if !reflect.DeepEqual(observedRanges, expectedRanges) {
				return errors.Newf("expected ranges %v, but got %v", expectedRanges, observedRanges)
			}
			return nil
		}, 10*time.Second)
	}
	checkExpectedRanges()

	// Add another range and ensure we can observe it.
	sqlDB.ExecMultiple(t,
		`INSERT INTO FOO VALUES(5)`,
		`ALTER TABLE foo SPLIT AT VALUES(5)`,
	)
	expectedRanges = map[string]struct{}{
		makeSpan("1{-/1}"):  {},
		makeSpan("1/{1-2}"): {},
		makeSpan("1/{2-3}"): {},
		makeSpan("1/{3-4}"): {},
		makeSpan("1/{4-5}"): {},
		makeSpan("{1/5-2}"): {},
	}
	checkExpectedRanges()
}

// TestMuxRangeFeedCanCloseStream verifies stream termination functionality in mux rangefeed.
func TestMuxRangeFeedCanCloseStream(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	ts := tc.Server(0)
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	// Insert 1000 rows, and split them into 10 ranges.
	sqlDB.ExecMultiple(t,
		`SET CLUSTER SETTING kv.rangefeed.enabled = true`,
		`SET CLUSTER SETTING kv.closed_timestamp.target_duration='100ms'`,
		`ALTER DATABASE defaultdb  CONFIGURE ZONE USING num_replicas = 1`,
		`CREATE TABLE foo (key INT PRIMARY KEY)`,
		`INSERT INTO foo (key) SELECT * FROM generate_series(1, 1000)`,
		`ALTER TABLE foo SPLIT AT (SELECT * FROM generate_series(100, 900, 100))`,
	)

	fooDesc := desctestutils.TestingGetPublicTableDescriptor(
		ts.DB(), keys.SystemSQLCodec, "defaultdb", "foo")
	fooSpan := fooDesc.PrimaryIndexSpan(keys.SystemSQLCodec)

	frontier, err := span.MakeFrontier(fooSpan)
	require.NoError(t, err)
	frontier = span.MakeConcurrentFrontier(frontier)

	expectFrontierAdvance := func() {
		t.Helper()
		// Closed timestamp for range advances every100ms.  We'll require frontier to
		// advance a bit more thn that.
		threshold := frontier.Frontier().AddDuration(250 * time.Millisecond)
		testutils.SucceedsWithin(t, func() error {
			if frontier.Frontier().Less(threshold) {
				return errors.Newf("waiting for frontier advance to at least %s", threshold)
			}
			return nil
		}, 10*time.Second)
	}

	var observedStreams syncutil.Set[int64]
	var capturedSender atomic.Value

	ignoreValues := func(event kvcoord.RangeFeedMessage) {}
	var numRestartStreams atomic.Int32

	closeFeed := rangeFeed(ts.DistSenderI(), fooSpan, ts.Clock().Now(), ignoreValues,
		kvcoord.TestingWithMuxRangeFeedRequestSenderCapture(
			// We expect a single mux sender since we have 1 node in this test.
			func(nodeID roachpb.NodeID, capture func(request *kvpb.RangeFeedRequest) error) {
				capturedSender.Store(capture)
			},
		),
		kvcoord.TestingWithOnRangefeedEvent(
			func(ctx context.Context, s roachpb.Span, streamID int64, event *kvpb.RangeFeedEvent) (skip bool, _ error) {
				switch t := event.GetValue().(type) {
				case *kvpb.RangeFeedCheckpoint:
					observedStreams.Add(streamID)
					_, err := frontier.Forward(t.Span, t.ResolvedTS)
					if err != nil {
						return true, err
					}
				case *kvpb.RangeFeedError:
					// Keep track of mux errors due to RangeFeedRetryError_REASON_RANGEFEED_CLOSED.
					// Those results when we issue CloseStream request.
					err := t.Error.GoError()
					log.Infof(ctx, "Got err: %v", err)
					var retryErr *kvpb.RangeFeedRetryError
					if ok := errors.As(err, &retryErr); ok && retryErr.Reason == kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED {
						numRestartStreams.Add(1)
					}
				}

				return false, nil
			}),
	)
	defer closeFeed()

	// Wait until we capture mux rangefeed request sender.  There should only be 1.
	var muxRangeFeedRequestSender func(req *kvpb.RangeFeedRequest) error
	testutils.SucceedsWithin(t, func() error {
		v, ok := capturedSender.Load().(func(request *kvpb.RangeFeedRequest) error)
		if ok {
			muxRangeFeedRequestSender = v
			return nil
		}
		return errors.New("waiting to capture mux rangefeed request sender.")
	}, 10*time.Second)

	cancelledStreams := make(map[int64]struct{})
	for i := 0; i < 5; i++ {
		// Wait for the test frontier to advance.  Once it advances,
		// we know the rangefeed is started, all ranges are running.
		expectFrontierAdvance()

		// Pick some number of streams to close. Since syncutil.Map iteration order
		// is non-deterministic, we'll pick few random streams.
		initialClosed := numRestartStreams.Load()
		numToCancel := 1 + rand.Int31n(3)
		var numCancelled int32 = 0
		observedStreams.Range(func(streamID int64) bool {
			if _, wasCancelled := cancelledStreams[streamID]; wasCancelled {
				return true // try another stream.
			}
			numCancelled++
			cancelledStreams[streamID] = struct{}{}
			req, err := kvcoord.NewCloseStreamRequest(ctx, ts.ClusterSettings(), streamID)
			require.NoError(t, err)
			require.NoError(t, muxRangeFeedRequestSender(req))
			return numCancelled < numToCancel
		})

		// Observe numToCancel errors.
		testutils.SucceedsWithin(t, func() error {
			numRestarted := numRestartStreams.Load()
			if numRestarted == initialClosed+numCancelled {
				return nil
			}
			return errors.Newf("waiting for %d streams to be closed (%d so far)", numCancelled, numRestarted-initialClosed)
		}, 10*time.Second)

		// When we close the stream(s), the rangefeed server responds with a retryable error.
		// Mux rangefeed should retry, and thus we expect frontier to keep advancing.
	}
}

// TestMuxRangeFeedDoesNotDeadlockWithLocalStreams verifies mux rangefeed does not
// deadlock when running against many local ranges.  Local ranges use local RPC
// bypass (rpc/context.go) which utilize buffered channels for client/server streaming
// RPC communication.
func TestMuxRangeFeedDoesNotDeadlockWithLocalStreams(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	if !syncutil.DeadlockEnabled {
		t.Log("skipping test: it requires deadlock detection enabled.")
		return
	}

	// Lower syncutil deadlock timeout.
	deadlock.Opts.DeadlockTimeout = 2 * time.Minute

	// Make deadlock more likely: use unbuffered channel.
	defer rpc.TestingSetLocalStreamChannelBufferSize(0)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	ts := tc.Server(0).ApplicationLayer()
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	// Insert 1000 rows, and split them into many ranges.
	sqlDB.ExecMultiple(t,
		`SET CLUSTER SETTING kv.rangefeed.enabled = true`,
		`SET CLUSTER SETTING kv.closed_timestamp.target_duration='100ms'`,
		`ALTER DATABASE defaultdb  CONFIGURE ZONE USING num_replicas = 1`,
		`CREATE TABLE foo (key INT PRIMARY KEY)`,
	)

	startFrom := ts.Clock().Now()

	sqlDB.ExecMultiple(t,
		`INSERT INTO foo (key) SELECT * FROM generate_series(1, 1000)`,
		`ALTER TABLE foo SPLIT AT (SELECT * FROM generate_series(100, 900, 20))`,
	)

	fooDesc := desctestutils.TestingGetPublicTableDescriptor(
		ts.DB(), keys.SystemSQLCodec, "defaultdb", "foo")
	fooSpan := fooDesc.PrimaryIndexSpan(keys.SystemSQLCodec)

	allSeen, onValue := observeNValues(1000)
	closeFeed := rangeFeed(ts.DistSenderI(), fooSpan, startFrom, onValue,
		kvcoord.TestingWithBeforeSendRequest(func() {
			// Prior to sending rangefeed request, block for just a bit
			// to make deadlock more likely.
			time.Sleep(100 * time.Millisecond)
		}),
	)
	defer closeFeed()
	channelWaitWithTimeout(t, allSeen)
}
