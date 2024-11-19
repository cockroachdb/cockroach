// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockClient struct {
	rangefeed func(
		ctx context.Context,
		spans []roachpb.Span,
		startFrom hlc.Timestamp,
		eventC chan<- kvcoord.RangeFeedMessage,
	) error

	rangeFeedFromFrontier func(
		ctx context.Context,
		frontier span.Frontier,
		eventC chan<- kvcoord.RangeFeedMessage,
	) error

	scan func(
		ctx context.Context,
		spans []roachpb.Span,
		asOf hlc.Timestamp,
		rowFn func(value roachpb.KeyValue),
		rowsFn func(values []kv.KeyValue),
		cfg rangefeed.ScanConfig,
	) error
}

func (m *mockClient) RangeFeed(
	ctx context.Context,
	spans []roachpb.Span,
	startFrom hlc.Timestamp,
	eventC chan<- kvcoord.RangeFeedMessage,
	opts ...kvcoord.RangeFeedOption,
) error {
	return m.rangefeed(ctx, spans, startFrom, eventC)
}

func (m *mockClient) RangeFeedFromFrontier(
	ctx context.Context,
	frontier span.Frontier,
	eventC chan<- kvcoord.RangeFeedMessage,
	opts ...kvcoord.RangeFeedOption,
) error {
	return m.rangeFeedFromFrontier(ctx, frontier, eventC)
}

func (m *mockClient) Scan(
	ctx context.Context,
	spans []roachpb.Span,
	asOf hlc.Timestamp,
	rowFn func(value roachpb.KeyValue),
	rowsFn func(values []kv.KeyValue),
	cfg rangefeed.ScanConfig,
) error {
	return m.scan(ctx, spans, asOf, rowFn, rowsFn, cfg)
}

var _ rangefeed.KVDB = (*mockClient)(nil)

// TestRangefeedMock utilizes the DB interface to test the behavior of the
// RangeFeed.
func TestRangeFeedMock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	shortRetryOptions := retry.Options{
		InitialBackoff: time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
	}
	t.Run("scan retries", func(t *testing.T) {
		stopper := stop.NewStopper()
		ctx := context.Background()
		defer stopper.Stop(ctx)
		ctx, cancel := context.WithCancel(ctx)
		var i int
		sp := roachpb.Span{
			Key:    roachpb.Key("a"),
			EndKey: roachpb.Key("b"),
		}
		ts := hlc.Timestamp{WallTime: 1}
		row := roachpb.KeyValue{
			Key:   sp.Key,
			Value: roachpb.Value{},
		}
		const numFailures = 2
		mc := mockClient{
			scan: func(ctx context.Context, spans []roachpb.Span, asOf hlc.Timestamp,
				rowFn func(value roachpb.KeyValue), rowsFn func(_ []kv.KeyValue), cfg rangefeed.ScanConfig) error {
				assert.Equal(t, ts, asOf)
				assert.Equal(t, []roachpb.Span{sp}, spans)
				rowFn(row)
				if i++; i <= numFailures {
					return errors.New("boom")
				}
				// Ensure the rangefeed doesn't start up by canceling the context prior
				// to concluding the scan.
				cancel()
				return nil
			},
		}
		f := rangefeed.NewFactoryWithDB(stopper, &mc, nil /* knobs */)
		require.NotNil(t, f)
		rows := make(chan *kvpb.RangeFeedValue)

		r, err := f.RangeFeed(ctx, "foo", []roachpb.Span{sp}, ts, func(ctx context.Context, value *kvpb.RangeFeedValue) {
			rows <- value
		}, rangefeed.WithInitialScan(func(ctx context.Context) {
			close(rows)
		}), rangefeed.WithRetry(shortRetryOptions))
		require.NoError(t, err)
		require.NotNil(t, r)
		for i := 0; i < numFailures+1; i++ {
			r, ok := <-rows
			require.Equal(t, row.Key, r.Key)
			require.True(t, ok)
		}
		_, ok := <-rows
		require.False(t, ok)
		r.Close()
	})
	t.Run("changefeed retries", func(t *testing.T) {
		stopper := stop.NewStopper()
		ctx := context.Background()
		defer stopper.Stop(ctx)
		sp := roachpb.Span{
			Key:    roachpb.Key("a"),
			EndKey: roachpb.Key("c"),
		}
		initialTS := hlc.Timestamp{WallTime: 1}
		nextTS := initialTS.Next()
		lastTS := nextTS.Next()
		row := roachpb.KeyValue{
			Key:   sp.Key,
			Value: roachpb.Value{},
		}
		const (
			numRestartsBeforeCheckpoint = 3
			firstPartialCheckpoint      = numRestartsBeforeCheckpoint + 1
			secondPartialCheckpoint     = firstPartialCheckpoint + 1
			fullCheckpoint              = secondPartialCheckpoint + 1
			lastEvent                   = fullCheckpoint + 1
			totalRestarts               = lastEvent - 1
		)
		var iteration int
		var gotToTheEnd bool
		mc := mockClient{
			scan: func(
				ctx context.Context, spans []roachpb.Span, asOf hlc.Timestamp,
				rowFn func(value roachpb.KeyValue), rowsFn func(_ []kv.KeyValue), config rangefeed.ScanConfig,
			) error {
				t.Error("this should not be called")
				return nil
			},
			rangefeed: func(
				ctx context.Context, spans []roachpb.Span, startFrom hlc.Timestamp, eventC chan<- kvcoord.RangeFeedMessage,
			) error {
				sendEvent := func(ts hlc.Timestamp) {
					eventC <- kvcoord.RangeFeedMessage{
						RangeFeedEvent: &kvpb.RangeFeedEvent{
							Val: &kvpb.RangeFeedValue{
								Key: sp.Key,
							},
						}}
				}
				iteration++
				switch {
				case iteration <= numRestartsBeforeCheckpoint:
					sendEvent(initialTS)
					assert.Equal(t, startFrom, initialTS)
					return errors.New("boom")
				case iteration == firstPartialCheckpoint:
					assert.Equal(t, startFrom, initialTS)
					eventC <- kvcoord.RangeFeedMessage{RangeFeedEvent: &kvpb.RangeFeedEvent{
						Checkpoint: &kvpb.RangeFeedCheckpoint{
							Span: roachpb.Span{
								Key:    sp.Key,
								EndKey: sp.Key.PrefixEnd(),
							},
							ResolvedTS: nextTS,
						},
					}}
					sendEvent(initialTS)
					return errors.New("boom")
				case iteration == secondPartialCheckpoint:
					assert.Equal(t, startFrom, initialTS)
					eventC <- kvcoord.RangeFeedMessage{
						RangeFeedEvent: &kvpb.RangeFeedEvent{
							Checkpoint: &kvpb.RangeFeedCheckpoint{
								Span: roachpb.Span{
									Key:    sp.Key.PrefixEnd(),
									EndKey: sp.EndKey,
								},
								ResolvedTS: nextTS,
							},
						}}
					sendEvent(nextTS)
					return errors.New("boom")
				case iteration == fullCheckpoint:
					// At this point the frontier should have a complete checkpoint at
					// nextTS.
					assert.Equal(t, startFrom, nextTS)
					eventC <- kvcoord.RangeFeedMessage{
						RangeFeedEvent: &kvpb.RangeFeedEvent{
							Checkpoint: &kvpb.RangeFeedCheckpoint{
								Span:       sp,
								ResolvedTS: lastTS,
							},
						}}
					sendEvent(nextTS)
					return errors.New("boom")
				case iteration == lastEvent:
					// Send a last event.
					sendEvent(lastTS)
					gotToTheEnd = true
					<-ctx.Done()
					return ctx.Err()
				default:
					panic(iteration)
				}
			},
		}
		f := rangefeed.NewFactoryWithDB(stopper, &mc, nil /* knobs */)
		rows := make(chan *kvpb.RangeFeedValue)
		r, err := f.RangeFeed(ctx, "foo", []roachpb.Span{sp}, initialTS, func(
			ctx context.Context, value *kvpb.RangeFeedValue,
		) {
			rows <- value
		}, rangefeed.WithRetry(shortRetryOptions))
		require.NoError(t, err)
		require.NotNil(t, r)
		start := timeutil.Now()
		for i := 0; i < lastEvent; i++ {
			r := <-rows
			assert.Equal(t, row.Key, r.Key)
		}
		minimumBackoff := 850 * time.Microsecond // initialBackoff less jitter
		totalBackoff := timeutil.Since(start)
		require.Greater(t, totalBackoff.Nanoseconds(), (totalRestarts * minimumBackoff).Nanoseconds())
		r.Close()
		require.True(t, gotToTheEnd)
	})
	t.Run("withDiff", func(t *testing.T) {
		stopper := stop.NewStopper()
		ctx := context.Background()
		defer stopper.Stop(ctx)
		sp := roachpb.Span{
			Key:    roachpb.Key("a"),
			EndKey: roachpb.Key("c"),
		}
		mc := mockClient{
			scan: func(ctx context.Context, spans []roachpb.Span, asOf hlc.Timestamp,
				rowFn func(value roachpb.KeyValue), rowsFn func(_ []kv.KeyValue), config rangefeed.ScanConfig,
			) error {
				t.Error("this should not be called")
				return nil
			},
			rangefeed: func(
				ctx context.Context, spans []roachpb.Span, startFrom hlc.Timestamp, eventC chan<- kvcoord.RangeFeedMessage,
			) error {
				eventC <- kvcoord.RangeFeedMessage{
					RangeFeedEvent: &kvpb.RangeFeedEvent{
						Val: &kvpb.RangeFeedValue{
							Key: sp.Key,
						},
					}}
				<-ctx.Done()
				return ctx.Err()
			},
		}
		f := rangefeed.NewFactoryWithDB(stopper, &mc, nil /* knobs */)
		rows := make(chan *kvpb.RangeFeedValue)
		r, err := f.RangeFeed(ctx, "foo", []roachpb.Span{sp}, hlc.Timestamp{}, func(
			ctx context.Context, value *kvpb.RangeFeedValue,
		) {
			rows <- value
		}, rangefeed.WithDiff(true))
		require.NoError(t, err)
		<-rows
		r.Close()
	})
	t.Run("stopper already stopped", func(t *testing.T) {
		stopper := stop.NewStopper()
		ctx := context.Background()
		sp := roachpb.Span{
			Key:    roachpb.Key("a"),
			EndKey: roachpb.Key("c"),
		}
		stopper.Stop(ctx)
		f := rangefeed.NewFactoryWithDB(stopper, &mockClient{}, nil /* knobs */)
		r, err := f.RangeFeed(ctx, "foo", []roachpb.Span{sp}, hlc.Timestamp{}, func(
			ctx context.Context, value *kvpb.RangeFeedValue,
		) {
		})
		require.Nil(t, r)
		require.True(t, errors.Is(err, stop.ErrUnavailable), "%v", err)
	})
	t.Run("initial scan error", func(t *testing.T) {
		stopper := stop.NewStopper()
		ctx := context.Background()
		defer stopper.Stop(ctx)
		sp := roachpb.Span{
			Key:    roachpb.Key("a"),
			EndKey: roachpb.Key("c"),
		}
		var called int
		f := rangefeed.NewFactoryWithDB(stopper, &mockClient{
			scan: func(ctx context.Context, spans []roachpb.Span, asOf hlc.Timestamp,
				rowFn func(value roachpb.KeyValue), rowsFn func(_ []kv.KeyValue), config rangefeed.ScanConfig) error {
				return errors.New("boom")
			},
		}, nil /* knobs */)
		done := make(chan struct{})
		r, err := f.RangeFeed(ctx, "foo", []roachpb.Span{sp}, hlc.Timestamp{}, func(
			ctx context.Context, value *kvpb.RangeFeedValue,
		) {
		},
			rangefeed.WithInitialScan(nil),
			rangefeed.WithOnInitialScanError(func(ctx context.Context, err error) (shouldFail bool) {
				if called++; called <= 1 {
					close(done)
					return false
				}
				return true
			}))
		require.NotNil(t, r)
		require.NoError(t, err)
		<-done
		r.Close()
	})

	// Test that the initial scan of [a, z) makes progress and completes even when
	// the rangefeed is being fully restarted after errors during its initial scan
	// such as might happen if a job running a rangefeed is replans or restarts,
	// so long as that job passes progress previously made and persisted to the
	// restarted rangefeed via a frontier. In this test the persistance of the
	// frontier is simply that it is outside the loop used to run each rangefeed
	// but in a job a frontier visitor would likely be copying state out of tthe
	// rangefeed's frontier while it is running and persisting it somewhere to be
	// used to construct a new but non-empty frontier to pass to resumptions.
	t.Run("initial scan resumed from frontier makes progress", func(t *testing.T) {
		stopper := stop.NewStopper()
		ctx := context.Background()
		defer stopper.Stop(ctx)

		s := func(s, e string) roachpb.Span {
			return roachpb.Span{Key: roachpb.Key(s), EndKey: roachpb.Key(e)}
		}

		// Valid result spans represent partial successes while invalid spans here
		// result in synthetic scan errors.
		scanResults := []roachpb.Span{
			s("a", "c"), s("c", "f"), s("f", "g"), s("g", "z"),
		}
		resultCh := make(chan roachpb.Span, len(scanResults))
		for _, sp := range scanResults {
			resultCh <- sp
		}

		// Mock a rangefeed whose scan results will be partial successes or errors
		// from the above channel, and for which successfully "scanned" spans will
		// call the usual rangefeed initial scan span complete callback.
		f := rangefeed.NewFactoryWithDB(stopper, &mockClient{
			scan: func(ctx context.Context, requested []roachpb.Span, _ hlc.Timestamp,
				rowFn func(_ roachpb.KeyValue), rowsFn func(_ []kv.KeyValue), cfg rangefeed.ScanConfig) error {
				res := <-resultCh
				if err := cfg.OnSpanDone(ctx, res); err != nil {
					return err
				}
				require.Equal(t, res.Key, requested[0].Key, "rangefeed should have tried to scan remaining span")
				return errors.New("err")
			},
		}, nil)

		frontier, err := span.MakeFrontier(s("a", "z"))
		require.NoError(t, err)
		defer frontier.Release()

		var runs int
		done := false
		// Run our rangefeed up to 5 times, or until it reports completing its
		// initial scan, simulating a job performing an initial scan restarting.
		for ; !done && runs < 5; runs++ {
			// Sanity check that our frontier isn't empty (i.e. wasn't Released).
			require.Greater(t, frontier.Len(), 0, frontier.String())
			// Create a rangefeed that will abort on scan error, to give our test a
			// chance to stop it and create a new one, similar to how a real one might
			// get recreated if a job is resumed or replanned after something like a
			// node restart or liveness failure.
			r := f.New("test", hlc.Timestamp{WallTime: 1},
				func(_ context.Context, _ *kvpb.RangeFeedValue) {},
				rangefeed.WithInitialScan(func(_ context.Context) { done = true }),
				rangefeed.WithOnInitialScanError(func(_ context.Context, _ error) (shouldFail bool) {
					return true
				}),
			)
			err := r.StartFromFrontier(ctx, frontier)
			require.NoError(t, err)
			r.Close()
		}
		require.True(t, done)
		require.Equal(t, 5, runs)
	})
	// This subtest restarts a rangefeed several times from a frontier and asserts
	// that each emitted checkpoint pushes the initialised frontier forward.
	t.Run("resume rangefeed from frontier", func(t *testing.T) {
		stopper := stop.NewStopper()
		ctx := context.Background()
		defer stopper.Stop(ctx)

		rand, _ := randutil.NewTestRand()

		s := func(s, e string) roachpb.Span {
			return roachpb.Span{Key: roachpb.Key(s), EndKey: roachpb.Key(e)}
		}

		spans := []roachpb.Span{
			s("a", "c"), s("c", "f"), s("f", "g"), s("g", "z"),
		}
		fullSpan := roachpb.Span{Key: spans[0].Key, EndKey: spans[len(spans)-1].EndKey}

		getRandomSpan := func() roachpb.Span {
			return spans[rand.Intn(len(spans))]
		}

		getSpanTimestamp := func(frontier span.Frontier, given roachpb.Span) hlc.Timestamp {
			maxTS := hlc.MinTimestamp
			frontier.SpanEntries(given, func(sp roachpb.Span, ts hlc.Timestamp) (done span.OpResult) {
				if maxTS.Less(ts) {
					maxTS = ts
				}
				return span.ContinueMatch
			})
			return maxTS
		}

		frontier, err := span.MakeFrontier(spans...)
		require.NoError(t, err)

		// NB: in this mocked implementation of rangeedFromFrontier, we read from
		// the frontier while processEvents updates it. To avoid a race, we use a
		// concurrent frontier.
		externalFrontier := span.MakeConcurrentFrontier(frontier)
		defer externalFrontier.Release()

		done := make(chan struct{})
		mc := mockClient{
			scan: func(ctx context.Context, spans []roachpb.Span, asOf hlc.Timestamp,
				rowFn func(value roachpb.KeyValue), rowsFn func(_ []kv.KeyValue), config rangefeed.ScanConfig,
			) error {
				t.Error("this should not be called")
				return nil
			},
			rangeFeedFromFrontier: func(
				ctx context.Context, internalFrontier span.Frontier, eventC chan<- kvcoord.RangeFeedMessage,
			) error {
				for i := 0; i < 5; i++ {
					sp := getRandomSpan()
					ts := getSpanTimestamp(internalFrontier, sp)

					for j := 0; j < i+1; j++ {
						// Ensure we always forward a timestamp for a given span.
						ts = ts.Next()
					}
					eventC <- kvcoord.RangeFeedMessage{
						RangeFeedEvent: &kvpb.RangeFeedEvent{
							Checkpoint: &kvpb.RangeFeedCheckpoint{
								Span:       sp,
								ResolvedTS: ts,
							},
						}}
				}
				done <- struct{}{}
				<-ctx.Done()
				return ctx.Err()
			},
		}
		f := rangefeed.NewFactoryWithDB(stopper, &mc, nil /* knobs */)
		onValue := func(ctx context.Context, value *kvpb.RangeFeedValue) {}
		for i := 0; i < 5; i++ {
			var observedUpdates int
			// Make a copy of the externalFrontier, which the rangefeed will update in place.
			initFrontier, err := span.MakeFrontier(fullSpan)
			require.NoError(t, err)

			externalFrontier.Entries(func(sp roachpb.Span, ts hlc.Timestamp) (done span.OpResult) {
				_, err := initFrontier.Forward(sp, ts)
				require.NoError(t, err)
				return span.ContinueMatch
			})
			require.NoError(t, err)

			onCheckpoint := func(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {
				// Ensure the checkpoint timestamp is always greater than the initial timestamp.
				initFrontier.SpanEntries(checkpoint.Span, func(sp roachpb.Span, ts hlc.Timestamp) (done span.OpResult) {
					require.True(t, ts.Less(checkpoint.ResolvedTS), "checkpoint %s", checkpoint)
					return span.ContinueMatch
				})
				observedUpdates++
			}

			r := f.New("foo", hlc.Timestamp{}, onValue, rangefeed.WithOnCheckpoint(onCheckpoint))

			err = r.StartFromFrontier(ctx, externalFrontier)
			require.NoError(t, err)
			<-done
			r.Close()

			// Sanity check that udpates were observed.
			require.Greater(t, observedUpdates, 0)

			// Assert the external frontier advanced.
			require.NotEqual(t, externalFrontier.String(), initFrontier.String())
		}
	})
}

// TestBackoffOnRangefeedFailure ensures that the rangefeed is retried on
// failures.
func TestBackoffOnRangefeedFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	ctrl := gomock.NewController(t)
	db := rangefeed.NewMockDB(ctrl)

	// Make sure scan failure gets retried.
	db.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("scan failed"))
	db.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	// Make sure rangefeed is retried even after 3 failures, then succeed and cancel context
	// (which signals the rangefeed to shut down gracefully).
	db.EXPECT().RangeFeed(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(3).
		Return(errors.New("rangefeed failed"))
	db.EXPECT().RangeFeed(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(context.Context, []roachpb.Span, hlc.Timestamp, chan<- kvcoord.RangeFeedMessage, ...kvcoord.RangeFeedOption) {
			cancel()
		}).
		Return(nil)

	f := rangefeed.NewFactoryWithDB(stopper, db, nil /* knobs */)
	r, err := f.RangeFeed(ctx, "foo",
		[]roachpb.Span{{Key: keys.TableDataMin, EndKey: keys.TableDataMax}},
		hlc.Timestamp{},
		func(ctx context.Context, value *kvpb.RangeFeedValue) {},
		rangefeed.WithInitialScan(func(ctx context.Context) {}),
		rangefeed.WithRetry(retry.Options{InitialBackoff: time.Millisecond}),
	)
	require.NoError(t, err)
	defer r.Close()

	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
		require.Fail(t, "timed out waiting for retries")
	}
}
