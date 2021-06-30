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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockClient struct {
	rangefeed func(
		ctx context.Context,
		span roachpb.Span,
		startFrom hlc.Timestamp,
		withDiff bool,
		eventC chan<- *roachpb.RangeFeedEvent,
	) error

	scan func(
		ctx context.Context,
		span roachpb.Span,
		asOf hlc.Timestamp,
		rowFn func(value roachpb.KeyValue),
	) error
}

func (m *mockClient) RangeFeed(
	ctx context.Context,
	span roachpb.Span,
	startFrom hlc.Timestamp,
	withDiff bool,
	eventC chan<- *roachpb.RangeFeedEvent,
) error {
	return m.rangefeed(ctx, span, startFrom, withDiff, eventC)
}

func (m *mockClient) Scan(
	ctx context.Context, span roachpb.Span, asOf hlc.Timestamp, rowFn func(value roachpb.KeyValue),
) error {
	return m.scan(ctx, span, asOf, rowFn)
}

var _ (rangefeed.KVDB) = (*mockClient)(nil)

// TestRangefeedMock utilizes the kvDB interface to test the behavior of the
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
			scan: func(ctx context.Context, span roachpb.Span, asOf hlc.Timestamp, rowFn func(value roachpb.KeyValue)) error {
				assert.Equal(t, ts, asOf)
				assert.Equal(t, sp, span)
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
		rows := make(chan *roachpb.RangeFeedValue)

		r, err := f.RangeFeed(ctx, "foo", sp, ts, func(ctx context.Context, value *roachpb.RangeFeedValue) {
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
				ctx context.Context, span roachpb.Span, asOf hlc.Timestamp, rowFn func(value roachpb.KeyValue),
			) error {
				t.Error("this should not be called")
				return nil
			},
			rangefeed: func(
				ctx context.Context, span roachpb.Span, startFrom hlc.Timestamp, withDiff bool, eventC chan<- *roachpb.RangeFeedEvent,
			) error {
				assert.False(t, withDiff) // it was not set
				sendEvent := func(ts hlc.Timestamp) {
					eventC <- &roachpb.RangeFeedEvent{
						Val: &roachpb.RangeFeedValue{
							Key: sp.Key,
						},
					}
				}
				iteration++
				switch {
				case iteration <= numRestartsBeforeCheckpoint:
					sendEvent(initialTS)
					assert.Equal(t, startFrom, initialTS)
					return errors.New("boom")
				case iteration == firstPartialCheckpoint:
					assert.Equal(t, startFrom, initialTS)
					eventC <- &roachpb.RangeFeedEvent{
						Checkpoint: &roachpb.RangeFeedCheckpoint{
							Span: roachpb.Span{
								Key:    sp.Key,
								EndKey: sp.Key.PrefixEnd(),
							},
							ResolvedTS: nextTS,
						},
					}
					sendEvent(initialTS)
					return errors.New("boom")
				case iteration == secondPartialCheckpoint:
					assert.Equal(t, startFrom, initialTS)
					eventC <- &roachpb.RangeFeedEvent{
						Checkpoint: &roachpb.RangeFeedCheckpoint{
							Span: roachpb.Span{
								Key:    sp.Key.PrefixEnd(),
								EndKey: sp.EndKey,
							},
							ResolvedTS: nextTS,
						},
					}
					sendEvent(nextTS)
					return errors.New("boom")
				case iteration == fullCheckpoint:
					// At this point the frontier should have a complete checkpoint at
					// nextTS.
					assert.Equal(t, startFrom, nextTS)
					eventC <- &roachpb.RangeFeedEvent{
						Checkpoint: &roachpb.RangeFeedCheckpoint{
							Span:       sp,
							ResolvedTS: lastTS,
						},
					}
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
		rows := make(chan *roachpb.RangeFeedValue)
		r, err := f.RangeFeed(ctx, "foo", sp, initialTS, func(
			ctx context.Context, value *roachpb.RangeFeedValue,
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
			scan: func(
				ctx context.Context, span roachpb.Span, asOf hlc.Timestamp, rowFn func(value roachpb.KeyValue),
			) error {
				t.Error("this should not be called")
				return nil
			},
			rangefeed: func(
				ctx context.Context, span roachpb.Span, startFrom hlc.Timestamp, withDiff bool, eventC chan<- *roachpb.RangeFeedEvent,
			) error {
				assert.True(t, withDiff)
				eventC <- &roachpb.RangeFeedEvent{
					Val: &roachpb.RangeFeedValue{
						Key: sp.Key,
					},
				}
				<-ctx.Done()
				return ctx.Err()
			},
		}
		f := rangefeed.NewFactoryWithDB(stopper, &mc, nil /* knobs */)
		rows := make(chan *roachpb.RangeFeedValue)
		r, err := f.RangeFeed(ctx, "foo", sp, hlc.Timestamp{}, func(
			ctx context.Context, value *roachpb.RangeFeedValue,
		) {
			rows <- value
		}, rangefeed.WithDiff())
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
		r, err := f.RangeFeed(ctx, "foo", sp, hlc.Timestamp{}, func(
			ctx context.Context, value *roachpb.RangeFeedValue,
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
			scan: func(ctx context.Context, span roachpb.Span, asOf hlc.Timestamp, rowFn func(value roachpb.KeyValue)) error {
				return errors.New("boom")
			},
		}, nil /* knobs */)
		done := make(chan struct{})
		r, err := f.RangeFeed(ctx, "foo", sp, hlc.Timestamp{}, func(
			ctx context.Context, value *roachpb.RangeFeedValue,
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
}

// TestBackoffOnRangefeedFailure ensures that the rangefeed is retried on
// failures.
func TestBackoffOnRangefeedFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	ctrl := gomock.NewController(t)
	db := rangefeed.NewMockkvDB(ctrl)

	// Make sure scan failure gets retried.
	db.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("scan failed"))
	db.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	// Make sure rangefeed is retried even after 3 failures, then succeed and cancel context
	// (which signals the rangefeed to shut down gracefully).
	db.EXPECT().RangeFeed(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(3).
		Return(errors.New("rangefeed failed"))
	db.EXPECT().RangeFeed(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(context.Context, roachpb.Span, hlc.Timestamp, bool, chan<- *roachpb.RangeFeedEvent) {
			cancel()
		}).
		Return(nil)

	f := rangefeed.NewFactoryWithDB(stopper, db, nil /* knobs */)
	r, err := f.RangeFeed(ctx, "foo",
		roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey},
		hlc.Timestamp{},
		func(ctx context.Context, value *roachpb.RangeFeedValue) {},
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
