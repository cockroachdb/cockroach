// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/logtags"
)

//go:generate mockgen -package=rangefeed -source rangefeed.go -destination=mocks_generated.go .

// TODO(ajwerner): Expose hooks for metrics.
// TODO(ajwerner): Expose access to checkpoints and the frontier.
// TODO(ajwerner): Expose better control over how the exponential backoff gets
// reset when the feed has been running successfully for a while.

// kvDB is an adapter to the underlying KV store.
type kvDB interface {

	// RangeFeed runs a rangefeed on a given span with the given arguments.
	// It encapsulates the RangeFeed method on roachpb.Internal.
	RangeFeed(
		ctx context.Context,
		span roachpb.Span,
		startFrom hlc.Timestamp,
		withDiff bool,
		eventC chan<- *roachpb.RangeFeedEvent,
	) error

	// Scan encapsulates scanning a key span at a given point in time. The method
	// deals with pagination, calling the caller back for each row. Note that
	// the API does not require that the rows be ordered to allow for future
	// parallelism.
	Scan(
		ctx context.Context,
		span roachpb.Span,
		asOf hlc.Timestamp,
		rowFn func(value roachpb.KeyValue),
	) error
}

// Factory is used to construct RangeFeeds.
type Factory struct {
	stopper *stop.Stopper
	client  kvDB
	knobs   *TestingKnobs
}

// TestingKnobs is used to inject behavior into a rangefeed for testing.
type TestingKnobs struct {

	// OnRangefeedRestart is called when a rangefeed restarts.
	OnRangefeedRestart func()
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (t TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)

// NewFactory constructs a new Factory.
func NewFactory(stopper *stop.Stopper, db *kv.DB, knobs *TestingKnobs) (*Factory, error) {
	kvDB, err := newDBAdapter(db)
	if err != nil {
		return nil, err
	}
	return newFactory(stopper, kvDB, knobs), nil
}

func newFactory(stopper *stop.Stopper, client kvDB, knobs *TestingKnobs) *Factory {
	return &Factory{
		stopper: stopper,
		client:  client,
		knobs:   knobs,
	}
}

// RangeFeed constructs a new RangeFeed.
//
// The range feed can be stopped via Close(); otherwise, it will stop when the
// server shuts down.
//
// The only error which can be returned will indicate that the server is being
// shut down.
func (f *Factory) RangeFeed(
	ctx context.Context,
	name string,
	sp roachpb.Span,
	initialTimestamp hlc.Timestamp,
	onValue func(ctx context.Context, value *roachpb.RangeFeedValue),
	options ...Option,
) (_ *RangeFeed, err error) {
	r := RangeFeed{
		client:  f.client,
		stopper: f.stopper,
		knobs:   f.knobs,

		initialTimestamp: initialTimestamp,
		span:             sp,
		onValue:          onValue,

		stopped: make(chan struct{}),
	}
	initConfig(&r.config, options)

	// Maintain a frontier in order to resume at a reasonable timestamp.
	// TODO(ajwerner): Consider exposing the frontier through a RangeFeed method.
	// Doing so would require some synchronization.
	frontier, err := span.MakeFrontier(sp)
	if err != nil {
		return nil, err
	}
	if _, err := frontier.Forward(sp, initialTimestamp); err != nil {
		return nil, err
	}
	runWithFrontier := func(ctx context.Context) {
		r.run(ctx, frontier)
	}

	ctx = logtags.AddTag(ctx, "rangefeed", name)
	ctx, r.cancel = f.stopper.WithCancelOnQuiesce(ctx)
	if err := f.stopper.RunAsyncTask(ctx, "rangefeed", runWithFrontier); err != nil {
		r.cancel()
		return nil, err
	}
	return &r, nil
}

// RangeFeed represents a running RangeFeed.
type RangeFeed struct {
	config
	client  kvDB
	stopper *stop.Stopper
	knobs   *TestingKnobs

	initialTimestamp hlc.Timestamp

	span    roachpb.Span
	onValue func(ctx context.Context, value *roachpb.RangeFeedValue)

	closeOnce sync.Once
	cancel    context.CancelFunc
	stopped   chan struct{}
}

// Close closes the RangeFeed and waits for it to shut down.
// Close is idempotent.
func (f *RangeFeed) Close() {
	f.closeOnce.Do(func() {
		f.cancel()
		<-f.stopped
	})
}

// Run the rangefeed in a loop in the case of failure, likely due to node
// failures or general unavailability. If the rangefeed runs successfully for at
// least this long, then after subsequent failures we would like to reset the
// exponential backoff to experience long delays between retry attempts.
// This is the threshold of successful running after which the backoff state
// will be reset.
const resetThreshold = 30 * time.Second

// run will run the RangeFeed until the context is canceled or if the client
// indicates that an initial scan error is non-recoverable.
func (f *RangeFeed) run(ctx context.Context, frontier *span.Frontier) {
	defer close(f.stopped)
	r := retry.StartWithCtx(ctx, f.retryOptions)
	restartLogEvery := log.Every(10 * time.Second)
	if done := f.maybeRunInitialScan(ctx, &restartLogEvery, &r); done {
		return
	}

	// Check the context before kicking off a rangefeed.
	if ctx.Err() != nil {
		return
	}

	// TODO(ajwerner): Consider adding event buffering. Doing so would require
	// draining when the rangefeed fails.
	eventCh := make(chan *roachpb.RangeFeedEvent)
	errCh := make(chan error)

	for i := 0; r.Next(); i++ {

		// TODO(ajwerner): Figure out what to do if the rangefeed falls behind to
		// a point where the frontier timestamp precedes the GC threshold and thus
		// will never work. Perhaps an initial scan could be performed again for
		// some users. The API currently doesn't make that easy. Perhaps a callback
		// should be called in order to allow the client to kill the process or
		// something like that.
		ts := frontier.Frontier()
		log.VEventf(ctx, 1, "starting rangefeed from %v on %v", ts, f.span)
		start := timeutil.Now()

		// Note that the below channel send will not block forever because
		// processEvents will wait for the worker to send. RunWorker is safe here
		// because processEvents is guaranteed to consume the error before
		// returning.
		if err := f.stopper.RunAsyncTask(ctx, "rangefeed", func(ctx context.Context) {
			errCh <- f.client.RangeFeed(ctx, f.span, ts, f.withDiff, eventCh)
		}); err != nil {
			log.VEventf(ctx, 1, "exiting rangefeed due to stopper")
			return
		}

		err := f.processEvents(ctx, frontier, eventCh, errCh)
		if err != nil && ctx.Err() == nil && restartLogEvery.ShouldLog() {
			log.Warningf(ctx, "rangefeed failed %d times, restarting: %v",
				log.Safe(i), err)
		}
		if ctx.Err() != nil {
			log.VEventf(ctx, 1, "exiting rangefeed")
			return
		}

		ranFor := timeutil.Since(start)
		log.VEventf(ctx, 1, "restarting rangefeed for %v after %v",
			log.Safe(f.span), ranFor)
		if f.knobs != nil && f.knobs.OnRangefeedRestart != nil {
			f.knobs.OnRangefeedRestart()
		}

		// If the rangefeed ran successfully for long enough, reset the retry
		// state so that the exponential backoff begins from its minimum value.
		if ranFor > resetThreshold {
			i = 1
			r.Reset()
		}
	}
}

// maybeRunInitialScan will attempt to perform an initial data scan if one was
// requested. It will retry in the face of errors and will only return upon
// success, context cancellation, or an error handling function which indicates
// that an error is unrecoverable. The return value will be true if the context
// was canceled or if the OnInitialScanError function indicated that the
// RangeFeed should stop.
func (f *RangeFeed) maybeRunInitialScan(
	ctx context.Context, n *log.EveryN, r *retry.Retry,
) (canceled bool) {
	if !f.withInitialScan {
		return false // canceled
	}
	scan := func(kv roachpb.KeyValue) {
		v := roachpb.RangeFeedValue{
			Key:   kv.Key,
			Value: kv.Value,
		}

		// Mark the data as occurring at the initial timestamp, which is the
		// timestamp at which it was read.
		v.Value.Timestamp = f.initialTimestamp

		// Supply the value from the scan as also the previous value to avoid
		// indicating that the value was previously deleted.
		if f.withDiff {
			v.PrevValue = v.Value
		}

		// It's something of a bummer that we must allocate a new value for each
		// of these but the contract doesn't indicate that the value cannot be
		// retained so we have to assume that the callback may retain the value.
		f.onValue(ctx, &v)
	}
	for r.Next() {
		if err := f.client.Scan(ctx, f.span, f.initialTimestamp, scan); err != nil {
			if f.onInitialScanError != nil {
				if shouldStop := f.onInitialScanError(ctx, err); shouldStop {
					log.VEventf(ctx, 1, "stopping due to error: %v", err)
					return true
				}
			}
			if n.ShouldLog() {
				log.Warningf(ctx, "failed to perform initial scan: %v", err)
			}
		} else /* err == nil */ {
			if f.onInitialScanDone != nil {
				f.onInitialScanDone(ctx)
			}
			break
		}
	}
	return ctx.Err() != nil // canceled
}

// processEvents processes events sent by the rangefeed on the eventCh. It waits
// for the rangefeed to signal that it has exited by sending on errCh.
func (f *RangeFeed) processEvents(
	ctx context.Context,
	frontier *span.Frontier,
	eventCh <-chan *roachpb.RangeFeedEvent,
	errCh <-chan error,
) error {
	for {
		select {
		case ev := <-eventCh:
			switch {
			case ev.Val != nil:
				f.onValue(ctx, ev.Val)
			case ev.Checkpoint != nil:
				if _, err := frontier.Forward(ev.Checkpoint.Span, ev.Checkpoint.ResolvedTS); err != nil {
					return err
				}
			case ev.Error != nil:
				// Intentionally do nothing, we'll get an error returned from the
				// call to RangeFeed.
			}
		case <-ctx.Done():
			// Ensure that the RangeFeed goroutine stops.
			<-errCh
			return ctx.Err()
		case err := <-errCh:
			return err
		}
	}
}
