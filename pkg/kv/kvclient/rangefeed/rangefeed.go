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
	"sync/atomic"
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
	"github.com/cockroachdb/errors"
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

// EventHandler is a rangefeed event handler. We use an interface such that
// callers must explicitly handle each event type (if just to ignore them),
// especially as new event types are added.
type EventHandler interface {
	// OnValue is called when the rangefeed emits a key/value write.
	OnValue(context.Context, *roachpb.RangeFeedValue)

	// OnCheckpoint is called when a rangefeed checkpoint occurs.
	OnCheckpoint(context.Context, *roachpb.RangeFeedCheckpoint)

	// OnFrontierAdvance is called when the rangefeed frontier is advanced with
	// the new frontier timestamp.
	OnFrontierAdvance(context.Context, hlc.Timestamp)

	// OnUnrecoverableError is called when the rangefeed exits with an unrecoverable
	// error (preventing internal retries). One example is when the rangefeed falls
	// behind to a point where the frontier timestamp precedes the GC threshold, and
	// thus will never work. The callback lets callers find out about such errors
	// (possibly, in our example, to start a new rangefeed with an initial scan).
	OnUnrecoverableError(context.Context, error)
}

// InitialScanHandler is a rangefeed initial scan handler. It is separate from
// EventHandler such that only callers who need scans must implement it.
type InitialScanHandler interface {
	// OnInitialScanDone is called when an initial scan is finished, before any
	// rows from the rangefeed are supplied. WithInitialScan() must be enabled.
	OnInitialScanDone(context.Context)

	// OnInitialScanError is called when an initial scan encounters an error. It
	// allows the caller to tell the RangeFeed to stop as opposed to retrying
	// endlessly.
	OnInitialScanError(context.Context, error) (shouldFail bool)
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

// RangeFeed constructs a new rangefeed and runs it in an async task.
//
// The rangefeed can be stopped via Close(); otherwise, it will stop when the
// server shuts down.
//
// The only error which can be returned will indicate that the server is being
// shut down.
func (f *Factory) RangeFeed(
	ctx context.Context,
	name string,
	sp roachpb.Span,
	initialTimestamp hlc.Timestamp,
	eventHandler EventHandler,
	options ...Option,
) (_ *RangeFeed, err error) {
	r := f.New(name, sp, initialTimestamp, eventHandler, options...)
	if err := r.Start(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

// New constructs a new RangeFeed (without running it).
func (f *Factory) New(
	name string,
	sp roachpb.Span,
	initialTimestamp hlc.Timestamp,
	eventHandler EventHandler,
	options ...Option,
) *RangeFeed {
	r := RangeFeed{
		client:  f.client,
		stopper: f.stopper,
		knobs:   f.knobs,

		initialTimestamp: initialTimestamp,
		name:             name,
		span:             sp,
		eventHandler:     eventHandler,

		stopped: make(chan struct{}),
	}
	initConfig(&r.config, options)
	return &r
}

// OnValue is called for each rangefeed value.
type OnValue func(ctx context.Context, value *roachpb.RangeFeedValue)

// RangeFeed represents a running RangeFeed.
type RangeFeed struct {
	config
	name    string
	client  kvDB
	stopper *stop.Stopper
	knobs   *TestingKnobs

	initialTimestamp hlc.Timestamp

	span         roachpb.Span
	eventHandler EventHandler

	closeOnce sync.Once
	cancel    context.CancelFunc
	stopped   chan struct{}

	started int32 // accessed atomically
}

// Start kicks off the rangefeed in an async task, it can only be invoked once.
// All the installed callbacks (OnValue, OnCheckpoint, OnFrontierAdvance,
// OnInitialScanDone) are called in said async task in a single thread.
func (f *RangeFeed) Start(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&f.started, 0, 1) {
		return errors.AssertionFailedf("rangefeed already started")
	}

	// Maintain a frontier in order to resume at a reasonable timestamp.
	// TODO(ajwerner): Consider exposing the frontier through a RangeFeed method.
	// Doing so would require some synchronization.
	frontier, err := span.MakeFrontier(f.span)
	if err != nil {
		return err
	}
	if _, err := frontier.Forward(f.span, f.initialTimestamp); err != nil {
		return err
	}
	runWithFrontier := func(ctx context.Context) {
		f.run(ctx, frontier)
	}

	ctx = logtags.AddTag(ctx, "rangefeed", f.name)
	ctx, f.cancel = f.stopper.WithCancelOnQuiesce(ctx)
	if err := f.stopper.RunAsyncTask(ctx, "rangefeed", runWithFrontier); err != nil {
		f.cancel()
		return err
	}
	return nil
}

// Close closes the RangeFeed and waits for it to shut down; it does so
// idempotently. It waits for the currently running handler, if any, to complete
// and guarantees that no future handlers will be invoked after this point.
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
		if errors.HasType(err, &roachpb.BatchTimestampBeforeGCError{}) {
			f.eventHandler.OnUnrecoverableError(ctx, err)
			log.VEventf(ctx, 1, "exiting rangefeed due to internal error: %v", err)
			return
		}
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
			v.PrevValue.Timestamp = hlc.Timestamp{}
		}

		// It's something of a bummer that we must allocate a new value for each
		// of these but the contract doesn't indicate that the value cannot be
		// retained so we have to assume that the callback may retain the value.
		f.eventHandler.OnValue(ctx, &v)
	}
	for r.Next() {
		if err := f.client.Scan(ctx, f.span, f.initialTimestamp, scan); err != nil {
			if shouldStop := f.initialScanHandler.OnInitialScanError(ctx, err); shouldStop {
				log.VEventf(ctx, 1, "stopping due to error: %v", err)
				return true
			}
			if n.ShouldLog() {
				log.Warningf(ctx, "failed to perform initial scan: %v", err)
			}
		} else /* err == nil */ {
			f.initialScanHandler.OnInitialScanDone(ctx)
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
				f.eventHandler.OnValue(ctx, ev.Val)
			case ev.Checkpoint != nil:
				advanced, err := frontier.Forward(ev.Checkpoint.Span, ev.Checkpoint.ResolvedTS)
				if err != nil {
					return err
				}
				f.eventHandler.OnCheckpoint(ctx, ev.Checkpoint)
				if advanced {
					f.eventHandler.OnFrontierAdvance(ctx, frontier.Frontier())
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
