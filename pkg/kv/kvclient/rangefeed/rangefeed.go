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

// TODO(ajwerner): Expose hooks for metrics.
// TODO(ajwerner): Expose access to checkpoints and the frontier.
// TODO(ajwerner): Expose better control over how the retrier gets reset.

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

	// Scan encapsulates scanning a keyspan at a given point in time. The method
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
}

// NewFactory constructs a new Factory.
func NewFactory(stopper *stop.Stopper, db *kv.DB) (*Factory, error) {
	kvDB, err := newDBAdapter(db)
	if err != nil {
		return nil, err
	}
	return newFactory(stopper, kvDB), nil
}

func newFactory(stopper *stop.Stopper, client kvDB) *Factory {
	return &Factory{
		stopper: stopper,
		client:  client,
	}
}

// RangeFeed constructs a new RangeFeed.
func (f *Factory) RangeFeed(
	ctx context.Context,
	name string,
	span roachpb.Span,
	initialTimestamp hlc.Timestamp,
	onValue func(ctx context.Context, value *roachpb.RangeFeedValue),
	options ...Option,
) (_ *RangeFeed, err error) {
	r := RangeFeed{
		client:  f.client,
		stopper: f.stopper,

		initialTimestamp: initialTimestamp,
		span:             span,
		onValue:          onValue,

		stopped: make(chan struct{}),
	}
	initConfig(&r.config, options)
	ctx = logtags.AddTag(ctx, "rangefeed", name)
	ctx, r.cancel = f.stopper.WithCancelOnQuiesce(ctx)
	if err := f.stopper.RunAsyncTask(ctx, "rangefeed", r.run); err != nil {
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

	initialTimestamp hlc.Timestamp

	span    roachpb.Span
	onValue func(ctx context.Context, value *roachpb.RangeFeedValue)

	closeOnce sync.Once
	cancel    context.CancelFunc
	stopped   chan struct{}
}

// Close closes the RangeFeed and waits for it to shut down.
func (f *RangeFeed) Close() {
	f.closeOnce.Do(func() {
		f.cancel()
		<-f.stopped
	})
}

// Run the rangefeed in a loop in the case of failure, likely due to node
// failures or general unavailability. We'll reset the retrier if the
// rangefeed runs for longer than the resetThreshold.
const resetThreshold = 30 * time.Second

// run will run the RangeFeed until the context is canceled. The context is
// hooked up to the stopper's quiescence.
func (f *RangeFeed) run(ctx context.Context) {
	defer close(f.stopped)
	r := retry.StartWithCtx(ctx, f.retryOptions)
	restartLogEvery := log.Every(10 * time.Second)
	if done := f.maybeRunInitialScan(ctx, &restartLogEvery, &r); done {
		return
	}

	// TODO(ajwerner): Consider adding event buffering. Doing so would require
	// draining when the rangefeed fails.
	eventCh := make(chan *roachpb.RangeFeedEvent)
	errCh := make(chan error)

	// Maintain a frontier in order to resume at a reasonable timestamp.
	// TODO(ajwerner): Consider exposing the frontier through a RangeFeed method.
	// Doing so would require some synchronization.
	frontier := span.MakeFrontier(f.span)
	frontier.Forward(f.span, f.initialTimestamp)
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

		// Note that the below channel send is safe because processEvents will
		// wait for the worker to send. RunWorker is safe here because this
		// goroutine is run as an AsyncTask.
		f.stopper.RunWorker(ctx, func(ctx context.Context) {
			errCh <- f.client.RangeFeed(ctx, f.span, ts, f.withDiff, eventCh)
		})

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
		if ranFor > resetThreshold {
			i = 1
			r.Reset()
		}
	}
}

// maybeRunInitialScan will attempt to perform an initial data scan if one was
// requested. It will retry in the face of errors and will only return upon
// context cancellation.
func (f *RangeFeed) maybeRunInitialScan(
	ctx context.Context, n *log.EveryN, r *retry.Retry,
) (done bool) {
	if !f.withInitialScan {
		return ctx.Err() != nil // done
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
		// retained.
		f.onValue(ctx, &v)
	}
	for {
		if !r.Next() {
			return true
		}
		if err := f.client.Scan(ctx, f.span, f.initialTimestamp, scan); err != nil {
			if n.ShouldLog() {
				log.Warningf(ctx, "failed to perform initial scan: %v", err)
			}
		} else /* err == nil */ {
			if f.onInitialScanDone != nil {
				f.onInitialScanDone(ctx)
			}
			return ctx.Err() != nil // done
		}
	}
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
				frontier.Forward(ev.Checkpoint.Span, ev.Checkpoint.ResolvedTS)
			case ev.Error != nil:
				// Intentionally do nothing, we'll get an error returned from the
				// call to RangeFeed.
			}
		case <-ctx.Done():
			// Ensure that the RangeFeed goroutine stops.
			<-errCh
			return nil
		case err := <-errCh:
			return err
		}
	}
}
