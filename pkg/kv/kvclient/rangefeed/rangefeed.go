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
	"fmt"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

//go:generate mockgen -destination=mocks_generated_test.go --package=rangefeed . DB

// TODO(ajwerner): Expose hooks for metrics.
// TODO(ajwerner): Expose access to checkpoints and the frontier.
// TODO(ajwerner): Expose better control over how the exponential backoff gets
// reset when the feed has been running successfully for a while.
// TODO(yevgeniy): Instead of rolling our own logic to parallelize scans, we should
// use streamer API instead (https://github.com/cockroachdb/cockroach/pull/68430)

// DB is an adapter to the underlying KV store.
type DB interface {

	// RangeFeed runs a rangefeed on a given span with the given arguments.
	// It encapsulates the RangeFeed method on roachpb.Internal.
	RangeFeed(
		ctx context.Context,
		spans []roachpb.Span,
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
		spans []roachpb.Span,
		asOf hlc.Timestamp,
		rowFn func(value roachpb.KeyValue),
		cfg scanConfig,
	) error
}

// Factory is used to construct RangeFeeds.
type Factory struct {
	stopper *stop.Stopper
	client  DB
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
func NewFactory(
	stopper *stop.Stopper, db *kv.DB, st *cluster.Settings, knobs *TestingKnobs,
) (*Factory, error) {
	kvDB, err := newDBAdapter(db, st)
	if err != nil {
		return nil, err
	}
	return newFactory(stopper, kvDB, knobs), nil
}

func newFactory(stopper *stop.Stopper, client DB, knobs *TestingKnobs) *Factory {
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
	spans []roachpb.Span,
	initialTimestamp hlc.Timestamp,
	onValue OnValue,
	options ...Option,
) (_ *RangeFeed, err error) {
	r := f.New(name, initialTimestamp, onValue, options...)
	if err := r.Start(ctx, spans); err != nil {
		return nil, err
	}
	return r, nil
}

// New constructs a new RangeFeed (without running it).
func (f *Factory) New(
	name string, initialTimestamp hlc.Timestamp, onValue OnValue, options ...Option,
) *RangeFeed {
	r := RangeFeed{
		client:  f.client,
		stopper: f.stopper,
		knobs:   f.knobs,

		initialTimestamp: initialTimestamp,
		name:             name,
		onValue:          onValue,

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
	client  DB
	stopper *stop.Stopper
	knobs   *TestingKnobs

	initialTimestamp hlc.Timestamp
	spans            []roachpb.Span
	spansDebugStr    string // Debug string describing spans

	onValue OnValue

	closeOnce sync.Once
	cancel    context.CancelFunc
	stopped   chan struct{}

	started int32 // accessed atomically
}

// Start kicks off the rangefeed in an async task, it can only be invoked once.
// All the installed callbacks (OnValue, OnCheckpoint, OnFrontierAdvance,
// OnInitialScanDone) are called in said async task in a single thread.
func (f *RangeFeed) Start(ctx context.Context, spans []roachpb.Span) error {
	if len(spans) == 0 {
		return errors.AssertionFailedf("expected at least 1 span, got none")
	}

	if !atomic.CompareAndSwapInt32(&f.started, 0, 1) {
		return errors.AssertionFailedf("rangefeed already started")
	}

	// Maintain a frontier in order to resume at a reasonable timestamp.
	// TODO(ajwerner): Consider exposing the frontier through a RangeFeed method.
	// Doing so would require some synchronization.
	frontier, err := span.MakeFrontier(spans...)
	if err != nil {
		return err
	}

	for _, sp := range spans {
		if _, err := frontier.Forward(sp, f.initialTimestamp); err != nil {
			return err
		}
	}

	// Frontier merges and de-dups passed in spans.  So, use frontier to initialize
	// sorted list of spans.
	frontier.Entries(func(sp roachpb.Span, _ hlc.Timestamp) (done span.OpResult) {
		f.spans = append(f.spans, sp)
		return span.ContinueMatch
	})

	runWithFrontier := func(ctx context.Context) {
		// pprof.Do function does exactly what we do here, but it also results in
		// pprof.Do function showing up in the stack traces -- so, just set and reset
		// labels manually.
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, pprof.Labels(append(f.extraPProfLabels, "rangefeed", f.name)...))
		pprof.SetGoroutineLabels(ctx)
		f.run(ctx, frontier)
	}

	f.spansDebugStr = func() string {
		n := len(spans)
		if n == 1 {
			return spans[0].String()
		}

		return fmt.Sprintf("{%s}", frontier.String())
	}()

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

	if f.withInitialScan {
		if done := f.runInitialScan(ctx, &restartLogEvery, &r); done {
			return
		}
	}

	// Check the context before kicking off a rangefeed.
	if ctx.Err() != nil {
		return
	}

	// TODO(ajwerner): Consider adding event buffering. Doing so would require
	// draining when the rangefeed fails.
	eventCh := make(chan *roachpb.RangeFeedEvent)

	for i := 0; r.Next(); i++ {
		ts := frontier.Frontier()
		if log.ExpensiveLogEnabled(ctx, 1) {
			log.Eventf(ctx, "starting rangefeed from %v on %v", ts, f.spansDebugStr)
		}

		start := timeutil.Now()

		rangeFeedTask := func(ctx context.Context) error {
			return f.client.RangeFeed(ctx, f.spans, ts, f.withDiff, eventCh)
		}
		processEventsTask := func(ctx context.Context) error {
			return f.processEvents(ctx, frontier, eventCh)
		}

		err := ctxgroup.GoAndWait(ctx, rangeFeedTask, processEventsTask)
		if errors.HasType(err, &roachpb.BatchTimestampBeforeGCError{}) ||
			errors.HasType(err, &roachpb.MVCCHistoryMutationError{}) {
			if errCallback := f.onUnrecoverableError; errCallback != nil {
				errCallback(ctx, err)
			}

			log.VEventf(ctx, 1, "exiting rangefeed due to internal error: %v", err)
			return
		}
		if err != nil && ctx.Err() == nil && restartLogEvery.ShouldLog() {
			log.Warningf(ctx, "rangefeed failed %d times, restarting: %v",
				redact.Safe(i), err)
		}
		if ctx.Err() != nil {
			log.VEventf(ctx, 1, "exiting rangefeed")
			return
		}

		ranFor := timeutil.Since(start)
		log.VEventf(ctx, 1, "restarting rangefeed for %v after %v",
			f.spansDebugStr, ranFor)
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

// processEvents processes events sent by the rangefeed on the eventCh.
func (f *RangeFeed) processEvents(
	ctx context.Context, frontier *span.Frontier, eventCh <-chan *roachpb.RangeFeedEvent,
) error {
	for {
		select {
		case ev := <-eventCh:
			switch {
			case ev.Val != nil:
				f.onValue(ctx, ev.Val)
			case ev.Checkpoint != nil:
				advanced, err := frontier.Forward(ev.Checkpoint.Span, ev.Checkpoint.ResolvedTS)
				if err != nil {
					return err
				}
				if f.onCheckpoint != nil {
					f.onCheckpoint(ctx, ev.Checkpoint)
				}
				if advanced && f.onFrontierAdvance != nil {
					f.onFrontierAdvance(ctx, frontier.Frontier())
				}
			case ev.SST != nil:
				if f.onSSTable == nil {
					return errors.AssertionFailedf(
						"received unexpected rangefeed SST event with no OnSSTable handler")
				}
				f.onSSTable(ctx, ev.SST)
			case ev.Error != nil:
				// Intentionally do nothing, we'll get an error returned from the
				// call to RangeFeed.
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
