// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"
	"fmt"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
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
		eventC chan<- kvcoord.RangeFeedMessage,
		opts ...kvcoord.RangeFeedOption,
	) error

	// RangefeedFromFrontier runs a rangefeed on the frontier's spans, and
	// starting at each span's associated timestamp.
	RangeFeedFromFrontier(
		ctx context.Context,
		frontier span.Frontier,
		eventC chan<- kvcoord.RangeFeedMessage,
		opts ...kvcoord.RangeFeedOption,
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
		rowsFn func([]kv.KeyValue),
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

	// IgnoreOnDeleteRangeError will ignore any errors where a DeleteRange event
	// is emitted without an OnDeleteRange handler. This can be used e.g. with
	// StoreTestingKnobs.GlobalMVCCRangeTombstone, to prevent the global tombstone
	// causing rangefeed errors for consumers who don't expect it.
	IgnoreOnDeleteRangeError bool
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}

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
// server shuts down. The only error which can be returned will indicate that
// the server is being shut down.
//
// Rangefeeds do not support inline (unversioned) values, and may omit them or
// error on them. Similarly, rangefeeds will error if MVCC history is mutated
// via e.g. ClearRange. Do not use rangefeeds across such key spans.
//
// NB: for the rangefeed itself, initialTimestamp is exclusive, i.e. the first
// possible event emitted by the server (including the catchup scan) is at
// initialTimestamp.Next(). This follows from the gRPC API semantics. However,
// the initial scan (if any) is run at initialTimestamp.
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
	}
	initConfig(&r.config, options)
	return &r
}

// OnValue is called for each rangefeed value.
type OnValue func(ctx context.Context, value *kvpb.RangeFeedValue)

// OnValue is called for a batch of rangefeed values.
type OnValues func(ctx context.Context, values []kv.KeyValue)

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

	cancel  context.CancelFunc
	running sync.WaitGroup
	started int32 // accessed atomically
}

// Start kicks off the rangefeed in an async task, it can only be invoked once.
// All the installed callbacks (OnValue, OnCheckpoint, OnFrontierAdvance,
// OnInitialScanDone) are called in said async task in a single thread.
func (f *RangeFeed) Start(ctx context.Context, spans []roachpb.Span) error {
	if len(spans) == 0 {
		return errors.AssertionFailedf("expected at least 1 span, got none")
	}

	// Maintain a frontier in order to resume at a reasonable timestamp.
	// TODO(ajwerner): Consider exposing the frontier through a RangeFeed method.
	// Doing so would require some synchronization.
	frontier, err := span.MakeFrontier(spans...)
	if err != nil {
		return err
	}
	return f.start(ctx, frontier, true, false)
}

// StartFromFrontier is like Start but allows passing a frontier containing the
// spans on which to create the feed, which can reflect any previous progress,
// unlike passing just the spans to Start().
//
// The rangefeed takes ownership of the passed frontier until it is closed; the
// caller must not interact with it until Close returns. The caller remains
// responsible for releasing the frontier thereafter however.
func (f *RangeFeed) StartFromFrontier(ctx context.Context, frontier span.Frontier) error {
	return f.start(ctx, frontier, false, true)
}

func (f *RangeFeed) start(
	ctx context.Context, frontier span.Frontier, ownsFrontier bool, resumeFromFrontier bool,
) error {
	if !atomic.CompareAndSwapInt32(&f.started, 0, 1) {
		return errors.AssertionFailedf("rangefeed already started")
	}

	// Frontier merges and de-dups passed in spans.  So, use frontier to initialize
	// sorted list of spans.
	frontier.Entries(func(sp roachpb.Span, _ hlc.Timestamp) (done span.OpResult) {
		f.spans = append(f.spans, sp)
		return span.ContinueMatch
	})

	runWithFrontier := func(ctx context.Context) {
		if ownsFrontier {
			defer frontier.Release()
		}
		// pprof.Do function does exactly what we do here, but it also results in
		// pprof.Do function showing up in the stack traces -- so, just set and reset
		// labels manually.
		defer pprof.SetGoroutineLabels(ctx)
		ctx = pprof.WithLabels(ctx, pprof.Labels(append(f.extraPProfLabels, "rangefeed", f.name)...))
		pprof.SetGoroutineLabels(ctx)
		if f.invoker != nil {
			_ = f.invoker(func() error {
				f.run(ctx, frontier, resumeFromFrontier)
				return nil
			})
			return
		}
		f.run(ctx, frontier, resumeFromFrontier)
	}

	if l := frontier.Len(); l == 1 {
		f.spansDebugStr = frontier.PeekFrontierSpan().String()
	} else {
		var buf strings.Builder
		frontier.Entries(func(sp roachpb.Span, _ hlc.Timestamp) span.OpResult {
			if buf.Len() > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(sp.String())
			if buf.Len() >= 400 {
				fmt.Fprintf(&buf, "… [%d spans]", l)
				return span.StopMatch
			}
			return span.ContinueMatch
		})
		f.spansDebugStr = buf.String()
	}

	ctx = logtags.AddTag(ctx, "rangefeed", f.name)
	ctx, f.cancel = f.stopper.WithCancelOnQuiesce(ctx)
	f.running.Add(1)
	if err := f.stopper.RunAsyncTask(ctx, "rangefeed", runWithFrontier); err != nil {
		f.cancel()
		f.running.Done()
		return err
	}
	return nil
}

// Close closes the RangeFeed and waits for it to shut down; it does so
// idempotently. It waits for the currently running handler, if any, to complete
// and guarantees that no future handlers will be invoked after this point.
func (f *RangeFeed) Close() {
	f.cancel()
	f.running.Wait()
}

// Run the rangefeed in a loop in the case of failure, likely due to node
// failures or general unavailability. If the rangefeed runs successfully for at
// least this long, then after subsequent failures we would like to reset the
// exponential backoff to experience long delays between retry attempts.
// This is the threshold of successful running after which the backoff state
// will be reset.
const resetThreshold = 30 * time.Second

// run will run the RangeFeed until the context is canceled or if the client
// indicates that an initial scan error is non-recoverable. The
// resumeWithFrontier arg enables the client to resume the rangefeed using the
// span frontier instead of from the frontier's low water mark.
func (f *RangeFeed) run(ctx context.Context, frontier span.Frontier, resumeWithFrontier bool) {
	defer f.running.Done()
	r := retry.StartWithCtx(ctx, f.retryOptions)
	restartLogEvery := log.Every(10 * time.Second)

	if f.withInitialScan {
		if failed := f.runInitialScan(ctx, &restartLogEvery, &r, frontier); failed {
			return
		}
	} else if !resumeWithFrontier {
		for _, sp := range f.spans {
			if _, err := frontier.Forward(sp, f.initialTimestamp); err != nil {
				if fn := f.onUnrecoverableError; fn != nil {
					fn(ctx, err)
				}
				return
			}
		}
	}

	// Check the context before kicking off a rangefeed.
	if ctx.Err() != nil {
		return
	}

	// TODO(ajwerner): Consider adding event buffering. Doing so would require
	// draining when the rangefeed fails.
	eventCh := make(chan kvcoord.RangeFeedMessage)

	var rangefeedOpts []kvcoord.RangeFeedOption
	if f.scanConfig.overSystemTable {
		rangefeedOpts = append(rangefeedOpts, kvcoord.WithSystemTablePriority())
	}
	if f.withDiff {
		rangefeedOpts = append(rangefeedOpts, kvcoord.WithDiff())
	}
	if f.withFiltering {
		rangefeedOpts = append(rangefeedOpts, kvcoord.WithFiltering())
	}
	if len(f.withMatchingOriginIDs) != 0 {
		rangefeedOpts = append(rangefeedOpts, kvcoord.WithMatchingOriginIDs(f.withMatchingOriginIDs...))
	}
	if f.onMetadata != nil {
		rangefeedOpts = append(rangefeedOpts, kvcoord.WithMetadata())
	}
	rangefeedOpts = append(rangefeedOpts, kvcoord.WithConsumerID(f.consumerID))

	for i := 0; r.Next(); i++ {
		ts := frontier.Frontier()
		if log.ExpensiveLogEnabled(ctx, 1) {
			log.Eventf(ctx, "starting rangefeed from %v on %v", ts, f.spansDebugStr)
		}

		start := timeutil.Now()

		rangeFeedTask := func(ctx context.Context) error {
			if f.invoker == nil {
				return f.client.RangeFeed(ctx, f.spans, ts, eventCh, rangefeedOpts...)
			}
			return f.invoker(func() error {
				return f.client.RangeFeed(ctx, f.spans, ts, eventCh, rangefeedOpts...)
			})
		}

		if resumeWithFrontier {
			rangeFeedTask = func(ctx context.Context) error {
				if f.invoker == nil {
					return f.client.RangeFeedFromFrontier(ctx, frontier, eventCh, rangefeedOpts...)
				}
				return f.invoker(func() error {
					return f.client.RangeFeedFromFrontier(ctx, frontier, eventCh, rangefeedOpts...)
				})
			}
		}
		processEventsTask := func(ctx context.Context) error {
			if f.invoker != nil {
				return f.invoker(func() error {
					return f.processEvents(ctx, frontier, eventCh)
				})
			}
			return f.processEvents(ctx, frontier, eventCh)
		}

		err := ctxgroup.GoAndWait(ctx, rangeFeedTask, processEventsTask)
		if errors.HasType(err, &kvpb.BatchTimestampBeforeGCError{}) ||
			errors.HasType(err, &kvpb.MVCCHistoryMutationError{}) {
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
	ctx context.Context, frontier span.Frontier, eventCh <-chan kvcoord.RangeFeedMessage,
) error {
	for {
		select {
		case ev := <-eventCh:
			switch {
			case ev.Val != nil:
				f.onValue(ctx, ev.Val)
			case ev.Checkpoint != nil:
				ts := ev.Checkpoint.ResolvedTS
				if f.frontierQuantize != 0 {
					ts.Logical = 0
					ts.WallTime -= ts.WallTime % int64(f.frontierQuantize)
				}
				advanced, err := frontier.Forward(ev.Checkpoint.Span, ts)
				if err != nil {
					return err
				}
				if f.onCheckpoint != nil {
					f.onCheckpoint(ctx, ev.Checkpoint)
				}
				if advanced && f.onFrontierAdvance != nil {
					f.onFrontierAdvance(ctx, frontier.Frontier())
				}
				if f.frontierVisitor != nil {
					f.frontierVisitor(ctx, advanced, frontier)
				}
			case ev.SST != nil:
				if f.onSSTable == nil {
					return errors.AssertionFailedf(
						"received unexpected rangefeed SST event with no OnSSTable handler")
				}
				f.onSSTable(ctx, ev.SST, ev.RegisteredSpan)
			case ev.DeleteRange != nil:
				if f.onDeleteRange == nil {
					if f.knobs != nil && f.knobs.IgnoreOnDeleteRangeError {
						continue
					}
					return errors.AssertionFailedf(
						"received unexpected rangefeed DeleteRange event with no OnDeleteRange handler: %s", ev)
				}
				f.onDeleteRange(ctx, ev.DeleteRange)
			case ev.Metadata != nil:
				if f.onMetadata == nil {
					return errors.AssertionFailedf("received unexpected metadata event with no OnMetadata handler")
				}
				f.onMetadata(ctx, ev.Metadata)
			case ev.Error != nil:
				// Intentionally do nothing, we'll get an error returned from the
				// call to RangeFeed.
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
