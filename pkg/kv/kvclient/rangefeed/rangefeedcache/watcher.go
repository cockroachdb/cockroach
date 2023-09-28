// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeedcache

import (
	"context"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// Watcher is used to implement a consistent cache over spans of KV data
// on top of a RangeFeed. Note that while rangefeeds offer events as they
// happen at low latency, a consistent snapshot cannot be constructed until
// resolved timestamp checkpoints have been received for the relevant spans.
// The watcher internally buffers events until the complete span has been
// resolved to some timestamp, at which point the events up to that timestamp
// are published as an update. This will take on the order of the
// kv.closed_timestamp.target_duration cluster setting (default 3s).
//
// If the buffer overflows (as dictated by the buffer limit the Watcher is
// instantiated with), the old rangefeed is wound down and a new one
// re-established. The client interacts with data from the RangeFeed in two
// ways, firstly, by translating raw KVs into kvbuffer.Events, and by handling
// a batch of such events when either the initial scan completes or the
// frontier changes. The OnUpdateCallback which is handed a batch of events,
// called an Update, is informed whether the batch of events corresponds to a
// complete or incremental update.
//
// It's expected to be Start-ed once. Start internally invokes Run in a retry
// loop.
//
// The expectation is that the caller will use a mutex to update an underlying
// data structure.
//
// NOTE (for emphasis): Update events after the initial scan are published at a
// delay corresponding to kv.closed_timestamp.target_duration (default 3s) with
// an interval of kv.rangefeed.closed_timestamp_refresh_interval (default 3s).
// Users seeking to leverage the Updates which arrive with that delay but also
// react to the row-level events as they arrive can hijack the translateEvent
// function to trigger some non-blocking action.
type Watcher struct {
	name                   redact.SafeString
	clock                  *hlc.Clock
	rangefeedFactory       *rangefeed.Factory
	spans                  []roachpb.Span
	bufferSize             int
	withPrevValue          bool
	withRowTSInInitialScan bool

	started int32 // accessed atomically

	translateEvent TranslateEventFunc
	onUpdate       OnUpdateFunc

	lastFrontierTS hlc.Timestamp // used to assert monotonicity across rangefeed attempts

	// Used to force a restart during testing.
	restartErrCh chan error

	knobs TestingKnobs
}

// UpdateType is passed to an OnUpdateFunc to indicate whether the set of
// events represents a complete update or an incremental update.
type UpdateType bool

const (
	// CompleteUpdate indicates that the events represent the entirety of data
	// in the spans.
	CompleteUpdate UpdateType = true

	// IncrementalUpdate indicates that the events represent an incremental update
	// since the previous update.
	IncrementalUpdate UpdateType = false
)

func (u UpdateType) String() string {
	if u == CompleteUpdate {
		return "Complete Update"
	}
	return "Incremental Update"
}

// TranslateEventFunc is used by the client to translate a low-level event
// into an event for buffering. If nil is returned, the event is skipped.
type TranslateEventFunc func(
	context.Context, *kvpb.RangeFeedValue,
) rangefeedbuffer.Event

// OnUpdateFunc is used by the client to receive an Update, which is a batch
// of events which represent either a CompleteUpdate or an IncrementalUpdate.
type OnUpdateFunc func(context.Context, Update)

// Update corresponds to a set of events derived from the underlying RangeFeed.
type Update struct {
	Type      UpdateType
	Timestamp hlc.Timestamp
	Events    []rangefeedbuffer.Event
}

// TestingKnobs allows tests to inject behavior into the Watcher.
type TestingKnobs struct {
	// PostRangeFeedStart is invoked after the rangefeed is started.
	PostRangeFeedStart func()

	// PreExit is invoked right before returning from Run, after tearing
	// down internal components.
	PreExit func()

	// OnTimestampAdvance is called after the update to the data have been
	// handled for the given timestamp.
	OnTimestampAdvance func(timestamp hlc.Timestamp)

	// ErrorInjectionCh is a way for tests to conveniently inject buffer
	// overflow errors in order to test recovery.
	ErrorInjectionCh chan error
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (k *TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)

// NewWatcher instantiates a Watcher. The two key drivers of the watcher are
// translateEvent and onUpdate.
//
// The translateEvent function is called on each RangeFeedValue event as they
// arrive (including the events synthesized from the initial scan). Callers may
// utilize translateEvent to trigger actions without waiting for the delay due
// to resolving the complete set of spans. If nil is returned, no event will
// be buffered.
//
// Once a resolved timestamp for the complete set of spans has been processed,
// an Update is passed to onUpdate. Note that the update may contain no
// buffered events.
//
// Callers can control whether the values passed to translateEvent carry a
// populated PrevValue using the withPrevValue parameter. See
// rangefeed.WithDiff for more details.
func NewWatcher(
	name redact.SafeString,
	clock *hlc.Clock,
	rangeFeedFactory *rangefeed.Factory,
	bufferSize int,
	spans []roachpb.Span,
	withPrevValue bool,
	withRowTSInInitialScan bool,
	translateEvent TranslateEventFunc,
	onUpdate OnUpdateFunc,
	knobs *TestingKnobs,
) *Watcher {
	w := &Watcher{
		name:                   name,
		clock:                  clock,
		rangefeedFactory:       rangeFeedFactory,
		spans:                  spans,
		bufferSize:             bufferSize,
		withPrevValue:          withPrevValue,
		withRowTSInInitialScan: withRowTSInInitialScan,
		translateEvent:         translateEvent,
		onUpdate:               onUpdate,
		restartErrCh:           make(chan error),
	}
	if knobs != nil {
		w.knobs = *knobs
	}
	return w
}

// Start calls Run on the Watcher as an async task with exponential backoff.
// If the Watcher ran for what is deemed long enough, the backoff is reset.
func Start(ctx context.Context, stopper *stop.Stopper, c *Watcher, onError func(error)) error {
	return stopper.RunAsyncTask(ctx, string(c.name), func(ctx context.Context) {
		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		const aWhile = 5 * time.Minute // arbitrary but much longer than a retry
		for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
			started := timeutil.Now()
			if err := c.Run(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return // we're done here
				}
				if onError != nil {
					onError(err)
				}

				if timeutil.Since(started) > aWhile {
					r.Reset()
				}

				log.Warningf(ctx, "%s: failed with %v, retrying...", c.name, err)
				continue
			}

			return // we're done here (the stopper was stopped, Run exited cleanly)
		}
	})
}

// Run establishes a rangefeed over the configured spans, first performing an
// initial scan, which will be presented as a CompleteUpdate, and then
// accumulating events until a relevant checkpoint arrives, at which point
// an IncrementalUpdate will be published.
//
// This is a blocking operation, returning only when the context is canceled,
// or when an error occurs. For the latter, it's expected that callers will
// re-run the watcher.
func (s *Watcher) Run(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&s.started, 0, 1) {
		log.Fatal(ctx, "currently started: only allowed once at any point in time")
	}
	if fn := s.knobs.PreExit; fn != nil {
		defer fn()
	}
	defer func() { atomic.StoreInt32(&s.started, 0) }()

	// Initially construct an "unbounded" buffer. We want our initial scan to
	// succeed, regardless of size. If we exceed a hard-coded limit during the
	// initial scan, that's not recoverable/retryable -- a subsequent retry would
	// run into the same limit. Instead, we'll forego limiting for now but
	// set it below, when handling incremental updates.
	//
	// TODO(irfansharif): If this unbounded initial scan buffer proves worrying,
	// we could re-work these interfaces to have callers use the rangefeedcache to
	// keep a subset of the total table in-memory, fed by the rangefeed, and
	// transparently query the backing table if the record requested is not found.
	// We could also have the initial scan operate in chunks, handing off results
	// to the caller incrementally, all within the "initial scan" phase.
	buffer := rangefeedbuffer.New(math.MaxInt)
	frontierBumpedCh, initialScanDoneCh, errCh := make(chan struct{}), make(chan struct{}), make(chan error)
	mu := struct { // serializes access between the rangefeed and the main thread here
		syncutil.Mutex
		frontierTS hlc.Timestamp
	}{}

	defer func() {
		mu.Lock()
		s.lastFrontierTS.Forward(mu.frontierTS)
		mu.Unlock()
	}()

	onValue := func(ctx context.Context, ev *kvpb.RangeFeedValue) {
		bEv := s.translateEvent(ctx, ev)
		if bEv == nil {
			return
		}

		if err := buffer.Add(bEv); err != nil {
			select {
			case <-ctx.Done():
				// The context is canceled when the rangefeed is closed by the
				// main handler goroutine. It's closed after we stop listening
				// to errCh.
			case errCh <- err:
			}
		}
	}

	initialScanTS := s.clock.Now()
	if initialScanTS.Less(s.lastFrontierTS) {
		log.Fatalf(ctx, "%s: initial scan timestamp (%s) regressed from last recorded frontier (%s)", s.name, initialScanTS, s.lastFrontierTS)
	}

	// Tests can use a nil rangefeedFactory.
	if s.rangefeedFactory != nil {
		rangeFeed := s.rangefeedFactory.New(string(s.name), initialScanTS,
			onValue,
			rangefeed.WithInitialScan(func(ctx context.Context) {
				select {
				case <-ctx.Done():
					// The context is canceled when the rangefeed is closed by the
					// main handler goroutine. It's closed after we stop listening
					// to initialScanDoneCh.
				case initialScanDoneCh <- struct{}{}:
				}
			}),
			rangefeed.WithOnFrontierAdvance(func(ctx context.Context, frontierTS hlc.Timestamp) {
				mu.Lock()
				mu.frontierTS = frontierTS
				mu.Unlock()

				select {
				case <-ctx.Done():
				case frontierBumpedCh <- struct{}{}:
				}
			}),
			// TODO(irfansharif): Consider making this configurable on the Watcher
			// type. As of 2022-11 all uses of this type are system-internal ones
			// where a higher admission-pri makes sense.
			rangefeed.WithSystemTablePriority(),
			rangefeed.WithDiff(s.withPrevValue),
			rangefeed.WithRowTimestampInInitialScan(s.withRowTSInInitialScan),
			rangefeed.WithOnInitialScanError(func(ctx context.Context, err error) (shouldFail bool) {
				// TODO(irfansharif): Consider if there are other errors which we
				// want to treat as permanent. This was cargo culted from the
				// settings watcher.
				if grpcutil.IsAuthError(err) ||
					strings.Contains(err.Error(), "rpc error: code = Unauthenticated") {
					select {
					case <-ctx.Done():
						// The context is canceled when the rangefeed is closed by the
						// main handler goroutine. It's closed after we stop listening
						// to errCh.
					case errCh <- err:
					}
					return true
				}
				return false
			}),
		)
		if err := rangeFeed.Start(ctx, s.spans); err != nil {
			return err
		}
		defer rangeFeed.Close()
	}
	if fn := s.knobs.PostRangeFeedStart; fn != nil {
		fn()
	}

	log.Infof(ctx, "%s: established range feed cache", s.name)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-frontierBumpedCh:
			mu.Lock()
			frontierTS := mu.frontierTS
			mu.Unlock()
			s.handleUpdate(ctx, buffer, frontierTS, IncrementalUpdate)

		case <-initialScanDoneCh:
			s.handleUpdate(ctx, buffer, initialScanTS, CompleteUpdate)
			// We're done with our initial scan, set a hard limit for incremental
			// updates going forward.
			buffer.SetLimit(s.bufferSize)

		case err := <-errCh:
			return err
		case err := <-s.restartErrCh:
			return err
		case err := <-s.knobs.ErrorInjectionCh:
			return err
		}
	}
}

var restartErr = errors.New("testing restart requested")

// TestingRestart injects an error into the rangefeed cache, forcing
// it to restart. This is separate from the testing knob so that we
// can force a restart from test infrastructure without overriding the
// user-provided testing knobs.
func (s *Watcher) TestingRestart() {
	s.restartErrCh <- restartErr
}

func (s *Watcher) handleUpdate(
	ctx context.Context, buffer *rangefeedbuffer.Buffer, ts hlc.Timestamp, updateType UpdateType,
) {
	s.onUpdate(ctx, Update{
		Type:      updateType,
		Timestamp: ts,
		Events:    buffer.Flush(ctx, ts),
	})
	if fn := s.knobs.OnTimestampAdvance; fn != nil {
		fn(ts)
	}
}
