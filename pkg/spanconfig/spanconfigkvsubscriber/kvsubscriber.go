// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigkvsubscriber

import (
	"context"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigstore"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// KVSubscriber is used to subscribe to global span configuration changes. It's
// a concrete implementation of the spanconfig.KVSubscriber interface.
//
// Internally we maintain a rangefeed over the global store of span
// configurations (system.span_configurations), applying updates from it into an
// embedded spanconfig.Store. A read-only view of this data structure
// (spanconfig.StoreReader) is exposed as part of the KVSubscriber interface.
// Rangefeeds used as is don't offer any ordering guarantees with respect to
// updates made over non-overlapping keys, which is something we care about[1].
// For that reason we make use of a rangefeed buffer, accumulating raw rangefeed
// updates and flushing them out en-masse in timestamp order when the rangefeed
// frontier is bumped[2]. If the buffer overflows (as dictated by the memory
// limit the KVSubscriber is instantiated with), the subscriber is wound down and
// an appropriate error is returned to the caller.
//
// When running into the errors above, it's safe for the caller to re-subscribe
// to effectively re-establish the underlying rangefeeds. When re-establishing a
// new rangefeed and populating a spanconfig.Store using the contents of the
// initial scan[3], we wish to preserve the existing spanconfig.StoreReader.
// Discarding it would entail either blocking all external readers until a new
// spanconfig.StoreReader was fully populated, or presenting an inconsistent
// view of the spanconfig.Store that's currently being populated. For new
// rangefeeds what we do then is route all updates from the initial scan to a
// fresh spanconfig.Store, and once the initial scan is done, swap at the
// source for the exported spanconfig.StoreReader. During the initial scan,
// concurrent readers would continue to observe the last spanconfig.StoreReader
// if any. After the swap, it would observe the more up-to-date source instead.
// Future incremental updates will also target the new source. When this source
// swap occurs, we inform the handler of the need to possibly refresh its view
// of all configs.
//
// TODO(irfansharif): When swapping the old spanconfig.StoreReader for the new,
// instead of informing callers with an everything [min,max) span, we could diff
// the two data structures and only emit targeted updates.
//
// [1]: For a given key k, it's config may be stored as part of a larger span S
//      (where S.start <= k < S.end). It's possible for S to get deleted and
//      replaced with sub-spans S1...SN in the same transaction if the span is
//      getting split. When applying these updates, we need to make sure to
//      process the deletion event for S before processing S1...SN.
// [2]: In our example above deleting the config for S and adding configs for S1...Nwe
//      want to make sure that we apply the full set of updates all at once --
//      lest we expose the intermediate state where the config for S was deleted
//      but the configs for S1...SN were not yet applied.
// [3]: TODO(irfansharif): When tearing down the subscriber due to underlying errors,
//      we could also surface a checkpoint to use the next time the subscriber is
//      established. That way we can avoid the full initial scan over the span
//      configuration state and simply pick up where we left off with our existing
//      spanconfig.Store.
//
type KVSubscriber struct {
	stopper             *stop.Stopper
	db                  *kv.DB
	clock               *hlc.Clock
	rangefeedFactory    *rangefeed.Factory
	decoder             *spanConfigDecoder
	spanConfigTableSpan roachpb.Span // typically system.span_configurations, but overridable for tests
	bufferMemLimit      int64
	fallback            roachpb.SpanConfig
	knobs               *spanconfig.TestingKnobs

	subscribed int32    // accessed atomically
	mu         struct { // serializes between Subscribe and external threads
		syncutil.Mutex
		handler func(roachpb.Span)

		// external is the read-only view of the spanconfig.Store maintained by
		// the KVSubscriber. It's usually an opaque alias to an internal
		// spanconfig.Store, but when re-subscribing, a new spanconfig.Store is
		// populated while the existing StoreReader remains static. Once the new
		// spanconfig.Store is sufficiently caught up, the external view is
		// aliased to the new spanconfig.Store and the old
		// spanconfig.StoreReader is discarded. See the type-level comment for
		// more details.
		external spanconfig.StoreReader
	}

	lastFrontierTS hlc.Timestamp // used to assert monotonicity across subscription attempts
}

var _ spanconfig.KVSubscriber = &KVSubscriber{}

// spanConfigurationsTableRowSize is an estimate of the size of a single row in
// the system.span_configurations table (size of start/end key, and size of a
// marshaled span config proto). The value used here was pulled out of thin air
// -- it only serves to coarsely limit how large the KVSubscriber's underlying
// rangefeed buffer can get.
const spanConfigurationsTableRowSize = 5 << 10 // 5 KB

// New instantiates a KVSubscriber.
func New(
	stopper *stop.Stopper,
	db *kv.DB,
	clock *hlc.Clock,
	rangeFeedFactory *rangefeed.Factory,
	spanConfigurationsTableID uint32,
	bufferMemLimit int64,
	fallback roachpb.SpanConfig,
	knobs *spanconfig.TestingKnobs,
) *KVSubscriber {
	spanConfigTableStart := keys.SystemSQLCodec.IndexPrefix(
		spanConfigurationsTableID,
		keys.SpanConfigurationsTablePrimaryKeyIndexID,
	)
	spanConfigTableSpan := roachpb.Span{
		Key:    spanConfigTableStart,
		EndKey: spanConfigTableStart.PrefixEnd(),
	}
	spanConfigStore := spanconfigstore.New(fallback)
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	s := &KVSubscriber{
		stopper:             stopper,
		db:                  db,
		clock:               clock,
		bufferMemLimit:      bufferMemLimit,
		rangefeedFactory:    rangeFeedFactory,
		spanConfigTableSpan: spanConfigTableSpan,
		fallback:            fallback,
		knobs:               knobs,
		decoder:             newSpanConfigDecoder(),
	}
	s.mu.external = spanConfigStore
	return s
}

// Subscribe establishes a subscription (internally: rangefeed) over the global
// store of span configs. This is a blocking operation, returning only when the
// surrounding stopper is stopped or the context cancelled. The installed
// handler is invoked in this single main thread.
//
// It's possible for retryable errors to occur internally, at which point we
// tear down the existing subscription and re-establish another as dictated by
// the provided retry options. When unsubscribed, the exposed
// spanconfig.StoreReader continues to be readable (though no longer
// incrementally maintained -- the view gets progressively staler overtime). Any
// existing handler is kept intact and is notified when the subscription is
// re-established. After re-subscribing, the exported StoreReader will be
// up-to-date and continue to be incrementally maintained.
func (s *KVSubscriber) Subscribe(ctx context.Context, retryOpts retry.Options) error {
	ctx, cancel := s.stopper.WithCancelOnQuiesce(ctx)
	defer cancel()

	var err error
	for r := retry.Start(retryOpts); r.Next(); {
		if err = s.subscribeInner(ctx); err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				return ctx.Err() // we're done here
			}

			log.Warningf(ctx, "spanconfig-kvsubscriber failed with %v, retrying...", err)
			continue
		}

		return nil // we're done here
	}

	return err // return the last error (possibly unreachable code depending on retry options)
}

// subscribeInner establishes a rangefeed over the global store of span configs.
// This is a blocking operation, returning (and unsubscribing) only when the
// surrounding stopper is stopped, the context cancelled, or when a retryable
// error occurs.
func (s *KVSubscriber) subscribeInner(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&s.subscribed, 0, 1) {
		log.Fatal(ctx, "currently subscribed: only allowed once at any point in time")
	}
	if fn := s.knobs.KVSubscriberPreExitInterceptor; fn != nil {
		defer fn()
	}
	defer func() { atomic.StoreInt32(&s.subscribed, 0) }()

	buffer := rangefeedbuffer.New(int(s.bufferMemLimit / spanConfigurationsTableRowSize))
	frontierBumpedCh, initialScanDoneCh, errCh := make(chan struct{}), make(chan struct{}), make(chan error)
	mu := struct { // serializes access between the rangefeed and the main thread here
		syncutil.Mutex
		frontierTS hlc.Timestamp
	}{}

	defer func() {
		mu.Lock()
		s.lastFrontierTS = mu.frontierTS
		mu.Unlock()
	}()

	onValue := func(ctx context.Context, ev *roachpb.RangeFeedValue) {
		deleted := !ev.Value.IsPresent()
		var value roachpb.Value
		if deleted {
			if !ev.PrevValue.IsPresent() {
				// It's possible to write a KV tombstone on top of another KV
				// tombstone -- both the new and old value will be empty. We simply
				// ignore these events.
				return
			}

			// Since the end key is not part of the primary key, we need to
			// decode the previous value in order to determine what it is.
			value = ev.PrevValue
		} else {
			value = ev.Value
		}
		entry, err := s.decoder.decode(roachpb.KeyValue{
			Key:   ev.Key,
			Value: value,
		})
		if err != nil {
			log.Fatalf(ctx, "failed to decode row: %v", err) // non-retryable error; just fatal
		}

		if log.ExpensiveLogEnabled(ctx, 1) {
			log.Infof(ctx, "received span configuration update for %s (deleted=%t)", entry.Span, deleted)
		}

		update := spanconfig.Update{Span: entry.Span}
		if !deleted {
			update.Config = entry.Config
		}

		if err := buffer.Add(ctx, &bufferEvent{update, ev.Value.Timestamp}); err != nil {
			select {
			case <-ctx.Done():
				// The context is cancelled when the rangefeed is closed by the
				// main handler goroutine. It's closed after we stop listening
				// to errCh.
			case errCh <- err:
			}
		}
	}

	initialScanTS := s.clock.Now()
	if initialScanTS.Less(s.lastFrontierTS) {
		log.Fatalf(ctx, "initial scan timestamp (%s) regressed from last recorded frontier (%s)", initialScanTS, s.lastFrontierTS)
	}

	rangefeed := s.rangefeedFactory.New("spanconfig-rangefeed", s.spanConfigTableSpan, initialScanTS,
		onValue,
		rangefeed.WithInitialScan(func(ctx context.Context) {
			select {
			case <-ctx.Done():
				// The context is cancelled when the rangefeed is closed by the
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
		rangefeed.WithDiff(),
		rangefeed.WithOnInitialScanError(func(ctx context.Context, err error) (shouldFail bool) {
			// TODO(irfansharif): Consider if there are other errors which we
			// want to treat as permanent. This was cargo culted from the
			// settings watcher.
			if grpcutil.IsAuthError(err) ||
				strings.Contains(err.Error(), "rpc error: code = Unauthenticated") {
				return true
			}
			return false
		}),
	)
	if err := rangefeed.Start(ctx); err != nil {
		return err
	}
	defer rangefeed.Close()
	if fn := s.knobs.KVSubscriberPostRangefeedStartInterceptor; fn != nil {
		fn()
	}

	log.Info(ctx, "established range feed over span configurations table")

	injectedErrCh := s.knobs.KVSubscriberErrorInjectionCh
	internal := spanconfigstore.New(s.fallback)

	var handlerInitialized bool
	for {
		select {
		case <-s.stopper.ShouldQuiesce():
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-frontierBumpedCh:
			mu.Lock()
			frontierTS := mu.frontierTS
			mu.Unlock()

			events := buffer.Flush(ctx, frontierTS)
			for _, ev := range events {
				internal.Apply(ctx, ev.(*bufferEvent).Update, false /* dryrun */)
			}

			s.mu.Lock()
			handler := s.mu.handler
			s.mu.Unlock()

			if handler != nil {
				if !handlerInitialized {
					handler(keys.EverythingSpan)
					handlerInitialized = true
				} else {
					for _, ev := range events {
						handler(ev.(*bufferEvent).Update.Span)
					}
				}
			}

			if fn := s.knobs.KVSubscriberOnTimestampAdvanceInterceptor; fn != nil {
				fn(frontierTS)
			}
		case <-initialScanDoneCh:
			events := buffer.Flush(ctx, initialScanTS)
			for _, ev := range events {
				internal.Apply(ctx, ev.(*bufferEvent).Update, false /* dryrun */)
			}

			s.mu.Lock()
			s.mu.external = internal
			handler := s.mu.handler
			s.mu.Unlock()

			if handler != nil {
				// When re-establishing a rangefeed, it's possible we have a
				// spanconfig.Store with arbitrary updates from what was
				// exported last. Let's inform the handler than everything needs
				// to be checked again.
				handler(keys.EverythingSpan)

				// Given we're notifying the handler using the everything span,
				// we might as well mark it as initialized.
				handlerInitialized = true
			}

			if fn := s.knobs.KVSubscriberOnTimestampAdvanceInterceptor; fn != nil {
				fn(initialScanTS)
			}
		case err := <-errCh:
			return err
		case err := <-injectedErrCh:
			return err
		}
	}
}

// OnSpanConfigUpdate installs a callback that's invoked with whatever span may
// have seen a config update.
func (s *KVSubscriber) OnSpanConfigUpdate(ctx context.Context, handler func(roachpb.Span)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mu.handler != nil {
		log.Fatalf(ctx, "found existing handler: only one allowed")
	}
	s.mu.handler = handler
}

// NeedsSplit is part of the spanconfig.KVSubscriber interface.
func (s *KVSubscriber) NeedsSplit(ctx context.Context, start, end roachpb.RKey) bool {
	s.mu.Lock()
	reader := s.mu.external
	s.mu.Unlock()

	return reader.NeedsSplit(ctx, start, end)
}

// ComputeSplitKey is part of the spanconfig.KVSubscriber interface.
func (s *KVSubscriber) ComputeSplitKey(ctx context.Context, start, end roachpb.RKey) roachpb.RKey {
	s.mu.Lock()
	reader := s.mu.external
	s.mu.Unlock()

	return reader.ComputeSplitKey(ctx, start, end)
}

// GetSpanConfigForKey is part of the spanconfig.KVSubscriber interface.
func (s *KVSubscriber) GetSpanConfigForKey(
	ctx context.Context, key roachpb.RKey,
) (roachpb.SpanConfig, error) {
	s.mu.Lock()
	reader := s.mu.external
	s.mu.Unlock()

	return reader.GetSpanConfigForKey(ctx, key)
}

type bufferEvent struct {
	spanconfig.Update
	ts hlc.Timestamp
}

func (w *bufferEvent) Timestamp() hlc.Timestamp {
	return w.ts
}

var _ rangefeedbuffer.Event = &bufferEvent{}
