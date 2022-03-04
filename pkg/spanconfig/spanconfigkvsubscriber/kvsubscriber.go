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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigstore"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// KVSubscriber is used to subscribe to global span configuration changes. It's
// a concrete implementation of the spanconfig.KVSubscriber interface.
//
// It's expected to Start-ed once, after which one or many subscribers can
// listen in for updates. Internally we maintain a rangefeed over the global
// store of span configurations (system.span_configurations), applying updates
// from it into an internal spanconfig.Store. A read-only view of this data
// structure (spanconfig.StoreReader) is exposed as part of the KVSubscriber
// interface. Rangefeeds used as is don't offer any ordering guarantees with
// respect to updates made over non-overlapping keys, which is something we care
// about[1]. For that reason we make use of a rangefeed buffer, accumulating raw
// rangefeed updates and flushing them out en-masse in timestamp order when the
// rangefeed frontier is bumped[2]. If the buffer overflows (as dictated by the
// memory limit the KVSubscriber is instantiated with), the old rangefeed is
// wound down and a new one re-established.
//
// When running into the internal errors described above, it's safe for us to
// re-establish the underlying rangefeeds. When re-establishing a new rangefeed
// and populating a spanconfig.Store using the contents of the initial scan[3],
// we wish to preserve the existing spanconfig.StoreReader. Discarding it would
// entail either blocking all external readers until a new
// spanconfig.StoreReader was fully populated, or presenting an inconsistent
// view of the spanconfig.Store that's currently being populated. For new
// rangefeeds what we do then is route all updates from the initial scan to a
// fresh spanconfig.Store, and once the initial scan is done, swap at the source
// for the exported spanconfig.StoreReader. During the initial scan, concurrent
// readers would continue to observe the last spanconfig.StoreReader if any.
// After the swap, it would observe the more up-to-date source instead. Future
// incremental updates will also target the new source. When this source swap
// occurs, we inform the handler of the need to possibly refresh its view of all
// configs.
//
// TODO(irfansharif): When swapping the old spanconfig.StoreReader for the new,
// instead of informing registered handlers with an everything [min,max) span,
// we could diff the two data structures and only emit targeted updates.
//
// [1]: For a given key k, it's config may be stored as part of a larger span S
//      (where S.start <= k < S.end). It's possible for S to get deleted and
//      replaced with sub-spans S1...SN in the same transaction if the span is
//      getting split. When applying these updates, we need to make sure to
//      process the deletion event for S before processing S1...SN.
// [2]: In our example above deleting the config for S and adding configs for
//      S1...SN we want to make sure that we apply the full set of updates all
//      at once -- lest we expose the intermediate state where the config for S
//      was deleted but the configs for S1...SN were not yet applied.
// [3]: TODO(irfansharif): When tearing down the subscriber due to underlying
//      errors, we could also capture a checkpoint to use the next time the
//      subscriber is established. That way we can avoid the full initial scan
//      over the span configuration state and simply pick up where we left off
//      with our existing spanconfig.Store.
type KVSubscriber struct {
	fallback roachpb.SpanConfig
	knobs    *spanconfig.TestingKnobs

	rfc *rangefeedcache.Watcher

	mu struct { // serializes between Start and external threads
		syncutil.RWMutex
		lastUpdated hlc.Timestamp
		// internal is the internal spanconfig.Store maintained by the
		// KVSubscriber. A read-only view over this store is exposed as part of
		// the interface. When re-subscribing, a fresh spanconfig.Store is
		// populated while the exposed spanconfig.StoreReader appears static.
		// Once sufficiently caught up, the fresh spanconfig.Store is swapped in
		// and the old discarded. See type-level comment for more details.
		internal *spanconfigstore.Store
		handlers []handler
	}
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
		fallback: fallback,
		knobs:    knobs,
	}
	var rfCacheKnobs *rangefeedcache.TestingKnobs
	if knobs != nil {
		rfCacheKnobs, _ = knobs.KVSubscriberRangeFeedKnobs.(*rangefeedcache.TestingKnobs)
	}
	s.rfc = rangefeedcache.NewWatcher(
		"spanconfig-subscriber",
		clock, rangeFeedFactory,
		int(bufferMemLimit/spanConfigurationsTableRowSize),
		[]roachpb.Span{spanConfigTableSpan},
		true, // withPrevValue
		newSpanConfigDecoder().translateEvent,
		s.handleUpdate,
		rfCacheKnobs,
	)
	s.mu.internal = spanConfigStore
	return s
}

// Start establishes a subscription (internally: rangefeed) over the global
// store of span configs. It fires off an async task to do so, re-establishing
// internally when retryable errors[1] occur and stopping only when the surround
// stopper is quiescing or the context canceled. All installed handlers are
// invoked in the single async task thread.
//
// [1]: It's possible for retryable errors to occur internally, at which point
//      we tear down the existing subscription and re-establish another. When
//      unsubscribed, the exposed spanconfig.StoreReader continues to be
//      readable (though no longer incrementally maintained -- the view gets
//      progressively staler overtime). Existing handlers are kept intact and
//      notified when the subscription is re-established. After re-subscribing,
//      the exported StoreReader will be up-to-date and continue to be
//      incrementally maintained.
func (s *KVSubscriber) Start(ctx context.Context, stopper *stop.Stopper) error {
	return rangefeedcache.Start(ctx, stopper, s.rfc, nil /* onError */)
}

// Subscribe installs a callback that's invoked with whatever span may have seen
// a config update.
func (s *KVSubscriber) Subscribe(fn func(context.Context, roachpb.Span)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.handlers = append(s.mu.handlers, handler{fn: fn})
}

// LastUpdated is part of the spanconfig.KVSubscriber interface.
func (s *KVSubscriber) LastUpdated() hlc.Timestamp {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.mu.lastUpdated
}

// NeedsSplit is part of the spanconfig.KVSubscriber interface.
func (s *KVSubscriber) NeedsSplit(ctx context.Context, start, end roachpb.RKey) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.mu.internal.NeedsSplit(ctx, start, end)
}

// ComputeSplitKey is part of the spanconfig.KVSubscriber interface.
func (s *KVSubscriber) ComputeSplitKey(ctx context.Context, start, end roachpb.RKey) roachpb.RKey {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.mu.internal.ComputeSplitKey(ctx, start, end)
}

// GetSpanConfigForKey is part of the spanconfig.KVSubscriber interface.
func (s *KVSubscriber) GetSpanConfigForKey(
	ctx context.Context, key roachpb.RKey,
) (roachpb.SpanConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.mu.internal.GetSpanConfigForKey(ctx, key)
}

// GetProtectionTimestamps is part of the spanconfig.KVSubscriber interface.
func (s *KVSubscriber) GetProtectionTimestamps(
	ctx context.Context, sp roachpb.Span,
) (protectionTimestamps []hlc.Timestamp, asOf hlc.Timestamp, _ error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.mu.internal.ForEachOverlappingSpanConfig(ctx, sp,
		func(_ roachpb.Span, config roachpb.SpanConfig) error {
			for _, protection := range config.GCPolicy.ProtectionPolicies {
				protectionTimestamps = append(protectionTimestamps, protection.ProtectedTimestamp)
			}
			return nil
		}); err != nil {
		return nil, hlc.Timestamp{}, err
	}

	return protectionTimestamps, s.mu.lastUpdated, nil
}

func (s *KVSubscriber) handleUpdate(ctx context.Context, u rangefeedcache.Update) {
	switch u.Type {
	case rangefeedcache.CompleteUpdate:
		s.handleCompleteUpdate(ctx, u.Timestamp, u.Events)
	case rangefeedcache.IncrementalUpdate:
		s.handlePartialUpdate(ctx, u.Timestamp, u.Events)
	}
}

func (s *KVSubscriber) handleCompleteUpdate(
	ctx context.Context, ts hlc.Timestamp, events []rangefeedbuffer.Event,
) {
	freshStore := spanconfigstore.New(s.fallback)
	for _, ev := range events {
		freshStore.Apply(ctx, false /* dryrun */, ev.(*bufferEvent).Update)
	}
	s.mu.Lock()
	s.mu.internal = freshStore
	s.mu.lastUpdated = ts
	handlers := s.mu.handlers
	s.mu.Unlock()
	for i := range handlers {
		handler := &handlers[i] // mutated by invoke
		handler.invoke(ctx, keys.EverythingSpan)
	}
}

func (s *KVSubscriber) handlePartialUpdate(
	ctx context.Context, ts hlc.Timestamp, events []rangefeedbuffer.Event,
) {
	s.mu.Lock()
	for _, ev := range events {
		// TODO(irfansharif): We can apply a batch of updates atomically
		// now that the StoreWriter interface supports it; it'll let us
		// avoid this mutex.
		s.mu.internal.Apply(ctx, false /* dryrun */, ev.(*bufferEvent).Update)
	}
	s.mu.lastUpdated = ts
	handlers := s.mu.handlers
	s.mu.Unlock()

	for i := range handlers {
		handler := &handlers[i] // mutated by invoke
		for _, ev := range events {
			target := ev.(*bufferEvent).Update.Target
			handler.invoke(ctx, target.KeyspaceTargeted())
		}
	}
}

type handler struct {
	initialized bool // tracks whether we need to invoke with a [min,max) span first
	fn          func(ctx context.Context, update roachpb.Span)
}

func (h *handler) invoke(ctx context.Context, update roachpb.Span) {
	if !h.initialized {
		h.fn(ctx, keys.EverythingSpan)
		h.initialized = true

		if update.Equal(keys.EverythingSpan) {
			return // we can opportunistically avoid re-invoking with the same update
		}
	}

	h.fn(ctx, update)
}

type bufferEvent struct {
	spanconfig.Update
	ts hlc.Timestamp
}

// Timestamp implements the rangefeedbuffer.Event interface.
func (w *bufferEvent) Timestamp() hlc.Timestamp {
	return w.ts
}

var _ rangefeedbuffer.Event = &bufferEvent{}
