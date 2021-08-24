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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// KVSubscriber is used to subscribe to global span configuration changes. It's
// a concrete implementation of the spanconfig.KVSubscriber interface.
//
// Internally we maintain a long-lived rangefeed over the global store of span
// configurations (system.span_configurations), applying the updates from it
// into an embedded spanconfig.Store. A read-only view (spanconfig.StoreReader)
// of this data structure is exposed as part of the KVSubscriber interface.
// Rangefeeds used as is don't offer any ordering guarantees with respect to
// updates made over non-overlapping keys. For that reason we make use of a
// rangefeed buffer, accumulating raw rangefeed updates and flushing them out
// en-masse in timestamp order when the rangefeed frontier is bumped. If we end
// up buffering too many values (dictated by the memory limit the KVSubscriber
// is instantiated with), a new rangefeed is established transparently.
//
// When establishing a new rangefeed and populating a spanconfig.Store using
// the contents of the initial scan, we wish to preserve the existing
// spanconfig.StoreReader. Discarding it would entail either blocking all
// readers until a new spanconfig.StoreReader was fully populated, or presenting
// an inconsistent view of the spanconfig.Store that's currently being
// populated. For new rangefeeds what we do then is route all updates from the
// initial scan to a fresh spanconfig.Store, and once the initial scan is done,
// swap at the "source" for the exported spanconfig.StoreReader. During the
// initial scan, concurrent readers would continue to observe the last
// spanconfig.StoreReader if any. After the swap, it would observe the more
// up-to-date source instead. Subsequent incremental updates target the new
// source. When this source swap occurs, we inform the handler of the need to
// possibly refresh its view of all configs.
//
// TODO(irfansharif): When swapping the old spanconfig.StoreReader for the new,
// instead of informing callers with an everything [min,max) span, we could diff
// the two data structures and only emit targeted updates.
type KVSubscriber struct {
	stopper          *stop.Stopper
	db               *kv.DB
	clock            *hlc.Clock
	bufferMemLimit   int64
	decoder          *spanConfigDecoder
	fallback         roachpb.SpanConfig
	rangefeedFactory *rangefeed.Factory
	started          int32 // accessed atomically
	// spanConfigTableSpan is typically the key span for
	// system.span_configurations, but overridable for testing purposes.
	spanConfigTableSpan roachpb.Span

	buffer    *rangefeedbuffer.Buffer
	internal  spanconfig.Store
	rangefeed *rangefeed.RangeFeed
	initialTS hlc.Timestamp

	mu struct {
		syncutil.Mutex

		handler            func(roachpb.Span)
		handlerInitialized bool

		// external is the read-only view of the spanconfig.Store maintained by
		// the KVSubscriber. It's usually an opaque reference to the internal
		// spanconfig.Store, but when re-establishing rangefeeds, a new
		// spanconfig.Store is maintained while the existing StoreReader remains
		// static until it's swapped out altogether. See the type-level comment
		// for more details.
		external spanconfig.StoreReader
	}

	knobs *spanconfig.TestingKnobs
}

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
		internal:            spanConfigStore,
		decoder:             newSpanConfigDecoder(),
		spanConfigTableSpan: spanConfigTableSpan,
		fallback:            fallback,
		knobs:               knobs,
	}
	s.mu.external = spanConfigStore
	return s
}

var _ spanconfig.KVSubscriber = &KVSubscriber{}

// Start establishes a persistent feed over the global state of KV span configs.
// It manages the lifestyle of these feeds, restarting/re-establishing them
// transparently.
func (s *KVSubscriber) Start(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&s.started, 0, 1) {
		return errors.AssertionFailedf("kvsubscriber already started")
	}
	return s.establishRangefeed(ctx)
}

// SubscribeToKVUpdates installs a callback that's invoked with whatever key
// span may have seen a config update.
func (s *KVSubscriber) SubscribeToKVUpdates(ctx context.Context, handler func(roachpb.Span)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.handler != nil {
		log.Fatalf(ctx, "found existing handler: only one allowed")
	}

	s.mu.handler = handler
}

func (s *KVSubscriber) establishRangefeed(ctx context.Context) error {
	s.internal = spanconfigstore.New(s.fallback)
	s.buffer = rangefeedbuffer.New(int(s.bufferMemLimit / spanConfigurationsTableRowSize))

	s.initialTS = s.clock.Now()
	s.rangefeed = s.rangefeedFactory.New("spanconfig-rangefeed", s.spanConfigTableSpan, s.initialTS,
		s.onValue,
		rangefeed.WithOnFrontierAdvance(s.onFrontierAdvance),
		rangefeed.WithInitialScan(s.onInitialScanDone),
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
	if err := s.rangefeed.Start(ctx); err != nil {
		return err
	}
	s.stopper.AddCloser(s.rangefeed)

	log.Info(ctx, "established range feed over span configurations table")
	return nil
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

func (s *KVSubscriber) onValue(ctx context.Context, ev *roachpb.RangeFeedValue) {
	deleted := !ev.Value.IsPresent()
	var value roachpb.Value
	if deleted {
		if !ev.PrevValue.IsPresent() {
			// It's possible to write a KV tombstone on top of another KV
			// tombstone -- both the new and old value will be
			// empty. We simply ignore these events.
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
		// TODO(irfansharif): Should we fatal instead? Seems bad to simply
		// skip over the update, even if done so non-silently.
		log.Errorf(ctx, "failed to decode span configuration update: %v", err)
		return
	}

	if log.ExpensiveLogEnabled(ctx, 1) {
		log.Infof(ctx, "received span configuration update for %s (deleted=%t)", entry.Span, deleted)
	}

	update := spanconfig.Update{Span: entry.Span}
	if !deleted {
		update.Config = entry.Config
	}

	err = s.buffer.Add(ctx, &bufferEvent{update, ev.Value.Timestamp})
	if err == nil {
		return // nothing to do here.
	}
	if !errors.Is(err, rangefeedbuffer.ErrBufferLimitExceeded) {
		log.Fatalf(ctx, "unexpected error: %v", err)
	}

	// TODO(irfansharif): We could instantiate with a retry.Options above
	// and re-establish accordingly.
	s.rangefeed.Close()
	if err := s.establishRangefeed(ctx); err != nil {
		log.Fatalf(ctx, "unexpected error: %v", err) // TODO(irfansharif): Do something better with this error?
	}
}

func (s *KVSubscriber) onFrontierAdvance(ctx context.Context, timestamp hlc.Timestamp) {
	if fn := s.knobs.KVSubscriberOnFrontierAdvanceInterceptor; fn != nil {
		fn(timestamp)
	}

	events := s.buffer.Flush(ctx, timestamp)
	for _, ev := range events {
		s.internal.Apply(ctx, ev.(*bufferEvent).Update, false /* dryrun */)
	}
	s.mu.Lock()
	handler := s.mu.handler
	needsInitialization := !s.mu.handlerInitialized
	s.mu.Unlock()

	if handler == nil {
		return
	}

	if needsInitialization {
		handler(keys.EverythingSpan)
		s.mu.Lock()
		s.mu.handlerInitialized = true
		s.mu.Unlock()
	}
	for _, ev := range events {
		handler(ev.(*bufferEvent).Update.Span)
	}
}

func (s *KVSubscriber) onInitialScanDone(ctx context.Context) {
	events := s.buffer.Flush(ctx, s.initialTS)
	for _, ev := range events {
		s.internal.Apply(ctx, ev.(*bufferEvent).Update, false /* dryrun */)
	}

	s.mu.Lock()
	s.mu.external = s.internal
	handler := s.mu.handler
	needsInitialization := !s.mu.handlerInitialized
	s.mu.Unlock()

	if handler == nil {
		return
	}

	if needsInitialization {
		handler(keys.EverythingSpan)
		s.mu.Lock()
		s.mu.handlerInitialized = true
		s.mu.Unlock()
		return
	}

	handler(keys.EverythingSpan)
}

type bufferEvent struct {
	spanconfig.Update
	ts hlc.Timestamp
}

func (w *bufferEvent) Timestamp() hlc.Timestamp {
	return w.ts
}

var _ rangefeedbuffer.Event = &bufferEvent{}
