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

// KVSubscriber is used to subscribe to span configuration in KV.
type KVSubscriber struct {
	stopper          *stop.Stopper
	db               *kv.DB
	clock            *hlc.Clock
	memoryLimit      int64
	decoder          *spanConfigDecoder
	fallback         roachpb.SpanConfig
	rangeFeedFactory *rangefeed.Factory

	// spanConfigTableSpan is typically the key span for
	// system.span_configurations, but overridable for testing purposes.
	spanConfigTableSpan roachpb.Span

	mu struct {
		syncutil.Mutex

		handler            func(roachpb.Span)
		handlerInitialized bool
		external           spanconfig.StoreReader
	}

	buffer   *rangefeedbuffer.Buffer
	internal spanconfig.Store
	rf       *rangefeed.RangeFeed
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

const spanConfigurationsTableRowSize = 1 << 10 // 1 KB

// New instantiates a KVSubscriber.
func New(
	stopper *stop.Stopper,
	db *kv.DB,
	clock *hlc.Clock,
	rangeFeedFactory *rangefeed.Factory,
	spanConfigurationsTableID uint32,
	memoryLimit int64,
	fallback roachpb.SpanConfig,
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
	s := &KVSubscriber{
		stopper:             stopper,
		db:                  db,
		clock:               clock,
		memoryLimit:         memoryLimit,
		rangeFeedFactory:    rangeFeedFactory,
		internal:            spanConfigStore,
		decoder:             newSpanConfigDecoder(),
		spanConfigTableSpan: spanConfigTableSpan,
		fallback:            fallback,
	}
	s.mu.external = spanConfigStore
	return s
}

var _ spanconfig.KVSubscriber = &KVSubscriber{}

// Start establishes a persistent feed over the global state of KV span configs.
// It manages the lifestyle of these feeds, restarting/re-establishing them
// transparently. All subs
func (s *KVSubscriber) Start(ctx context.Context) error {
	return s.establishRangefeed(ctx)
}

// SubscribeToKVUpdates installs a callback that's invoked with whatever key
// span may have seen a config update.
func (s *KVSubscriber) SubscribeToKVUpdates(ctx context.Context, handler func(roachpb.Span)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.handler != nil {
		log.Fatalf(ctx, "found existing handler, only one supported")
	}

	s.mu.handler = handler
}

func (s *KVSubscriber) establishRangefeed(ctx context.Context) error {
	s.internal = spanconfigstore.New(s.fallback)
	s.buffer = rangefeedbuffer.New(int(s.memoryLimit / spanConfigurationsTableRowSize))

	initialTS := s.clock.Now()
	s.rf = s.rangeFeedFactory.New("spanconfig-rangefeed", s.spanConfigTableSpan, initialTS,
		s.onValue(),
		rangefeed.WithOnFrontierAdvance(s.onFrontierAdvance()),
		rangefeed.WithInitialScan(s.onInitialScanDone(initialTS)),
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
	if err := s.rf.Start(ctx); err != nil {
		return err
	}
	s.stopper.AddCloser(s.rf)

	log.Info(ctx, "established range feed over span configurations table")
	return nil
}

func (s *KVSubscriber) onValue() rangefeed.OnValue {
	return func(ctx context.Context, ev *roachpb.RangeFeedValue) {
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
		s.rf.Close()
		if err := s.establishRangefeed(ctx); err != nil {
			log.Fatalf(ctx, "unexpected error: %v", err) // TODO(irfansharif): Do something better with this error?
		}
	}
}

func (s *KVSubscriber) onFrontierAdvance() rangefeed.OnFrontierAdvance {
	return func(ctx context.Context, timestamp hlc.Timestamp) {
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
}

func (s *KVSubscriber) onInitialScanDone(timestamp hlc.Timestamp) rangefeed.OnInitialScanDone {
	return func(ctx context.Context) {
		events := s.buffer.Flush(ctx, timestamp)
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
}

type bufferEvent struct {
	spanconfig.Update
	ts hlc.Timestamp
}

func (w *bufferEvent) Timestamp() hlc.Timestamp {
	return w.ts
}

var _ rangefeedbuffer.Event = &bufferEvent{}
