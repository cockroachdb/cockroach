// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigkvwatcher

import (
	"context"
	"errors"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// KVWatcher is used to watch for span configuration changes over
// system.span_configurations.
type KVWatcher struct {
	stopper          *stop.Stopper
	db               *kv.DB
	clock            *hlc.Clock
	buffer           *rangefeedbuffer.Buffer
	rangeFeedFactory *rangefeed.Factory

	// spanConfigurationsTableID is typically the table ID for
	// system.span_configurations, but overridable for testing purposes.
	spanConfigurationsTableID uint32
}

const spanConfigurationsTableRowSize = 1 << 10 // 1 KB

// New instantiates a KVWatcher.
func New(
	stopper *stop.Stopper,
	db *kv.DB,
	clock *hlc.Clock,
	rangeFeedFactory *rangefeed.Factory,
	spanConfigurationsTableID uint32,
	memoryLimit int64,
) *KVWatcher {
	return &KVWatcher{
		stopper:          stopper,
		db:               db,
		clock:            clock,
		buffer:           rangefeedbuffer.New(int(memoryLimit / spanConfigurationsTableRowSize)),
		rangeFeedFactory: rangeFeedFactory,

		spanConfigurationsTableID: spanConfigurationsTableID,
	}
}

var _ spanconfig.KVWatcher = &KVWatcher{}

// WatchForKVUpdates will kick off the KV span config watcher. It establishes a
// rangefeed over the span configurations table, and propagates updates to it
// through the returned channel.
func (w *KVWatcher) WatchForKVUpdates(ctx context.Context) (<-chan spanconfig.Update, error) {
	updateCh := make(chan spanconfig.Update)
	spanConfigTableStart := keys.SystemSQLCodec.IndexPrefix(
		w.spanConfigurationsTableID,
		keys.SpanConfigurationsTablePrimaryKeyIndexID,
	)
	spanConfigTableSpan := roachpb.Span{
		Key:    spanConfigTableStart,
		EndKey: spanConfigTableStart.PrefixEnd(),
	}

	rowDecoder := newSpanConfigDecoder()
	handleUpdate := func(
		ctx context.Context, ev *roachpb.RangeFeedValue,
	) {
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
		entry, err := rowDecoder.decode(roachpb.KeyValue{
			Key:   ev.Key,
			Value: value,
		})
		if err != nil {
			log.Warningf(ctx, "failed to decode span configuration update: %v", err)
			return
		}

		if log.ExpensiveLogEnabled(ctx, 1) {
			log.Infof(ctx, "received span configuration update for %s (deleted=%t)", entry.Span, deleted)
		}

		update := spanconfig.Update{Span: entry.Span}
		if !deleted {
			update.Config = entry.Config
		}
		select {
		case <-ctx.Done():
		case updateCh <- update:
		}
	}

	rf, err := w.rangeFeedFactory.RangeFeed(
		ctx, "spanconfig-rangefeed", spanConfigTableSpan, w.clock.Now(), handleUpdate,
		rangefeed.WithDiff(),
		rangefeed.WithInitialScan(nil /* OnInitialScanDone */),
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
	if err != nil {
		return nil, err
	}
	w.stopper.AddCloser(rf)

	log.Info(ctx, "established range feed over span configurations table")
	return updateCh, nil
}

// WatchForKVUpdatesNew will kick off the KV span config watcher. It establishes
// a rangefeed over the span configurations table, and relays ordered updates
// through the provided callback.
//
// XXX: How are buffer overflows supposed to be handled? We want to error out
// somehow (part of the handle?) and have the caller re-establish the watcher. I
// suspect we want to discard our existing spanconfig.Store at that point at
// start afresh.
func (w *KVWatcher) WatchForKVUpdatesNew(
	ctx context.Context, handle func(spanconfig.Update),
) error {
	spanConfigTableStart := keys.SystemSQLCodec.IndexPrefix(
		w.spanConfigurationsTableID,
		keys.SpanConfigurationsTablePrimaryKeyIndexID,
	)
	spanConfigTableSpan := roachpb.Span{
		Key:    spanConfigTableStart,
		EndKey: spanConfigTableStart.PrefixEnd(),
	}

	var rf *rangefeed.RangeFeed

	rowDecoder := newSpanConfigDecoder()
	onRangefeedValue := func(ctx context.Context, ev *roachpb.RangeFeedValue) {
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
		entry, err := rowDecoder.decode(roachpb.KeyValue{
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

		err = w.buffer.Add(ctx, &bufferEvent{update, ev.Value.Timestamp})
		if errors.Is(err, rangefeedbuffer.ErrBufferLimitExceeded) {
			rf.Close() // Close
		} else {
			log.Fatalf(ctx, "unexpected error: %v", err)
		}
	}
	onFrontierAdvance := func(ctx context.Context, timestamp hlc.Timestamp) {
		events := w.buffer.Flush(ctx, timestamp)
		for _, ev := range events {
			handle(ev.(*bufferEvent).Update)
		}
	}

	// XXX: Could have an InitialScanDone to
	var err error
	rf, err = w.rangeFeedFactory.RangeFeed(
		ctx, "spanconfig-rangefeed", spanConfigTableSpan, w.clock.Now(),
		onRangefeedValue,
		rangefeed.WithOnFrontierAdvance(onFrontierAdvance),
		rangefeed.WithDiff(),
		rangefeed.WithInitialScan(nil /* OnInitialScanDone */),
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
	if err != nil {
		return err
	}
	w.stopper.AddCloser(rf)

	log.Info(ctx, "established range feed over span configurations table")
	return nil
}

type bufferEvent struct {
	spanconfig.Update
	ts hlc.Timestamp
}

func (w *bufferEvent) Timestamp() hlc.Timestamp {
	return w.ts
}

var _ rangefeedbuffer.Event = &bufferEvent{}
