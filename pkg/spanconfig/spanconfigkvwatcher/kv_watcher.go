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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// New instantiates a KVWatcher that watches span configuration updates on
//syste.span_configurations.
func New(db *kv.DB, clock *hlc.Clock, rangeFeedFactory *rangefeed.Factory) *KVWatcher {
	return &KVWatcher{
		db:               db,
		clock:            clock,
		rangeFeedFactory: rangeFeedFactory,
	}
}

// KVWatcher is used to watch for span configuration changes using a rangefeed
// over system.span_configurations.
type KVWatcher struct {
	db               *kv.DB
	clock            *hlc.Clock
	rangeFeedFactory *rangefeed.Factory
}

var _ spanconfig.KVWatcher = &KVWatcher{}

// WatchForKVUpdates will kick off the span config watcher. It establishes a
// rangefeed over the span configurations table, and sends forth updates to it
// through the returned channel.
func (w *KVWatcher) WatchForKVUpdates(
	ctx context.Context, stopper *stop.Stopper,
) (<-chan spanconfig.Update, error) {
	updateCh := make(chan spanconfig.Update)
	spanConfigTableStart := keys.SystemSQLCodec.TablePrefix(keys.SpanConfigurationsTableID)
	spanConfigTableSpan := roachpb.Span{
		Key:    spanConfigTableStart,
		EndKey: spanConfigTableStart.PrefixEnd(),
	}
	rowDecoder := NewSpanConfigDecoder()

	handleUpdate := func(
		ctx context.Context, ev *roachpb.RangeFeedValue,
	) {
		deleted := !ev.Value.IsPresent()
		var value roachpb.Value
		if deleted {
			// Since the end key is not part of the primary key, we need to
			// decode the previous value in order to determine what it is.
			value = ev.PrevValue
		} else {
			value = ev.Value
		}
		entry, err := rowDecoder.Decode(roachpb.KeyValue{
			Key:   ev.Key,
			Value: value,
		})
		if err != nil {
			log.Warningf(ctx, "failed to decode span configuration update: %v", err)
			return
		}

		if log.V(1) {
			log.Infof(ctx, "received span configuration update for %s (deleted=%t)", entry.Span, deleted)
		}

		select {
		case <-ctx.Done():
		case updateCh <- spanconfig.Update{Entry: entry, Deleted: deleted}:
		}
	}

	rf, err := w.rangeFeedFactory.RangeFeed(
		ctx, "spanconfig-rangefeed", spanConfigTableSpan, w.clock.Now(), handleUpdate,
		rangefeed.WithDiff(),
		rangefeed.WithInitialScan(nil),
		rangefeed.WithOnInitialScanError(func(ctx context.Context, err error) (shouldFail bool) {
			// TODO(zcfgs-pod): Are there errors that should prevent us from
			// retrying again? The setting watcher considers grpc auth errors as
			// permanent.
			return false
		}),
	)
	if err != nil {
		return nil, err
	}
	stopper.AddCloser(rf)

	log.Info(ctx, "established range feed over span configurations table")
	return updateCh, nil
}
