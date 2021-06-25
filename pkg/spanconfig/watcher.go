// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfig

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// NewWatcher instantiates a span configuration watcher.
func NewWatcher(db *kv.DB, clock *hlc.Clock) *Watcher {
	return &Watcher{
		db:         db,
		clock:      clock,
		rowDecoder: newRowDecoder(),
	}
}

// Watcher is used to watch for span configuration changes using a rangefeed.
type Watcher struct {
	db         *kv.DB
	clock      *hlc.Clock
	rowDecoder *rowDecoder
}

// Update is the the unit of what the span config watcher emits.
type Update struct {
	// Entry captures the keyspan and corresponding config that has been
	// updated. If deleted is false, the embedded config is what the keyspan was
	// updated with.
	Entry roachpb.SpanConfigEntry

	// Deleted is true if the span config entry has been deleted.
	Deleted bool
}

func (e *Update) String() string {
	return fmt.Sprintf("span config update: span=%s/deleted=%t", e.Entry.Span, e.Deleted)
}

func (s *Watcher) Start(ctx context.Context, stopper *stop.Stopper) (<-chan Update, error) {
	updateCh := make(chan Update)
	spanConfigTableStart := keys.SystemSQLCodec.TablePrefix(keys.SpanConfigurationsTableID)
	spanConfigTableSpan := roachpb.Span{
		Key:    spanConfigTableStart,
		EndKey: spanConfigTableStart.PrefixEnd(),
	}

	handleUpdate := func(
		ctx context.Context, ev *roachpb.RangeFeedValue,
	) {
		deleted := !ev.Value.IsPresent()
		var value roachpb.Value
		if !deleted {
			value = ev.Value
		} else {
			// Since the end key is not part of the primary key, we need to
			// decode the previous value in order to determine what it is.
			value = ev.PrevValue
		}
		entry, err := s.rowDecoder.Decode(roachpb.KeyValue{
			Key:   ev.Key,
			Value: value,
		})
		if err != nil {
			log.Warningf(ctx, "failed to decode span configuration update (key=%v, value=%v, deleted=%t): %v",
				ev.Key, ev.Value, deleted, err)
			return
		}

		if log.V(1) {
			log.Infof(ctx, "received span configuration update for %s (deleted=%t)", entry.Span, deleted)
		}

		select {
		case <-ctx.Done():
		case updateCh <- Update{Entry: entry, Deleted: deleted}:
		}
	}

	rangeFeedFactory, err := rangefeed.NewFactory(stopper, s.db, nil)
	if err != nil {
		return nil, err
	}
	rf, err := rangeFeedFactory.RangeFeed(
		ctx, "spanconfig-rangefeed", spanConfigTableSpan, s.clock.Now(), handleUpdate,
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
