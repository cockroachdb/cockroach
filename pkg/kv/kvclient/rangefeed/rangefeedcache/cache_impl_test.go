// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeedcache

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/redact"
)

// Cache caches a set of KVs in a set of spans using a rangefeed. The
// cache provides a consistent snapshot when available, but the snapshot
// may be stale.
type Cache struct {
	w *Watcher[*kvpb.RangeFeedValue]

	mu struct {
		syncutil.RWMutex

		data      []roachpb.KeyValue
		timestamp hlc.Timestamp
	}
}

// NewCache constructs a new Cache.
func NewCache(
	name redact.SafeString, clock *hlc.Clock, f *rangefeed.Factory, spans []roachpb.Span,
) *Cache {
	// TODO(ajwerner): Deal with what happens if the system config has more than this
	// many rows.
	const bufferSize = 1 << 20 // infinite?
	const withPrevValue = false
	const withRowTSInInitialScan = true
	c := Cache{}
	c.w = NewWatcher(
		name, clock, f,
		bufferSize,
		spans,
		withPrevValue,
		withRowTSInInitialScan,
		passThroughTranslation,
		c.handleUpdate,
		nil)
	return &c
}

// Start starts the cache.
func (c *Cache) Start(ctx context.Context, stopper *stop.Stopper) error {
	return Start(ctx, stopper, c.w, nil /* onError */)
}

// GetSnapshot returns the set of cached KVs over the spans and the timestamp
// from which it applies. If no snapshot is available, false will be returned.
func (c *Cache) GetSnapshot() ([]roachpb.KeyValue, hlc.Timestamp, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.mu.timestamp.IsEmpty() {
		return nil, hlc.Timestamp{}, false
	}
	return c.mu.data, c.mu.timestamp, true
}

func (c *Cache) handleUpdate(ctx context.Context, update Update[*kvpb.RangeFeedValue]) {
	updateKVs := rangefeedbuffer.EventsToKVs(update.Events,
		rangefeedbuffer.RangeFeedValueEventToKV)
	var updatedData []roachpb.KeyValue
	switch update.Type {
	case CompleteUpdate:
		updatedData = updateKVs
	case IncrementalUpdate:
		// Note that handleUpdate is synchronous within the underlying watcher,
		// so we can use the old snapshot as the basis for the new snapshot
		// without any risk of a concurrent update modifying the snapshot.
		prev, _, _ := c.GetSnapshot() // okay if prev is nil
		updatedData = rangefeedbuffer.MergeKVs(prev, updateKVs)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.data = updatedData
	c.mu.timestamp = update.Timestamp
}

func passThroughTranslation(
	ctx context.Context, value *kvpb.RangeFeedValue,
) (*kvpb.RangeFeedValue, bool) {
	return value, value != nil
}
