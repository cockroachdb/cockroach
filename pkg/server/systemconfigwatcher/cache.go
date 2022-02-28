// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package systemconfigwatcher

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Cache caches a set of KVs in a set of spans using a rangefeed. The
// cache provides a consistent snapshot when available, but the snapshot
// may be stale.
type Cache struct {
	w                 *rangefeedcache.Watcher
	defaultZoneConfig *zonepb.ZoneConfig
	mu                struct {
		syncutil.RWMutex

		cfg       *config.SystemConfig
		timestamp hlc.Timestamp

		registered map[chan<- struct{}]struct{}
	}
}

// New constructs a new Cache.
func New(
	codec keys.SQLCodec, clock *hlc.Clock, f *rangefeed.Factory, defaultZoneConfig *zonepb.ZoneConfig,
) *Cache {
	// TODO(ajwerner): Deal with what happens if the system config has more than this
	// many rows.
	const bufferSize = 1 << 20 // infinite?
	const withPrevValue = false
	c := Cache{
		defaultZoneConfig: defaultZoneConfig,
	}
	c.mu.registered = make(map[chan<- struct{}]struct{})

	// TODO(ajwerner): Consider stripping this down to just watching
	// descriptor and zones.
	span := roachpb.Span{
		Key:    append(codec.TenantPrefix(), keys.SystemConfigSplitKey...),
		EndKey: append(codec.TenantPrefix(), keys.SystemConfigTableDataMax...),
	}
	c.w = rangefeedcache.NewWatcher(
		"system-config-cache", clock, f,
		bufferSize,
		[]roachpb.Span{span},
		withPrevValue,
		passThroughTranslation,
		c.handleUpdate,
		nil)
	return &c
}

// Start starts the cache.
func (c *Cache) Start(ctx context.Context, stopper *stop.Stopper) error {
	return rangefeedcache.Start(ctx, stopper, c.w, nil /* onError */)
}

// GetSystemConfig is part of the config.SystemConfigProvider interface.
func (c *Cache) GetSystemConfig() *config.SystemConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.cfg
}

// RegisterSystemConfigChannel is part of the config.SystemConfigProvider
// interface.
func (c *Cache) RegisterSystemConfigChannel() (_ <-chan struct{}, unregister func()) {
	ch := make(chan struct{}, 1)
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mu.registered[ch] = struct{}{}
	return ch, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		delete(c.mu.registered, ch)
	}
}

// LastUpdated returns the timestamp corresponding to the current state of
// the cache. Any subsequent call to GetSystemConfig will see a state that
// corresponds to a snapshot as least as new as this timestamp.
func (c *Cache) LastUpdated() hlc.Timestamp {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.timestamp
}

type keyValues []roachpb.KeyValue

func (k keyValues) Len() int           { return len(k) }
func (k keyValues) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k keyValues) Less(i, j int) bool { return k[i].Key.Compare(k[j].Key) < 0 }

var _ sort.Interface = (keyValues)(nil)

func (c *Cache) handleUpdate(_ context.Context, update rangefeedcache.Update) {
	updateKVs := rangefeedbuffer.EventsToKVs(update.Events,
		rangefeedbuffer.RangeFeedValueEventToKV)
	var updatedData []roachpb.KeyValue
	switch update.Type {
	case rangefeedcache.CompleteUpdate:
		sort.Sort(keyValues(updateKVs))
		updatedData = updateKVs
	case rangefeedcache.IncrementalUpdate:
		// Note that handleUpdate is called synchronously, so we can use the
		// old snapshot as the basis for the new snapshot without any risk of
		// missing anything.
		prev := c.GetSystemConfig()

		// If there is nothing interesting, just update the timestamp and
		// return without notifying anybody.
		if len(updateKVs) == 0 {
			c.setUpdatedConfig(prev, update.Timestamp)
			return
		}
		updatedData = rangefeedbuffer.MergeKVs(prev.Values, updateKVs)
	}

	updatedCfg := config.NewSystemConfig(c.defaultZoneConfig)
	updatedCfg.Values = updatedData
	c.setUpdatedConfig(updatedCfg, update.Timestamp)
}

func (c *Cache) setUpdatedConfig(updated *config.SystemConfig, ts hlc.Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	changed := c.mu.cfg != updated
	c.mu.cfg = updated
	c.mu.timestamp = ts
	if !changed {
		return
	}
	for ch := range c.mu.registered {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func passThroughTranslation(
	ctx context.Context, value *roachpb.RangeFeedValue,
) rangefeedbuffer.Event {
	return value
}

var _ config.SystemConfigProvider = (*Cache)(nil)
