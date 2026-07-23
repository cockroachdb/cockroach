// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Cache caches a set of KVs in a set of spans using a rangefeed. The
// cache provides a consistent snapshot when available, but the snapshot
// may be stale.
type Cache struct {
	w                 *rangefeedcache.Watcher[*kvpb.RangeFeedValue]
	defaultZoneConfig *zonepb.ZoneConfig
	mu                struct {
		syncutil.RWMutex

		cfg *config.SystemConfig

		registry notificationRegistry
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
	const withRowTSInInitialScan = true
	c := Cache{
		defaultZoneConfig: defaultZoneConfig,
	}
	c.mu.registry = notificationRegistry{}

	spans := []roachpb.Span{
		codec.TableSpan(keys.DescriptorTableID),
		codec.TableSpan(keys.ZonesTableID),
	}
	c.w = rangefeedcache.NewWatcher(
		"system-config-cache", clock, f,
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
	if err := rangefeedcache.Start(ctx, stopper, c.w, nil /* onError */); err != nil {
		return err
	}
	return nil
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
	if c.mu.cfg != nil {
		ch <- struct{}{}
	}

	c.mu.registry[ch] = struct{}{}
	return ch, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		delete(c.mu.registry, ch)
	}
}

type keyValues []roachpb.KeyValue

func (k keyValues) Len() int           { return len(k) }
func (k keyValues) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k keyValues) Less(i, j int) bool { return k[i].Key.Compare(k[j].Key) < 0 }

var _ sort.Interface = (keyValues)(nil)

func (c *Cache) handleUpdate(
	_ context.Context, update rangefeedcache.Update[*kvpb.RangeFeedValue],
) {
	updateKVs := rangefeedbuffer.EventsToKVs(update.Events,
		rangefeedbuffer.RangeFeedValueEventToKV)
	c.mu.Lock()
	defer c.mu.Unlock()
	var updatedData []roachpb.KeyValue
	switch update.Type {
	case rangefeedcache.CompleteUpdate:
		sort.Sort(keyValues(updateKVs))
		updatedData = updateKVs
	case rangefeedcache.IncrementalUpdate:
		if len(updateKVs) == 0 {
			// Simply return since there is nothing interesting.
			return
		}
		// Note that handleUpdate is called synchronously, so we can use the
		// old snapshot as the basis for the new snapshot without any risk of
		// missing anything.
		prev := c.mu.cfg
		updatedData = rangefeedbuffer.MergeKVs(prev.Values, updateKVs)
	}

	updatedCfg := config.NewSystemConfig(c.defaultZoneConfig)
	updatedCfg.Values = updatedData
	c.setUpdatedConfigLocked(updatedCfg)
}

func (c *Cache) setUpdatedConfigLocked(updated *config.SystemConfig) {
	changed := c.mu.cfg != updated
	c.mu.cfg = updated
	if changed {
		c.mu.registry.notify()
	}
}

func passThroughTranslation(
	ctx context.Context, value *kvpb.RangeFeedValue,
) (*kvpb.RangeFeedValue, bool) {
	return value, value != nil
}

var _ config.SystemConfigProvider = (*Cache)(nil)

type notificationRegistry map[chan<- struct{}]struct{}

func (nr notificationRegistry) notify() {
	for ch := range nr {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
