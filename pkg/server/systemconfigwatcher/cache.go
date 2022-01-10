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

		registered []chan<- struct{}
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
func (c *Cache) RegisterSystemConfigChannel() <-chan struct{} {
	ch := make(chan struct{}, 1)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.registered = append(c.mu.registered, ch)
	return ch
}

func (c *Cache) handleUpdate(ctx context.Context, update rangefeedcache.Update) {
	updateKVs := rangefeedbuffer.EventsToKVs(update.Events,
		rangefeedbuffer.RangeFeedValueEventToKV)
	var updatedData []roachpb.KeyValue
	switch update.Type {
	case rangefeedcache.CompleteUpdate:
		updatedData = updateKVs
	case rangefeedcache.PartialUpdate:
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
	toNotify := c.setUpdatedConfig(updatedCfg, update.Timestamp)
	for _, c := range toNotify {
		select {
		case c <- struct{}{}:
		default:
		}
	}
}

func (c *Cache) setUpdatedConfig(
	updated *config.SystemConfig, ts hlc.Timestamp,
) (toNotify []chan<- struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.cfg = updated
	c.mu.timestamp = ts
	return c.mu.registered
}

func passThroughTranslation(
	ctx context.Context, value *roachpb.RangeFeedValue,
) rangefeedbuffer.Event {
	return value
}

var _ config.SystemConfigProvider = (*Cache)(nil)
