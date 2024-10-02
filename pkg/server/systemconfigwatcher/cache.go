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
	w                   *rangefeedcache.Watcher[*kvpb.RangeFeedValue]
	defaultZoneConfig   *zonepb.ZoneConfig
	additionalKVsSource config.SystemConfigProvider
	mu                  struct {
		syncutil.RWMutex

		cfg *config.SystemConfig

		registry notificationRegistry

		// additionalKVs provides a mechanism for the creator of the
		// cache to provide additional values.
		//
		// This is used to support injecting some key-value pairs from the
		// system tenant into the system config.
		additionalKVs []roachpb.KeyValue
	}
}

// New constructs a new Cache.
func New(
	codec keys.SQLCodec, clock *hlc.Clock, f *rangefeed.Factory, defaultZoneConfig *zonepb.ZoneConfig,
) *Cache {
	return NewWithAdditionalProvider(
		codec, clock, f, defaultZoneConfig, nil, /* additionalProvider */
	)
}

// NewWithAdditionalProvider constructs a new Cache with the addition of
// another provider of a SystemConfig. This additional provider is used only
// for the KVs in its system config. The key-value pairs it provides should
// not overlap with those of this provider, if they do, the latest values
// will be preferred.
//
// This functionality exists to provide access to the system tenant's view
// of its zone configuration for RANGE DEFAULT and RANGE TENANTS. This is
// needed primarily in the mixed-version state before the tenant is in control
// of its own zone configurations.
//
// TODO(ajwerner): Remove this functionality once it's no longer needed in 22.2.
func NewWithAdditionalProvider(
	codec keys.SQLCodec,
	clock *hlc.Clock,
	f *rangefeed.Factory,
	defaultZoneConfig *zonepb.ZoneConfig,
	additional config.SystemConfigProvider,
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
	c.additionalKVsSource = additional

	spans := []roachpb.Span{
		{
			Key:    append(codec.TenantPrefix(), keys.SystemDescriptorTableSpan.Key...),
			EndKey: append(codec.TenantPrefix(), keys.SystemDescriptorTableSpan.EndKey...),
		},
		{
			Key:    append(codec.TenantPrefix(), keys.SystemZonesTableSpan.Key...),
			EndKey: append(codec.TenantPrefix(), keys.SystemZonesTableSpan.EndKey...),
		},
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
	if c.additionalKVsSource != nil {
		setAdditionalKeys := func() {
			if cfg := c.additionalKVsSource.GetSystemConfig(); cfg != nil {
				c.setAdditionalKeys(cfg.Values)
			}
		}
		ch, unregister := c.additionalKVsSource.RegisterSystemConfigChannel()
		// Check if there are any additional keys to set before returning from
		// start. This is mostly to make tests deterministic.
		select {
		case <-ch:
			setAdditionalKeys()
		default:
		}
		if err := stopper.RunAsyncTask(ctx, "systemconfigwatcher-additional", func(ctx context.Context) {
			for {
				select {
				case <-ctx.Done():
					return
				case <-stopper.ShouldQuiesce():
					return
				case <-ch:
					setAdditionalKeys()
				}
			}
		}); err != nil {
			unregister()
			return err
		}
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

func (c *Cache) setAdditionalKeys(kvs []roachpb.KeyValue) {
	c.mu.Lock()
	defer c.mu.Unlock()

	sort.Sort(keyValues(kvs))
	if c.mu.cfg == nil {
		c.mu.additionalKVs = kvs
		return
	}

	cloned := append([]roachpb.KeyValue(nil), c.mu.cfg.Values...)
	trimmed := append(trimOldKVs(cloned, c.mu.additionalKVs), kvs...)
	sort.Sort(keyValues(trimmed))
	c.mu.cfg = config.NewSystemConfig(c.defaultZoneConfig)
	c.mu.cfg.Values = trimmed
	c.mu.additionalKVs = kvs
	c.mu.registry.notify()
}

// trimOldKVs removes KVs from cloned where for all keys in prev.
// This function assumes that both cloned and prev are sorted.
func trimOldKVs(cloned, prev []roachpb.KeyValue) []roachpb.KeyValue {
	trimmed := cloned[:0]
	shouldSkip := func(clonedOrd int) (shouldSkip bool) {
		for len(prev) > 0 {
			if cmp := prev[0].Key.Compare(cloned[clonedOrd].Key); cmp >= 0 {
				return cmp == 0
			}
			prev = prev[1:]
		}
		return false
	}
	for i := range cloned {
		if !shouldSkip(i) {
			trimmed = append(trimmed, cloned[i])
		}
	}
	return trimmed
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
		updatedData = rangefeedbuffer.MergeKVs(c.mu.additionalKVs, updateKVs)
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
