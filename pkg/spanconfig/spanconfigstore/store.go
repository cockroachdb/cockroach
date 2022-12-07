// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigstore

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// EnabledSetting is a hidden cluster setting to enable the use of the span
// configs infrastructure in KV. It switches each store in the cluster from
// using the gossip backed system config span to instead using the span configs
// infrastructure. It has no effect if COCKROACH_DISABLE_SPAN_CONFIGS
// is set.
//
// TODO(irfansharif): We should remove this.
var EnabledSetting = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"spanconfig.store.enabled",
	`use the span config infrastructure in KV instead of the system config span`,
	true,
)

// FallbackConfigOverride is a hidden cluster setting to override the fallback
// config used for ranges with no explicit span configs set.
var FallbackConfigOverride = settings.RegisterProtobufSetting(
	settings.SystemOnly,
	"spanconfig.store.fallback_config_override",
	"override the fallback used for ranges with no explicit span configs set",
	&roachpb.SpanConfig{},
)

// Store is an in-memory data structure to store, retrieve, and incrementally
// update the span configuration state. Internally, it makes use of an interval
// btree based spanConfigStore to store non-overlapping span configurations that
// target keyspans. It's safe for concurrent use.
type Store struct {
	mu struct {
		syncutil.RWMutex
		spanConfigStore       *spanConfigStore
		systemSpanConfigStore *systemSpanConfigStore
	}

	settings *cluster.Settings

	// fallback is the span config we'll fall back on in the absence of
	// something more specific.
	//
	// TODO(irfansharif): We're using a static[1] fallback span config here, we
	// could instead have this directly track the host tenant's RANGE DEFAULT
	// config, or go a step further and use the tenant's own RANGE DEFAULT
	// instead if the key is within the tenant's keyspace. We'd have to thread
	// that through the KVAccessor interface by reserving special keys for these
	// default configs.
	//
	// [1]: Modulo the private spanconfig.store.fallback_config_override, which
	//      applies globally.
	fallback roachpb.SpanConfig

	knobs *spanconfig.TestingKnobs
}

var _ spanconfig.Store = &Store{}

// New instantiates a span config store with the given fallback.
func New(
	fallback roachpb.SpanConfig, settings *cluster.Settings, knobs *spanconfig.TestingKnobs,
) *Store {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	s := &Store{
		settings: settings,
		fallback: fallback,
		knobs:    knobs,
	}
	s.mu.spanConfigStore = newSpanConfigStore(settings, s.knobs)
	s.mu.systemSpanConfigStore = newSystemSpanConfigStore()
	return s
}

// NeedsSplit is part of the spanconfig.StoreReader interface.
func (s *Store) NeedsSplit(ctx context.Context, start, end roachpb.RKey) bool {
	return len(s.ComputeSplitKey(ctx, start, end)) > 0
}

// ComputeSplitKey is part of the spanconfig.StoreReader interface.
func (s *Store) ComputeSplitKey(ctx context.Context, start, end roachpb.RKey) roachpb.RKey {
	s.mu.RLock()
	defer s.mu.RUnlock()

	splitKey, err := s.mu.spanConfigStore.computeSplitKey(start, end)
	if err != nil {
		log.FatalfDepth(ctx, 3, "unable to compute split key: %v", err)
	}
	return splitKey
}

// GetSpanConfigForKey is part of the spanconfig.StoreReader interface.
func (s *Store) GetSpanConfigForKey(
	ctx context.Context, key roachpb.RKey,
) (roachpb.SpanConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.getSpanConfigForKeyRLocked(ctx, key)
}

// getSpanConfigForKeyRLocked is like GetSpanConfigForKey but requires the
// caller to hold the Store read lock.
func (s *Store) getSpanConfigForKeyRLocked(
	ctx context.Context, key roachpb.RKey,
) (roachpb.SpanConfig, error) {
	conf, found := s.mu.spanConfigStore.getSpanConfigForKey(ctx, key)
	if !found {
		conf = s.getFallbackConfig()
	}
	return s.mu.systemSpanConfigStore.combine(key, conf)
}

func (s *Store) getFallbackConfig() roachpb.SpanConfig {
	if conf := FallbackConfigOverride.Get(&s.settings.SV).(*roachpb.SpanConfig); !conf.IsEmpty() {
		return *conf
	}
	return s.fallback
}

// Apply is part of the spanconfig.StoreWriter interface.
func (s *Store) Apply(
	ctx context.Context, dryrun bool, updates ...spanconfig.Update,
) (deleted []spanconfig.Target, added []spanconfig.Record) {
	deleted, added, err := s.applyInternal(ctx, dryrun, updates...)
	if err != nil {
		log.Fatalf(ctx, "%v", err)
	}
	return deleted, added
}

// ForEachOverlappingSpanConfig is part of the spanconfig.Store interface.
func (s *Store) ForEachOverlappingSpanConfig(
	ctx context.Context, span roachpb.Span, f func(roachpb.Span, roachpb.SpanConfig) error,
) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.spanConfigStore.forEachOverlapping(span, func(sp roachpb.Span, conf roachpb.SpanConfig) error {
		config, err := s.getSpanConfigForKeyRLocked(ctx, roachpb.RKey(sp.Key))
		if err != nil {
			return err
		}
		return f(sp, config)
	})
}

// Clone returns a copy of the Store.
func (s *Store) Clone() *Store {
	s.mu.Lock()
	defer s.mu.Unlock()

	clone := New(s.fallback, s.settings, s.knobs)
	clone.mu.spanConfigStore = s.mu.spanConfigStore.clone()
	clone.mu.systemSpanConfigStore = s.mu.systemSpanConfigStore.clone()
	return clone
}

func (s *Store) applyInternal(
	ctx context.Context, dryrun bool, updates ...spanconfig.Update,
) (deleted []spanconfig.Target, added []spanconfig.Record, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Accumulate all spanStoreUpdates. We do this because we want to apply
	// a set of updates at once instead of individually, to correctly construct
	// the deleted/added slices.
	spanStoreUpdates := make([]spanconfig.Update, 0, len(updates))
	systemSpanConfigStoreUpdates := make([]spanconfig.Update, 0, len(updates))
	for _, update := range updates {
		switch {
		case update.GetTarget().IsSpanTarget():
			spanStoreUpdates = append(spanStoreUpdates, update)
		case update.GetTarget().IsSystemTarget():
			systemSpanConfigStoreUpdates = append(systemSpanConfigStoreUpdates, update)
		default:
			return nil, nil, errors.AssertionFailedf("unknown target type")
		}
	}
	deletedSpans, addedEntries, err := s.mu.spanConfigStore.apply(ctx, dryrun, spanStoreUpdates...)
	if err != nil {
		return nil, nil, err
	}

	for _, sp := range deletedSpans {
		deleted = append(deleted, spanconfig.MakeTargetFromSpan(sp))
	}

	for _, entry := range addedEntries {
		record, err := spanconfig.MakeRecord(
			spanconfig.MakeTargetFromSpan(entry.span),
			entry.conf(),
		)
		if err != nil {
			return nil, nil, err
		}
		added = append(added, record)
	}

	deletedSystemTargets, addedSystemSpanConfigRecords, err := s.mu.systemSpanConfigStore.apply(
		systemSpanConfigStoreUpdates...,
	)
	if err != nil {
		return nil, nil, err
	}
	for _, systemTarget := range deletedSystemTargets {
		deleted = append(deleted, spanconfig.MakeTargetFromSystemTarget(systemTarget))
	}
	added = append(added, addedSystemSpanConfigRecords...)

	return deleted, added, nil
}

// Iterate iterates through all the entries in the Store in sorted order.
func (s *Store) Iterate(f func(spanconfig.Record) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// System targets are considered to be less than span targets.
	if err := s.mu.systemSpanConfigStore.iterate(f); err != nil {
		return err
	}
	return s.mu.spanConfigStore.forEachOverlapping(
		keys.EverythingSpan,
		func(sp roachpb.Span, conf roachpb.SpanConfig) error {
			record, err := spanconfig.MakeRecord(spanconfig.MakeTargetFromSpan(sp), conf)
			if err != nil {
				return err
			}
			return f(record)
		})
}
