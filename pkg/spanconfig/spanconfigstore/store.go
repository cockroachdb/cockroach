// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package spanconfigstore exposes utilities for storing and retrieving
// SpanConfigs associated with a single span.
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

// FallbackConfigOverride is a hidden cluster setting to override the fallback
// config used for ranges with no explicit span configs set.
var FallbackConfigOverride = settings.RegisterProtobufSetting(
	settings.SystemOnly,
	"spanconfig.store.fallback_config_override",
	"override the fallback used for ranges with no explicit span configs set",
	&roachpb.SpanConfig{},
)

// BoundsEnabled is a hidden cluster setting which controls whether
// SpanConfigBounds should be consulted (to perform clamping of secondary tenant
// span configurations) before serving span configs.
var boundsEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"spanconfig.bounds.enabled",
	"dictates whether span config bounds are consulted when serving span configs for secondary tenants",
	true,
	settings.WithPublic)

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

	// boundsReader provides a handle to the global SpanConfigBounds state.
	boundsReader BoundsReader
}

var _ spanconfig.Store = &Store{}

// New instantiates a span config store with the given fallback.
func New(
	fallback roachpb.SpanConfig,
	settings *cluster.Settings,
	boundsReader BoundsReader,
	knobs *spanconfig.TestingKnobs,
) *Store {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	s := &Store{
		settings:     settings,
		fallback:     fallback,
		boundsReader: boundsReader,
		knobs:        knobs,
	}
	s.mu.spanConfigStore = newSpanConfigStore(settings, s.knobs)
	s.mu.systemSpanConfigStore = newSystemSpanConfigStore()
	return s
}

// NeedsSplit is part of the spanconfig.StoreReader interface.
func (s *Store) NeedsSplit(ctx context.Context, start, end roachpb.RKey) (bool, error) {
	splits, err := s.ComputeSplitKey(ctx, start, end)
	if err != nil {
		return false, err
	}

	return len(splits) > 0, nil
}

// ComputeSplitKey is part of the spanconfig.StoreReader interface.
func (s *Store) ComputeSplitKey(
	ctx context.Context, start, end roachpb.RKey,
) (roachpb.RKey, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.mu.spanConfigStore.computeSplitKey(ctx, start, end)
}

// GetSpanConfigForKey is part of the spanconfig.StoreReader interface.
func (s *Store) GetSpanConfigForKey(
	ctx context.Context, key roachpb.RKey,
) (roachpb.SpanConfig, roachpb.Span, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getSpanConfigForKeyRLocked(ctx, key)
}

// getSpanConfigForKeyRLocked is like GetSpanConfigForKey but requires the
// caller to hold the Store read lock.
func (s *Store) getSpanConfigForKeyRLocked(
	ctx context.Context, key roachpb.RKey,
) (roachpb.SpanConfig, roachpb.Span, error) {
	conf, confSpan, found := s.mu.spanConfigStore.getSpanConfigForKey(ctx, key)
	if !found {
		conf = s.getFallbackConfig()
	}
	var err error
	conf, err = s.mu.systemSpanConfigStore.combine(key, conf)
	if err != nil {
		return roachpb.SpanConfig{}, roachpb.Span{}, err
	}

	// No need to perform clamping if SpanConfigBounds are not enabled.
	if !boundsEnabled.Get(&s.settings.SV) {
		return conf, confSpan, nil
	}

	_, tenID, err := keys.DecodeTenantPrefix(roachpb.Key(key))
	if err != nil {
		return roachpb.SpanConfig{}, roachpb.Span{}, err
	}
	if tenID.IsSystem() {
		// SpanConfig bounds do not apply to the system tenant.
		return conf, confSpan, nil
	}

	bounds, found := s.boundsReader.Bounds(tenID)
	if !found {
		return conf, confSpan, nil
	}

	clamped := bounds.Clamp(&conf)

	if clamped {
		log.VInfof(ctx, 3, "span config for tenant clamped to %v", conf)
	}
	return conf, confSpan, nil
}

func (s *Store) getFallbackConfig() roachpb.SpanConfig {
	if conf := FallbackConfigOverride.Get(&s.settings.SV).(*roachpb.SpanConfig); !conf.IsEmpty() {
		return *conf
	}
	if s.knobs != nil && s.knobs.OverrideFallbackConf != nil {
		return s.knobs.OverrideFallbackConf(s.fallback)
	}
	return s.fallback
}

// Apply is part of the spanconfig.StoreWriter interface.
func (s *Store) Apply(
	ctx context.Context, updates ...spanconfig.Update,
) (deleted []spanconfig.Target, added []spanconfig.Record) {

	// Log the potential span config changes.
	for _, update := range updates {
		err := s.maybeLogUpdate(ctx, &update)
		if err != nil {
			log.KvDistribution.Warningf(ctx, "attempted to log a spanconfig update to "+
				"target:%+v, but got the following error:%+v", update.GetTarget(), err)
		}
	}

	deleted, added, err := s.applyInternal(ctx, updates...)
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
	var foundOverlapping bool
	err := s.mu.spanConfigStore.forEachOverlapping(span, func(sp roachpb.Span, conf roachpb.SpanConfig) error {
		foundOverlapping = true
		config, _, err := s.getSpanConfigForKeyRLocked(ctx, roachpb.RKey(sp.Key))
		if err != nil {
			return err
		}
		return f(sp, config)
	})
	if err != nil {
		return err
	}
	// For a span that doesn't overlap with any span configs, we use the fallback
	// config combined with the system span configs that may be applicable to the
	// span.
	//
	// For example, when a table is dropped and all of its data (including the
	// range deletion tombstone installed by the drop) is GC'ed, the associated
	// schema change GC job will delete the table's span config. In this case, we
	// will not find any overlapping span configs for the table's span, but a
	// system span config, such as a cluster wide protection policy, may still be
	// applicable to the replica with the empty table span.
	if !foundOverlapping {
		log.VInfof(ctx, 3, "no overlapping span config found for span %s", span)
		config, _, err := s.getSpanConfigForKeyRLocked(ctx, roachpb.RKey(span.Key))
		if err != nil {
			return err
		}
		return f(span, config)
	}
	return nil
}

// Clone returns a copy of the Store.
func (s *Store) Clone() *Store {
	s.mu.Lock()
	defer s.mu.Unlock()

	clone := New(s.fallback, s.settings, s.boundsReader, s.knobs)
	clone.mu.spanConfigStore = s.mu.spanConfigStore.clone()
	clone.mu.systemSpanConfigStore = s.mu.systemSpanConfigStore.clone()
	return clone
}

func (s *Store) applyInternal(
	ctx context.Context, updates ...spanconfig.Update,
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
	deletedSpans, addedEntries, err := s.mu.spanConfigStore.apply(ctx, spanStoreUpdates...)
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

// maybeLogUpdate logs the SpanConfig changes to the distribution channel. It
// also logs changes to the span boundaries. It doesn't log the changes to the
// SpanConfig for high churn fields like protected timestamps.
func (s *Store) maybeLogUpdate(ctx context.Context, update *spanconfig.Update) error {
	nextSC := update.GetConfig()
	target := update.GetTarget()

	// Return early from SystemTarget updates because they correspond to PTS
	// updates.
	if !target.IsSpanTarget() {
		return nil
	}

	rKey, err := keys.Addr(target.GetSpan().Key)
	if err != nil {
		return err
	}

	var curSpanConfig roachpb.SpanConfig
	var curSpan roachpb.Span
	var found bool
	func() {
		s.mu.RLock()
		defer s.mu.RUnlock()
		curSpanConfig, curSpan, found = s.mu.spanConfigStore.getSpanConfigForKey(ctx, rKey)
	}()

	// Check if the span bounds have changed.
	if found &&
		(!curSpan.Key.Equal(target.GetSpan().Key) ||
			!curSpan.EndKey.Equal(target.GetSpan().EndKey)) {
		log.KvDistribution.Infof(ctx,
			"changing the span boundaries for span:%+v from:[%+v:%+v) to:[%+v:%+v) "+
				"with config: %+v", target, curSpan.Key, curSpan.EndKey, target.GetSpan().Key,
			target.GetSpan().EndKey, nextSC)
	}

	// Log if there is a SpanConfig change in any field other than
	// ProtectedTimestamps to avoid logging PTS updates.
	if log.V(2) || (found && curSpanConfig.HasConfigurationChange(nextSC)) {
		log.KvDistribution.Infof(ctx,
			"changing the spanconfig for span:%+v from:%+v to:%+v",
			target, curSpanConfig, nextSC)
	}
	return nil
}
