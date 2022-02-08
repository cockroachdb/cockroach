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
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// EnabledSetting is a hidden cluster setting to enable the use of the span
// configs infrastructure in KV. It switches each store in the cluster from
// using the gossip backed system config span to instead using the span configs
// infrastructure. It has no effect if COCKROACH_DISABLE_SPAN_CONFIGS
// is set.
var EnabledSetting = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"spanconfig.store.enabled",
	`use the span config infrastructure in KV instead of the system config span`,
	true,
)

// Store is an in-memory data structure to store, retrieve, and incrementally
// update the span configuration state. Internally, it makes use of an interval
// tree based spanConfigStore to store non-overlapping span configurations that
// target keyspans. It's safe for concurrent use.
//
// TODO(arul): In the future we'll teach this thing about system span
// configurations as well.
type Store struct {
	mu struct {
		syncutil.RWMutex
		spanConfigStore *spanConfigStore
	}

	// TODO(irfansharif): We're using a static fall back span config here, we
	// could instead have this track the host tenant's RANGE DEFAULT config, or
	// go a step further and use the tenant's own RANGE DEFAULT instead if the
	// key is within the tenant's keyspace. We'd have to thread that through the
	// KVAccessor interface by reserving special keys for these default configs.

	// fallback is the span config we'll fall back on in the absence of
	// something more specific.
	fallback roachpb.SpanConfig
}

var _ spanconfig.Store = &Store{}

// New instantiates a span config store with the given fallback.
func New(fallback roachpb.SpanConfig) *Store {
	s := &Store{fallback: fallback}
	s.mu.spanConfigStore = newSpanConfigStore()
	return s
}

// NeedsSplit is part of the spanconfig.StoreReader interface.
func (s *Store) NeedsSplit(ctx context.Context, start, end roachpb.RKey) bool {
	return len(s.ComputeSplitKey(ctx, start, end)) > 0
}

// ComputeSplitKey is part of the spanconfig.StoreReader interface.
func (s *Store) ComputeSplitKey(_ context.Context, start, end roachpb.RKey) roachpb.RKey {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.mu.spanConfigStore.computeSplitKey(start, end)
}

// GetSpanConfigForKey is part of the spanconfig.StoreReader interface.
func (s *Store) GetSpanConfigForKey(
	ctx context.Context, key roachpb.RKey,
) (roachpb.SpanConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	conf, found, err := s.mu.spanConfigStore.getSpanConfigForKey(ctx, key)
	if err != nil {
		return roachpb.SpanConfig{}, err
	}
	if !found {
		return s.fallback, nil
	}
	return conf, nil
}

// Apply is part of the spanconfig.StoreWriter interface.
func (s *Store) Apply(
	ctx context.Context, dryrun bool, updates ...spanconfig.Update,
) (deleted []spanconfig.Target, added []spanconfig.Record) {
	deleted, added, err := s.applyInternal(dryrun, updates...)
	if err != nil {
		log.Fatalf(ctx, "%v", err)
	}
	return deleted, added
}

// Copy returns a copy of the Store.
func (s *Store) Copy(ctx context.Context) *Store {
	s.mu.Lock()
	defer s.mu.Unlock()

	clone := New(s.fallback)
	clone.mu.spanConfigStore = s.mu.spanConfigStore.copy(ctx)
	return clone
}

func (s *Store) applyInternal(
	dryrun bool, updates ...spanconfig.Update,
) (deleted []spanconfig.Target, added []spanconfig.Record, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Accumulate all spanStoreUpdates. We do this because we want to apply
	// a set of updates at once instead of individually, to correctly construct
	// the deleted/added slices.
	spanStoreUpdates := make([]spanconfig.Update, 0, len(updates))
	for _, update := range updates {
		// TODO(arul): We'll hijack system span configurations here.
		if update.Target.IsSpanTarget() {
			spanStoreUpdates = append(spanStoreUpdates, update)
		}
	}
	deletedSpans, addedEntries, err := s.mu.spanConfigStore.apply(dryrun, spanStoreUpdates...)
	if err != nil {
		return nil, nil, err
	}

	for _, sp := range deletedSpans {
		deleted = append(deleted, spanconfig.MakeTargetFromSpan(sp))
	}

	for _, entry := range addedEntries {
		added = append(added, spanconfig.Record{
			Target: spanconfig.MakeTargetFromSpan(entry.span),
			Config: entry.config,
		})
	}
	return deleted, added, nil
}

// Iterate iterates through all the entries in the Store in sorted order.
func (s *Store) Iterate(f func(spanconfig.Record) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.spanConfigStore.forEachOverlapping(
		keys.EverythingSpan,
		func(s spanConfigEntry) error {
			return f(spanconfig.Record{
				Target: spanconfig.MakeTargetFromSpan(s.span),
				Config: s.config,
			})
		})
}
