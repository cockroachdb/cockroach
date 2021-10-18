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
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Store is an in-memory data structure to store and retrieve span configs.
// Internally it makes use of an interval tree to store non-overlapping span
// configs.
type Store struct {
	mu struct {
		syncutil.RWMutex
		tree    interval.Tree
		idAlloc int64
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
	s.mu.tree = interval.NewTree(interval.ExclusiveOverlapper)
	return s
}

// NeedsSplit is part of the spanconfig.StoreReader interface.
func (s *Store) NeedsSplit(ctx context.Context, start, end roachpb.RKey) bool {
	return len(s.ComputeSplitKey(ctx, start, end)) > 0
}

// ComputeSplitKey is part of the spanconfig.StoreReader interface.
func (s *Store) ComputeSplitKey(ctx context.Context, start, end roachpb.RKey) roachpb.RKey {
	sp := roachpb.Span{Key: start.AsRawKey(), EndKey: end.AsRawKey()}

	// We don't want to split within the system config span while we're still
	// also using it to disseminate zone configs.
	//
	// TODO(irfansharif): Once we've fully phased out the system config span, we
	// can get rid of this special handling.
	if keys.SystemConfigSpan.Contains(sp) {
		return nil
	}
	if keys.SystemConfigSpan.ContainsKey(sp.Key) {
		return roachpb.RKey(keys.SystemConfigSpan.EndKey)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	idx := 0
	var splitKey roachpb.RKey = nil
	s.mu.tree.DoMatching(func(i interval.Interface) (done bool) {
		if idx > 0 {
			splitKey = roachpb.RKey(i.(*storeEntry).Span.Key)
			return true // we found our split key, we're done
		}

		idx++
		return false // more
	}, sp.AsRange())

	return splitKey
}

// GetSpanConfigForKey is part of the spanconfig.StoreReader interface.
func (s *Store) GetSpanConfigForKey(
	ctx context.Context, key roachpb.RKey,
) (roachpb.SpanConfig, error) {
	sp := roachpb.Span{Key: key.AsRawKey(), EndKey: key.Next().AsRawKey()}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var conf roachpb.SpanConfig
	found := false
	s.mu.tree.DoMatching(func(i interval.Interface) (done bool) {
		conf = i.(*storeEntry).Config
		found = true
		return true
	}, sp.AsRange())

	if !found {
		if log.ExpensiveLogEnabled(ctx, 1) {
			log.Warningf(ctx, "span config not found for %s", key.String())
		}
		conf = s.fallback
	}
	return conf, nil
}

// Apply is part of the spanconfig.StoreWriter interface.
func (s *Store) Apply(
	ctx context.Context, update spanconfig.Update, dryrun bool,
) (deleted []roachpb.Span, added []roachpb.SpanConfigEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !update.Span.Valid() || len(update.Span.EndKey) == 0 {
		log.Fatalf(ctx, "invalid span")
	}

	entriesToDelete, entriesToAdd := s.accumulateOpsForLocked(update)

	deleted = make([]roachpb.Span, len(entriesToDelete))
	for i := range entriesToDelete {
		entry := &entriesToDelete[i]
		if !dryrun {
			if err := s.mu.tree.Delete(entry, false); err != nil {
				log.Fatalf(ctx, "%v", err)
			}
		}
		deleted[i] = entry.Span
	}

	added = make([]roachpb.SpanConfigEntry, len(entriesToAdd))
	for i := range entriesToAdd {
		entry := &entriesToAdd[i]
		if !dryrun {
			if err := s.mu.tree.Insert(entry, false); err != nil {
				log.Fatalf(ctx, "%v", err)
			}
		}
		added[i] = entry.SpanConfigEntry
	}

	return deleted, added
}

// accumulateOpsForLocked returns the list of store entries that would be
// deleted and added if the given update was to be applied. To apply a given
// update, we want to find all overlapping spans and clear out just the
// intersections. If the update is adding a new span config, we'll also want to
// add it store entry after. We do this by deleting all overlapping spans in
// their entirety and re-adding the non-overlapping portions, if any.
// Pseudo-code:
//
// 	for entry in store.overlapping(update.span):
// 		union, intersection = union(update.span, entry), intersection(update.span, entry)
// 		pre, post = span{union.start_key, intersection.start_key}, span{intersection.end_key, union.end_key}
//
// 		delete entry
// 		if entry.contains(update.span.start_key):
// 			add pre=entry.conf
// 		if entry.contains(update.span.end_key):
// 			add post=entry.conf
//
//   if adding:
//       add update.span=update.conf
//
func (s *Store) accumulateOpsForLocked(update spanconfig.Update) (toDelete, toAdd []storeEntry) {
	for _, overlapping := range s.mu.tree.Get(update.Span.AsRange()) {
		existing := overlapping.(*storeEntry)
		var (
			union = existing.Span.Combine(update.Span)
			inter = existing.Span.Intersect(update.Span)

			pre  = roachpb.Span{Key: union.Key, EndKey: inter.Key}
			post = roachpb.Span{Key: inter.EndKey, EndKey: union.EndKey}
		)

		// Delete the existing span in its entirety. Below we'll re-add the
		// non-intersecting parts of the span.
		toDelete = append(toDelete, *existing)

		if existing.Span.ContainsKey(update.Span.Key) { // existing entry contains the update span's start key
			// ex:     [-----------------)
			//
			// up:         [-------)
			// up:         [-------------)
			// up:         [--------------
			// up:     [-------)
			// up:     [-----------------)
			// up:     [------------------

			// Re-add the non-intersecting span, if any.
			if pre.Valid() {
				toAdd = append(toAdd, s.makeEntryLocked(pre, existing.Config))
			}
		}

		if existing.Span.ContainsKey(update.Span.EndKey) { // existing entry contains the update span's end key
			// ex:     [-----------------)
			//
			// up:     -------------)
			// up:     [------------)
			// up:        [---------)

			// Re-add the non-intersecting span.
			toAdd = append(toAdd, s.makeEntryLocked(post, existing.Config))
		}
	}

	if update.Addition() {
		if len(toDelete) == 1 &&
			toDelete[0].Span.Equal(update.Span) &&
			toDelete[0].Config.Equal(update.Config) {
			// We're deleting exactly what we're going to add, this is a no-op.
			return nil, nil
		}

		// Add the update itself.
		toAdd = append(toAdd, s.makeEntryLocked(update.Span, update.Config))

		// TODO(irfansharif): If we're adding an entry, we could inspect the
		// entries before and after and check whether either of them have the
		// same config. If they do, we could coalesce them into a single span.
		// Given that these boundaries determine where we split ranges, we'd be
		// able to reduce the number of ranges drastically (think adjacent
		// tables/indexes/partitions with the same config). This would be
		// especially significant for secondary tenants, where we'd be able to
		// avoid unconditionally splitting on table boundaries. We'd still want
		// to split on tenant boundaries, so certain preconditions would need to
		// hold. For performance reasons, we'd probably also want to offer
		// a primitive to allow manually splitting on specific table boundaries.
	}

	return toDelete, toAdd
}

func (s *Store) makeEntryLocked(sp roachpb.Span, conf roachpb.SpanConfig) storeEntry {
	s.mu.idAlloc++
	return storeEntry{
		SpanConfigEntry: roachpb.SpanConfigEntry{Span: sp, Config: conf},
		id:              s.mu.idAlloc,
	}
}

// storeEntry is the type used to store and sort values in the span config
// store.
type storeEntry struct {
	roachpb.SpanConfigEntry
	id int64
}

var _ interval.Interface = &storeEntry{}

// Range implements interval.Interface.
func (s *storeEntry) Range() interval.Range {
	return s.Span.AsRange()
}

// ID implements interval.Interface.
func (s *storeEntry) ID() uintptr {
	return uintptr(s.id)
}
