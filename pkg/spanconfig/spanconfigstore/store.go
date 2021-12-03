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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// EnabledSetting is a hidden cluster setting to enable the use of the span
// configs infrastructure in KV. It switches each store in the cluster from
// using the gossip backed system config span to instead using the span configs
// infrastructure. It has no effect unless COCKROACH_EXPERIMENTAL_SPAN_CONFIGS
// is set.
var EnabledSetting = settings.RegisterBoolSetting(
	"spanconfig.experimental_store.enabled",
	`use the span config infrastructure in KV instead of the system config span`,
	false,
).WithSystemOnly()

// Store is an in-memory data structure to store and retrieve span configs.
// Internally it makes use of an interval tree to store non-overlapping span
// configs. It's safe for concurrent use.
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
func (s *Store) ComputeSplitKey(_ context.Context, start, end roachpb.RKey) roachpb.RKey {
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
	ctx context.Context, dryrun bool, updates ...spanconfig.Update,
) (deleted []roachpb.Span, added []roachpb.SpanConfigEntry) {
	deleted, added, err := s.applyInternal(dryrun, updates...)
	if err != nil {
		log.Fatalf(ctx, "%v", err)
	}
	return deleted, added
}

// Copy returns a copy of the Store.
func (s *Store) Copy(ctx context.Context) *Store {
	clone := New(s.fallback)
	_ = s.ForEachOverlapping(ctx, keys.EverythingSpan, func(entry roachpb.SpanConfigEntry) error {
		clone.Apply(ctx, false /* dryrun */, spanconfig.Update{
			Span:   entry.Span,
			Config: entry.Config,
		})
		return nil
	})
	return clone
}

// ForEachOverlapping iterates through the set of entries that overlap with the
// given span, in sorted order. It does not return an error if the callback
// doesn't.
func (s *Store) ForEachOverlapping(
	_ context.Context, sp roachpb.Span, f func(roachpb.SpanConfigEntry) error,
) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Iterate over all overlapping ranges and invoke the callback with the
	// corresponding span config entries.
	for _, overlapping := range s.mu.tree.Get(sp.AsRange()) {
		entry := overlapping.(*storeEntry).SpanConfigEntry
		if err := f(entry); err != nil {
			if iterutil.Done(err) {
				err = nil
			}
			return err
		}
	}
	return nil
}

func (s *Store) applyInternal(
	dryrun bool, updates ...spanconfig.Update,
) (deleted []roachpb.Span, added []roachpb.SpanConfigEntry, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := range updates {
		if !updates[i].Span.Valid() || len(updates[i].Span.EndKey) == 0 {
			return nil, nil, errors.New("invalid span")
		}
	}

	sorted := make([]spanconfig.Update, len(updates))
	copy(sorted, updates)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Span.Key.Compare(sorted[j].Span.Key) < 0
	})
	updates = sorted // re-use the same variable

	for i := range updates {
		if i == 0 {
			continue
		}
		if updates[i].Span.Overlaps(updates[i-1].Span) {
			return nil, nil, errors.Newf("found overlapping updates %s and %s", updates[i-1].Span, updates[i].Span)
		}
	}

	entriesToDelete, entriesToAdd := s.accumulateOpsForLocked(updates)

	deleted = make([]roachpb.Span, len(entriesToDelete))
	for i := range entriesToDelete {
		entry := &entriesToDelete[i]
		if !dryrun {
			if err := s.mu.tree.Delete(entry, false); err != nil {
				return nil, nil, err
			}
		}
		deleted[i] = entry.Span
	}

	added = make([]roachpb.SpanConfigEntry, len(entriesToAdd))
	for i := range entriesToAdd {
		entry := &entriesToAdd[i]
		if !dryrun {
			if err := s.mu.tree.Insert(entry, false); err != nil {
				return nil, nil, err
			}
		}
		added[i] = entry.SpanConfigEntry
	}

	return deleted, added, nil
}

// accumulateOpsForLocked returns the list of store entries that would be
// deleted and added if the given set of updates were to be applied.
//
// To apply a single update, we want to find all overlapping spans and clear out
// just the intersections. If the update is adding a new span config, we'll also
// want to add the corresponding store entry after. We do this by deleting all
// overlapping spans in their entirety and re-adding the non-overlapping
// segments. Pseudo-code:
//
//   for entry in store.overlapping(update.span):
//       union, intersection = union(update.span, entry), intersection(update.span, entry)
//       pre  = span{union.start_key, intersection.start_key}
//       post = span{intersection.end_key, union.end_key}
//
//       delete {span=entry.span, conf=entry.conf}
//       if entry.contains(update.span.start_key):
//           # First entry overlapping with update.
//           add {span=pre, conf=entry.conf} if non-empty
//       if entry.contains(update.span.end_key):
//           # Last entry overlapping with update.
//           add {span=post, conf=entry.conf} if non-empty
//
//   if adding:
//       add {span=update.span, conf=update.conf} # add ourselves
//
// When extending to a set of updates, things are more involved (but only
// slightly!). Let's assume that the updates are non-overlapping and sorted
// by start key. As before, we want to delete overlapping entries in their
// entirety and re-add the non-overlapping segments. With multiple updates, it's
// possible that a segment being re-added will overlap another update. If
// processing one update at a time in sorted order, we want to only re-add the
// gap between the consecutive updates.
//
//   keyspace         a  b  c  d  e  f  g  h  i  j
//   existing state      [--------X--------)
//   updates          [--A--)           [--B--)
//
// When processing [a,c):A, after deleting [b,h):X, it would be incorrect to
// re-add [c,h):X since we're also looking to apply [g,i):B. Instead of
// re-adding the trailing segment right away, we carry it forward and process it
// when iterating over the second, possibly overlapping update. In our example,
// when iterating over [g,i):B we can subtract the overlap from [c,h):X and only
// re-add [c,g):X.
//
// It's also possible for the segment to extend past the second update. In the
// example below, when processing [d,f):B and having [b,h):X carried over, we
// want to re-add [c,d):X and carry forward [f,h):X to the update after (i.e.
// [g,i):C)).
//
//   keyspace         a  b  c  d  e  f  g  h  i  j
//   existing state      [--------X--------)
//   updates          [--A--)  [--B--)  [--C--)
//
// One final note: we're iterating through the updates without actually applying
// any mutations. Going back to our first example, when processing [g,i):B,
// retrieving the set of overlapping spans would (again) retrieve [b,h):X -- an
// entry we've already encountered when processing [a,c):A. Re-adding
// non-overlapping segments naively would re-add [b,g):X -- an entry that
// overlaps with our last update [a,c):A. When retrieving overlapping entries,
// we need to exclude any that overlap with the segment that was carried over.
// Pseudo-code:
//
//   carry-over = <empty>
//   for update in updates:
//       carried-over, carry-over = carry-over, <empty>
//       if update.overlap(carried-over):
//           # Fill in the gap between consecutive updates.
//           add {span=span{carried-over.start_key, update.start_key}, conf=carried-over.conf}
//           # Consider the trailing span after update; carry it forward if non-empty.
//           carry-over = {span=span{update.end_key, carried-over.end_key}, conf=carried-over.conf}
//       else:
//           add {span=carried-over.span, conf=carried-over.conf} if non-empty
//
//       for entry in store.overlapping(update.span):
//          if entry.overlap(processed):
//               continue # already processed
//
//           union, intersection = union(update.span, entry), intersection(update.span, entry)
//           pre  = span{union.start_key, intersection.start_key}
//           post = span{intersection.end_key, union.end_key}
//
//           delete {span=entry.span, conf=entry.conf}
//           if entry.contains(update.span.start_key):
//               # First entry overlapping with update.
//               add {span=pre, conf=entry.conf} if non-empty
//           if entry.contains(update.span.end_key):
//               # Last entry overlapping with update.
//               carry-over = {span=post, conf=entry.conf}
//
//        if adding:
//           add {span=update.span, conf=update.conf} # add ourselves
//
//   add {span=carry-over.span, conf=carry-over.conf} if non-empty
//
func (s *Store) accumulateOpsForLocked(updates []spanconfig.Update) (toDelete, toAdd []storeEntry) {
	var carryOver roachpb.SpanConfigEntry
	for _, update := range updates {
		var carriedOver roachpb.SpanConfigEntry
		carriedOver, carryOver = carryOver, roachpb.SpanConfigEntry{}
		if update.Span.Overlaps(carriedOver.Span) {
			gapBetweenUpdates := roachpb.Span{Key: carriedOver.Span.Key, EndKey: update.Span.Key}
			if gapBetweenUpdates.Valid() {
				toAdd = append(toAdd, s.makeEntryLocked(gapBetweenUpdates, carriedOver.Config))
			}

			carryOverSpanAfterUpdate := roachpb.Span{Key: update.Span.EndKey, EndKey: carriedOver.Span.EndKey}
			if carryOverSpanAfterUpdate.Valid() {
				carryOver = roachpb.SpanConfigEntry{
					Span:   carryOverSpanAfterUpdate,
					Config: carriedOver.Config,
				}
			}
		} else if !carriedOver.Empty() {
			toAdd = append(toAdd, s.makeEntryLocked(carriedOver.Span, carriedOver.Config))
		}

		skipAddingSelf := false
		for _, overlapping := range s.mu.tree.Get(update.Span.AsRange()) {
			existing := overlapping.(*storeEntry)
			if existing.Span.Overlaps(carriedOver.Span) {
				continue // we've already processed this entry above.
			}

			var (
				union = existing.Span.Combine(update.Span)
				inter = existing.Span.Intersect(update.Span)

				pre  = roachpb.Span{Key: union.Key, EndKey: inter.Key}
				post = roachpb.Span{Key: inter.EndKey, EndKey: union.EndKey}
			)

			if update.Addition() {
				if existing.Span.Equal(update.Span) && existing.Config.Equal(update.Config) {
					skipAddingSelf = true
					break // no-op; peep-hole optimization
				}
			}

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

				// Carry over the non-intersecting span.
				carryOver = roachpb.SpanConfigEntry{
					Span:   post,
					Config: existing.Config,
				}
			}
		}

		if update.Addition() && !skipAddingSelf {
			// Add the update itself.
			toAdd = append(toAdd, s.makeEntryLocked(update.Span, update.Config))

			// TODO(irfansharif): If we're adding an entry, we could inspect the
			// entries before and after and check whether either of them have
			// the same config. If they do, we could coalesce them into a single
			// span. Given that these boundaries determine where we split
			// ranges, we'd be able to reduce the number of ranges drastically
			// (think adjacent tables/indexes/partitions with the same config).
			// This would be especially significant for secondary tenants, where
			// we'd be able to avoid unconditionally splitting on table
			// boundaries. We'd still want to split on tenant boundaries, so
			// certain preconditions would need to hold. For performance
			// reasons, we'd probably also want to offer a primitive to allow
			// manually splitting on specific table boundaries.
		}
	}

	if !carryOver.Empty() {
		toAdd = append(toAdd, s.makeEntryLocked(carryOver.Span, carryOver.Config))
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
