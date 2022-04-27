// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// tenantCoalesceAdjacentSetting is a hidden cluster setting that controls
// whether we coalesce adjacent ranges in the tenant keyspace if they have the
// same span config.
var tenantCoalesceAdjacentSetting = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"spanconfig.tenant_coalesce_adjacent.enabled",
	`collapse adjacent ranges with the same span configs`,
	true,
)

// hostCoalesceAdjacentSetting is a hidden cluster setting that controls
// whether we coalesce adjacent ranges in the host tenant keyspace if they have
// the same span config.
var hostCoalesceAdjacentSetting = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"spanconfig.host_coalesce_adjacent.enabled",
	`collapse adjacent ranges with the same span configs`,
	false,
)

// spanConfigStore is an in-memory data structure to store and retrieve
// SpanConfigs associated with a single span. Internally it makes use of an
// interval tree to store non-overlapping span configurations. It isn't safe for
// concurrent use.
type spanConfigStore struct {
	btree       *btree
	treeIDAlloc uint64
	interner    *interner

	settings *cluster.Settings
	knobs    *spanconfig.TestingKnobs
}

// newSpanConfigStore constructs and returns a new spanConfigStore.
func newSpanConfigStore(
	settings *cluster.Settings, knobs *spanconfig.TestingKnobs,
) *spanConfigStore {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	s := &spanConfigStore{
		settings: settings,
		knobs:    knobs,
		btree:    &btree{},
		interner: newInterner(),
	}
	return s
}

// copy returns a copy of the spanConfigStore.
func (s *spanConfigStore) copy(ctx context.Context) *spanConfigStore {
	clone := newSpanConfigStore(s.settings, s.knobs)
	_ = s.forEachOverlapping(keys.EverythingSpan, func(sp roachpb.Span, conf roachpb.SpanConfig) error {
		record, err := spanconfig.MakeRecord(spanconfig.MakeTargetFromSpan(sp), conf)
		if err != nil {
			log.Fatalf(ctx, "%v", err)
		}
		_, _, err = clone.apply(ctx, false /* dryrun */, spanconfig.Update(record))
		if err != nil {
			log.Fatalf(ctx, "%v", err)
		}
		return nil
	})
	return clone
}

// forEachOverlapping iterates through the set of entries that overlap with the
// given span, in sorted order. It does not return an error if the callback
// doesn't.
func (s *spanConfigStore) forEachOverlapping(
	sp roachpb.Span, f func(roachpb.Span, roachpb.SpanConfig) error,
) error {
	// Iterate over all overlapping ranges and invoke the callback with the
	// corresponding span config entries.
	iter, query := s.btree.MakeIter(), makeQueryEntry(sp)
	for iter.FirstOverlap(query); iter.Valid(); iter.NextOverlap(query) {
		entry := iter.Cur().spanConfigPairInterned
		if err := f(entry.span, entry.conf(s.interner)); err != nil {
			if iterutil.Done(err) {
				err = nil
			}
			return err
		}
	}
	return nil
}

// computeSplitKey returns the first key we should split on because of the
// presence a span config given a start and end key pair.
func (s *spanConfigStore) computeSplitKey(
	ctx context.Context, start, end roachpb.RKey,
) roachpb.RKey {
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

	// Generally split keys are going to be the start keys of span config entries.
	// When computing a split key over ['b', 'z'), 'b' is not a valid split key;
	// in the iteration below we'll find all entries overlapping with the given
	// span but skipping over any start keys <= the given start key. In our
	// example this could be ['a', 'x') or ['b', 'y').
	var match spanConfigPairInterned
	{
		iter, query := s.btree.MakeIter(), makeQueryEntry(sp)
		for iter.FirstOverlap(query); iter.Valid(); iter.NextOverlap(query) {
			entry := iter.Cur().spanConfigPairInterned
			if entry.span.Key.Compare(sp.Key) <= 0 {
				continue // more
			}

			match = entry
			break // we found our split key, we're done
		}
		if match.isEmpty() {
			return nil // no overlapping entries == no split key
		}
	}

	if s.knobs.StoreDisableCoalesceAdjacent {
		return roachpb.RKey(match.span.Key)
	}

	rem, matchTenID, err := keys.DecodeTenantPrefix(match.span.Key)
	if err != nil {
		log.Fatalf(ctx, "%v", err)
	}

	if !s.knobs.StoreIgnoreCoalesceAdjacentExceptions {
		if matchTenID.IsSystem() {
			systemTableUpperBound := keys.SystemSQLCodec.TablePrefix(keys.MaxReservedDescID + 1)
			if roachpb.Key(rem).Compare(systemTableUpperBound) < 0 ||
				!hostCoalesceAdjacentSetting.Get(&s.settings.SV) {
				return roachpb.RKey(match.span.Key)
			}
		} else {
			if !tenantCoalesceAdjacentSetting.Get(&s.settings.SV) {
				return roachpb.RKey(match.span.Key)
			}
		}
	}

	// We're looking to coalesce adjacent spans with the same configs as long
	// they don't straddle across tenant boundaries. We'll first peek backwards to
	// find the entry with the same start key as our query span (exactly what we
	// skipped over above). If this entry has a different config or belongs to a
	// different tenant prefix from our original match, our original match key is
	// the split point we're interested in. If such an entry does not exist, then
	// too our original match key is the right split point.
	var firstMatch spanConfigPairInterned
	preSplitKeySp := roachpb.Span{Key: sp.Key, EndKey: match.span.Key}
	{
		iter, query := s.btree.MakeIter(), makeQueryEntry(preSplitKeySp)
		for iter.FirstOverlap(query); iter.Valid(); {
			firstMatch = iter.Cur().spanConfigPairInterned
			break // we're done
		}
		if firstMatch.isEmpty() {
			return roachpb.RKey(match.span.Key)
		}
		_, firstMatchTenID, err := keys.DecodeTenantPrefix(firstMatch.span.Key)
		if err != nil {
			log.Fatalf(ctx, "%v", err)
		}
		if firstMatch.internerID != match.internerID || firstMatchTenID.ToUint64() != matchTenID.ToUint64() {
			return roachpb.RKey(match.span.Key)
		}
	}

	// At least the first two entries with the given span have the same configs
	// and part of the same tenant range. Keep seeking ahead until we find a
	// different config or a different tenant.
	var lastMatch spanConfigPairInterned
	postSplitKeySp := roachpb.Span{Key: match.span.EndKey, EndKey: sp.EndKey}
	{
		iter, query := s.btree.MakeIter(), makeQueryEntry(postSplitKeySp)
		for iter.FirstOverlap(query); iter.Valid(); iter.NextOverlap(query) {
			nextEntry := iter.Cur().spanConfigPairInterned
			_, entryTenID, err := keys.DecodeTenantPrefix(nextEntry.span.Key)
			if err != nil {
				log.Fatalf(ctx, "%v", err)
			}
			if nextEntry.internerID != match.internerID || entryTenID.ToUint64() != matchTenID.ToUint64() {
				lastMatch = nextEntry
				break // we're done
			}
		}
		if !lastMatch.isEmpty() {
			return roachpb.RKey(lastMatch.span.Key)
		}
	}

	// All entries within the given span have the same config and part of the same
	// tenant. There are no split points here.
	return nil
}

// getSpanConfigForKey returns the span config corresponding to the supplied
// key.
func (s *spanConfigStore) getSpanConfigForKey(
	ctx context.Context, key roachpb.RKey,
) (conf roachpb.SpanConfig, found bool) {
	sp := roachpb.Span{Key: key.AsRawKey(), EndKey: key.Next().AsRawKey()}
	iter, query := s.btree.MakeIter(), makeQueryEntry(sp)
	for iter.FirstOverlap(query); iter.Valid(); {
		conf, found = iter.Cur().conf(s.interner), true
		break
	}
	if !found && log.ExpensiveLogEnabled(ctx, 1) {
		log.Warningf(ctx, "span config not found for %s", key.String())
	}
	return conf, found
}

// apply takes an incremental set of updates and returns the spans/span<->config
// entries deleted/added as a result of applying them. It also updates its state
// by applying them if dryrun is false.
func (s *spanConfigStore) apply(
	ctx context.Context, dryrun bool, updates ...spanconfig.Update,
) (deleted []roachpb.Span, added []spanConfigStoreEntry, err error) {
	if err := validateApplyArgs(updates...); err != nil {
		return nil, nil, err
	}

	sorted := make([]spanconfig.Update, len(updates))
	copy(sorted, updates)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].GetTarget().Less(sorted[j].GetTarget())
	})
	updates = sorted // re-use the same variable

	entriesToDelete, entriesToAdd, err := s.accumulateOpsFor(ctx, dryrun, updates)
	if err != nil {
		return nil, nil, err
	}

	deleted = make([]roachpb.Span, len(entriesToDelete))
	for i := range entriesToDelete {
		entry := &entriesToDelete[i]
		if !dryrun {
			s.btree.Delete(entry)
			s.interner.remove(ctx, entry.internerID)
		}
		deleted[i] = entry.span
	}

	added = make([]spanConfigStoreEntry, len(entriesToAdd))
	for i := range entriesToAdd {
		entry := &entriesToAdd[i]
		if !dryrun {
			s.btree.Set(entry)
		}
		added[i] = *entry
	}

	return deleted, added, nil
}

// accumulateOpsFor returns the list of store entries that would be
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
//          if entry.overlap(carried-over):
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
func (s *spanConfigStore) accumulateOpsFor(
	ctx context.Context, dryrun bool, updates []spanconfig.Update,
) (toDelete, toAdd []spanConfigStoreEntry, _ error) {
	var carryOver spanConfigPair
	for _, update := range updates {
		var carriedOver spanConfigPair
		carriedOver, carryOver = carryOver, spanConfigPair{}
		if update.GetTarget().GetSpan().Overlaps(carriedOver.span) {
			gapBetweenUpdates := roachpb.Span{
				Key:    carriedOver.span.Key,
				EndKey: update.GetTarget().GetSpan().Key}
			if gapBetweenUpdates.Valid() {
				toAdd = append(toAdd, s.makeEntry(ctx, dryrun, gapBetweenUpdates, carriedOver.config))
			}

			carryOverSpanAfterUpdate := roachpb.Span{
				Key:    update.GetTarget().GetSpan().EndKey,
				EndKey: carriedOver.span.EndKey}
			if carryOverSpanAfterUpdate.Valid() {
				carryOver = spanConfigPair{
					span:   carryOverSpanAfterUpdate,
					config: carriedOver.config,
				}
			}
		} else if !carriedOver.isEmpty() {
			toAdd = append(toAdd, s.makeEntry(ctx, dryrun, carriedOver.span, carriedOver.config))
		}

		skipAddingSelf := false
		iter, query := s.btree.MakeIter(), makeQueryEntry(update.GetTarget().GetSpan())
		for iter.FirstOverlap(query); iter.Valid(); iter.NextOverlap(query) {
			existing := iter.Cur()
			if existing.span.Overlaps(carriedOver.span) {
				continue // we've already processed this entry above.
			}

			var (
				union = existing.span.Combine(update.GetTarget().GetSpan())
				inter = existing.span.Intersect(update.GetTarget().GetSpan())

				pre  = roachpb.Span{Key: union.Key, EndKey: inter.Key}
				post = roachpb.Span{Key: inter.EndKey, EndKey: union.EndKey}
			)

			existingConf := existing.conf(s.interner)
			if update.Addition() {
				if existing.span.Equal(update.GetTarget().GetSpan()) && existingConf.Equal(update.GetConfig()) {
					skipAddingSelf = true
					break // no-op; peep-hole optimization
				}
			}

			// Delete the existing span in its entirety. Below we'll re-add the
			// non-intersecting parts of the span.
			toDelete = append(toDelete, *existing)
			// existing entry contains the update span's start key
			if existing.span.ContainsKey(update.GetTarget().GetSpan().Key) {
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
					toAdd = append(toAdd, s.makeEntry(ctx, dryrun, pre, existingConf))
				}
			}

			if existing.span.ContainsKey(update.GetTarget().GetSpan().EndKey) { // existing entry contains the update span's end key
				// ex:     [-----------------)
				//
				// up:     -------------)
				// up:     [------------)
				// up:        [---------)

				// Carry over the non-intersecting span.
				carryOver = spanConfigPair{
					span:   post,
					config: existingConf,
				}
			}
		}

		if update.Addition() && !skipAddingSelf {
			// Add the update itself.
			toAdd = append(toAdd, s.makeEntry(ctx, dryrun, update.GetTarget().GetSpan(), update.GetConfig()))

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

	if !carryOver.isEmpty() {
		toAdd = append(toAdd, s.makeEntry(ctx, dryrun, carryOver.span, carryOver.config))
	}
	return toDelete, toAdd, nil
}

// validateApplyArgs validates the supplied updates can be applied to the
// spanConfigStore. In particular, updates are expected to correspond to target
// spans, those spans be valid, and non-overlapping.
func validateApplyArgs(updates ...spanconfig.Update) error {
	for i := range updates {
		if !updates[i].GetTarget().IsSpanTarget() {
			return errors.New("expected update to target a span")
		}

		sp := updates[i].GetTarget().GetSpan()
		if !sp.Valid() || len(sp.EndKey) == 0 {
			return errors.New("invalid span")
		}
	}

	sorted := make([]spanconfig.Update, len(updates))
	copy(sorted, updates)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].GetTarget().GetSpan().Key.Compare(sorted[j].GetTarget().GetSpan().Key) < 0
	})
	updates = sorted // re-use the same variable

	for i := range updates {
		if i == 0 {
			continue
		}
		if updates[i].GetTarget().GetSpan().Overlaps(updates[i-1].GetTarget().GetSpan()) {
			return errors.Newf(
				"found overlapping updates %s and %s",
				updates[i-1].GetTarget().GetSpan(),
				updates[i].GetTarget().GetSpan(),
			)
		}
	}
	return nil
}

// spanConfigPair represents a span <->config pair.
type spanConfigPair struct {
	span   roachpb.Span
	config roachpb.SpanConfig
}

func (s *spanConfigPair) isEmpty() bool {
	return s.span.Equal(roachpb.Span{}) && s.config.IsEmpty()
}

// spanConfigPairInterned represents a span <->config pair, but unlike
// spanConfigPair, doesn't embed the span config itself. It instead holds onto
// an interner ID which can be used to retrieve the corresponding config.
type spanConfigPairInterned struct {
	span roachpb.Span
	internerID
}

func (s *spanConfigPairInterned) isEmpty() bool {
	return s.span.Equal(roachpb.Span{}) && s.internerID == internerID(0)
}

func (s *spanConfigPairInterned) conf(interner *interner) roachpb.SpanConfig {
	conf, _ := interner.get(s.internerID)
	return conf
}

//go:generate ../../util/interval/generic/gen.sh *spanConfigStoreEntry spanconfigstore

type spanConfigStoreEntry struct {
	spanConfigPairInterned
	id uint64
}

func (s *spanConfigStore) makeEntry(
	ctx context.Context, dryrun bool, sp roachpb.Span, conf roachpb.SpanConfig,
) spanConfigStoreEntry {
	if !dryrun {
		s.treeIDAlloc++
	}
	var internID internerID
	if !dryrun || s.knobs.StoreInternConfigsInDryRuns {
		internID = s.interner.add(ctx, conf)
	}
	return spanConfigStoreEntry{
		spanConfigPairInterned: spanConfigPairInterned{
			span:       sp,
			internerID: internID,
		},
		id: s.treeIDAlloc,
	}
}

func makeQueryEntry(s roachpb.Span) *spanConfigStoreEntry {
	var entry spanConfigStoreEntry
	entry.SetKey(s.Key)
	entry.SetEndKey(s.EndKey)
	return &entry
}

// Methods required by util/interval/generic type contract.

func (s *spanConfigStoreEntry) ID() uint64                 { return s.id }
func (s *spanConfigStoreEntry) Key() []byte                { return s.span.Key }
func (s *spanConfigStoreEntry) EndKey() []byte             { return s.span.EndKey }
func (s *spanConfigStoreEntry) String() string             { return s.span.String() }
func (s *spanConfigStoreEntry) New() *spanConfigStoreEntry { return new(spanConfigStoreEntry) }
func (s *spanConfigStoreEntry) SetID(id uint64)            { s.id = id }
func (s *spanConfigStoreEntry) SetKey(k []byte)            { s.span.Key = k }
func (s *spanConfigStoreEntry) SetEndKey(k []byte)         { s.span.EndKey = k }
