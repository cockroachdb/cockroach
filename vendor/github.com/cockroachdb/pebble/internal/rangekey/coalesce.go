// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangekey

import (
	"bytes"
	"sort"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// UserIteratorConfig holds state for constructing the range key iterator stack
// for user iteration.
type UserIteratorConfig struct {
	snapshot   uint64
	comparer   *base.Comparer
	miter      keyspan.MergingIter
	biter      keyspan.BoundedIter
	diter      keyspan.DefragmentingIter
	liters     [manifest.NumLevels]keyspan.LevelIter
	litersUsed int
	sortBuf    keysBySuffix
}

// Init initializes the range key iterator stack for user iteration. The
// resulting fragment iterator applies range key semantics, defragments spans
// according to their user-observable state and removes all Keys other than
// RangeKeySets describing the current state of range keys. The resulting spans
// contain Keys sorted by Suffix.
//
// The snapshot sequence number parameter determines which keys are visible. Any
// keys not visible at the provided snapshot are ignored.
func (ui *UserIteratorConfig) Init(
	comparer *base.Comparer,
	snapshot uint64,
	lower, upper []byte,
	hasPrefix *bool,
	prefix *[]byte,
	iters ...keyspan.FragmentIterator,
) keyspan.FragmentIterator {
	ui.snapshot = snapshot
	ui.comparer = comparer
	ui.miter.Init(comparer.Compare, ui, iters...)
	ui.biter.Init(comparer.Compare, comparer.Split, &ui.miter, lower, upper, hasPrefix, prefix)
	ui.diter.Init(comparer, &ui.biter, ui, keyspan.StaticDefragmentReducer)
	ui.litersUsed = 0
	return &ui.diter
}

// AddLevel adds a new level to the bottom of the iterator stack. AddLevel
// must be called after Init and before any other method on the iterator.
func (ui *UserIteratorConfig) AddLevel(iter keyspan.FragmentIterator) {
	ui.miter.AddLevel(iter)
}

// NewLevelIter returns a pointer to a newly allocated or reused
// keyspan.LevelIter. The caller is responsible for calling Init() on this
// instance.
func (ui *UserIteratorConfig) NewLevelIter() *keyspan.LevelIter {
	if ui.litersUsed >= len(ui.liters) {
		return &keyspan.LevelIter{}
	}
	ui.litersUsed++
	return &ui.liters[ui.litersUsed-1]
}

// SetBounds propagates bounds to the iterator stack. The fragment iterator
// interface ordinarily doesn't enforce bounds, so this is exposed as an
// explicit method on the user iterator config.
func (ui *UserIteratorConfig) SetBounds(lower, upper []byte) {
	ui.biter.SetBounds(lower, upper)
}

// Transform implements the keyspan.Transformer interface for use with a
// keyspan.MergingIter. It transforms spans by resolving range keys at the
// provided snapshot sequence number. Shadowing of keys is resolved (eg, removal
// of unset keys, removal of keys overwritten by a set at the same suffix, etc)
// and then non-RangeKeySet keys are removed. The resulting transformed spans
// only contain RangeKeySets describing the state visible at the provided
// sequence number, and hold their Keys sorted by Suffix.
func (ui *UserIteratorConfig) Transform(cmp base.Compare, s keyspan.Span, dst *keyspan.Span) error {
	// Apply shadowing of keys.
	dst.Start = s.Start
	dst.End = s.End
	ui.sortBuf = keysBySuffix{
		cmp:  cmp,
		keys: dst.Keys[:0],
	}
	if err := coalesce(&ui.sortBuf, s.Visible(ui.snapshot).Keys, &dst.Keys); err != nil {
		return err
	}
	// During user iteration over range keys, unsets and deletes don't
	// matter. Remove them. This step helps logical defragmentation during
	// iteration.
	keys := dst.Keys
	dst.Keys = dst.Keys[:0]
	for i := range keys {
		switch keys[i].Kind() {
		case base.InternalKeyKindRangeKeySet:
			if invariants.Enabled && len(dst.Keys) > 0 && cmp(dst.Keys[len(dst.Keys)-1].Suffix, keys[i].Suffix) > 0 {
				panic("pebble: keys unexpectedly not in ascending suffix order")
			}
			dst.Keys = append(dst.Keys, keys[i])
		case base.InternalKeyKindRangeKeyUnset:
			if invariants.Enabled && len(dst.Keys) > 0 && cmp(dst.Keys[len(dst.Keys)-1].Suffix, keys[i].Suffix) > 0 {
				panic("pebble: keys unexpectedly not in ascending suffix order")
			}
			// Skip.
			continue
		case base.InternalKeyKindRangeKeyDelete:
			// Skip.
			continue
		default:
			return base.CorruptionErrorf("pebble: unrecognized range key kind %s", keys[i].Kind())
		}
	}
	// coalesce results in dst.Keys being sorted by Suffix.
	dst.KeysOrder = keyspan.BySuffixAsc
	return nil
}

// ShouldDefragment implements the DefragmentMethod interface and configures a
// DefragmentingIter to defragment spans of range keys if their user-visible
// state is identical. This defragmenting method assumes the provided spans have
// already been transformed through (UserIterationConfig).Transform, so all
// RangeKeySets are user-visible sets and are already in Suffix order. This
// defragmenter checks for equality between set suffixes and values (ignoring
// sequence numbers). It's intended for use during user iteration, when the
// wrapped keyspan iterator is merging spans across all levels of the LSM.
func (ui *UserIteratorConfig) ShouldDefragment(equal base.Equal, a, b *keyspan.Span) bool {
	// This implementation must only be used on spans that have transformed by
	// ui.Transform. The transform applies shadowing, removes all keys besides
	// the resulting Sets and sorts the keys by suffix. Since shadowing has been
	// applied, each Set must set a unique suffix. If the two spans are
	// equivalent, they must have the same number of range key sets.
	if len(a.Keys) != len(b.Keys) || len(a.Keys) == 0 {
		return false
	}
	if a.KeysOrder != keyspan.BySuffixAsc || b.KeysOrder != keyspan.BySuffixAsc {
		panic("pebble: range key span's keys unexpectedly not in ascending suffix order")
	}

	ret := true
	for i := range a.Keys {
		if invariants.Enabled {
			if a.Keys[i].Kind() != base.InternalKeyKindRangeKeySet ||
				b.Keys[i].Kind() != base.InternalKeyKindRangeKeySet {
				panic("pebble: unexpected non-RangeKeySet during defragmentation")
			}
			if i > 0 && (ui.comparer.Compare(a.Keys[i].Suffix, a.Keys[i-1].Suffix) < 0 ||
				ui.comparer.Compare(b.Keys[i].Suffix, b.Keys[i-1].Suffix) < 0) {
				panic("pebble: range keys not ordered by suffix during defragmentation")
			}
		}
		if !equal(a.Keys[i].Suffix, b.Keys[i].Suffix) {
			ret = false
			break
		}
		if !bytes.Equal(a.Keys[i].Value, b.Keys[i].Value) {
			ret = false
			break
		}
	}
	return ret
}

// Coalesce imposes range key semantics and coalesces range keys with the same
// bounds. Coalesce drops any keys shadowed by more recent sets, unsets or
// deletes. Coalesce modifies the provided span's Keys slice, reslicing the
// slice to remove dropped keys.
//
// Coalescence has subtle behavior with respect to sequence numbers. Coalesce
// depends on a keyspan.Span's Keys being sorted in sequence number descending
// order. The first key has the largest sequence number. The returned coalesced
// span includes only the largest sequence number. All other sequence numbers
// are forgotten. When a compaction constructs output range keys from a
// coalesced span, it produces at most one RANGEKEYSET, one RANGEKEYUNSET and
// one RANGEKEYDEL. Each one of these keys adopt the largest sequence number.
//
// This has the potentially surprising effect of 'promoting' a key to a higher
// sequence number. This is okay, because:
//   - There are no other overlapping keys within the coalesced span of
//     sequence numbers (otherwise they would be in the compaction, due to
//     the LSM invariant).
//   - Range key sequence numbers are never compared to point key sequence
//     numbers. Range keys and point keys have parallel existences.
//   - Compactions only coalesce within snapshot stripes.
//
// Additionally, internal range keys at the same sequence number have subtle
// mechanics:
//   * RANGEKEYSETs shadow RANGEKEYUNSETs of the same suffix.
//   * RANGEKEYDELs only apply to keys at lower sequence numbers.
// This is required for ingestion. Ingested sstables are assigned a single
// sequence number for the file, at which all of the file's keys are visible.
// The RANGEKEYSET, RANGEKEYUNSET and RANGEKEYDEL key kinds are ordered such
// that among keys with equal sequence numbers (thus ordered by their kinds) the
// keys do not affect one another. Ingested sstables are expected to be
// consistent with respect to the set/unset suffixes: A given suffix should be
// set or unset but not both.
//
// The resulting dst Keys slice is sorted by Trailer.
func Coalesce(cmp base.Compare, keys []keyspan.Key, dst *[]keyspan.Key) error {
	// TODO(jackson): Currently, Coalesce doesn't actually perform the sequence
	// number promotion described in the comment above.
	keysBySuffix := keysBySuffix{
		cmp:  cmp,
		keys: (*dst)[:0],
	}
	if err := coalesce(&keysBySuffix, keys, dst); err != nil {
		return err
	}
	// coalesce left the keys in *dst sorted by suffix. Re-sort them by trailer.
	keyspan.SortKeysByTrailer(dst)
	return nil
}

func coalesce(keysBySuffix *keysBySuffix, keys []keyspan.Key, dst *[]keyspan.Key) error {
	var deleted bool
	for i := 0; i < len(keys) && !deleted; i++ {
		k := keys[i]
		if invariants.Enabled && i > 0 && k.Trailer > keys[i-1].Trailer {
			panic("pebble: invariant violation: span keys unordered")
		}

		// NB: Within a given sequence number, keys are ordered as:
		//   RangeKeySet > RangeKeyUnset > RangeKeyDelete
		// This is significant, because this ensures that none of the range keys
		// sharing a sequence number shadow each other.
		switch k.Kind() {
		case base.InternalKeyKindRangeKeySet:
			n := len(keysBySuffix.keys)

			if keysBySuffix.get(n, k.Suffix) < n {
				// This suffix is already set or unset at a higher sequence
				// number. Skip.
				continue
			}
			keysBySuffix.keys = append(keysBySuffix.keys, k)
			sort.Sort(keysBySuffix)
		case base.InternalKeyKindRangeKeyUnset:
			n := len(keysBySuffix.keys)

			if keysBySuffix.get(n, k.Suffix) < n {
				// This suffix is already set or unset at a higher sequence
				// number. Skip.
				continue
			}
			keysBySuffix.keys = append(keysBySuffix.keys, k)
			sort.Sort(keysBySuffix)
		case base.InternalKeyKindRangeKeyDelete:
			// All remaining range keys in this span have been deleted by this
			// RangeKeyDelete. There's no need to continue looping, because all
			// the remaining keys are shadowed by this one. The for loop
			// condition will terminate when it sees the last key is a
			// range key deletion.
			keysBySuffix.keys = append(keysBySuffix.keys, k)
			deleted = true
		default:
			return base.CorruptionErrorf("pebble: unexpected range key kind %s", k.Kind())
		}
	}

	// Update the span with the (potentially reduced) keys slice.
	// NB: We don't re-sort by Trailer. The exported Coalesce function however
	// will.
	*dst = keysBySuffix.keys
	return nil
}

type keysBySuffix struct {
	cmp  base.Compare
	keys []keyspan.Key
}

// get searches for suffix among the first n keys in keys. If the suffix is
// found, it returns the index of the item with the suffix. If the suffix is not
// found, it returns n.
func (s *keysBySuffix) get(n int, suffix []byte) (i int) {
	// Binary search for the suffix to see if there's an existing key with the
	// suffix. Only binary search among the first n items. get is called while
	// appending new keys with suffixes that may sort before existing keys.
	// The n parameter indicates what portion of the keys slice is sorted and
	// may contain relevant keys.

	i = sort.Search(n, func(i int) bool {
		return s.cmp(s.keys[i].Suffix, suffix) >= 0
	})
	if i < n && s.cmp(s.keys[i].Suffix, suffix) == 0 {
		return i
	}
	return n
}

func (s *keysBySuffix) Len() int           { return len(s.keys) }
func (s *keysBySuffix) Less(i, j int) bool { return s.cmp(s.keys[i].Suffix, s.keys[j].Suffix) < 0 }
func (s *keysBySuffix) Swap(i, j int)      { s.keys[i], s.keys[j] = s.keys[j], s.keys[i] }
