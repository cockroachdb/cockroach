// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package constraint

import (
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/partition"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// compare compares the key prefix in prefixInfo with the span prefix. The key
// prefix is considered less than the span prefix if it is longer than the
// span prefix, or if it sorts less according to the Datum.Compare interface.
func compare(prefixInfo partition.Prefix, span *Span, ps *partition.PrefixSorter) int {
	prefix := prefixInfo.Prefix
	prefixLength := len(prefix)
	spanPrefixLength := span.Prefix(ps.EvalCtx)
	// Longer prefixes sort before shorter ones.
	// The span prefix is allowed to be longer than the partition prefix and still
	// match.
	if prefixLength > spanPrefixLength {
		return -1
	}

	// Look for an exact match on the shared prefix.
	for k, datum := range prefix {
		compareResult := datum.Compare(ps.EvalCtx, span.StartKey().Value(k))
		if compareResult != 0 {
			return compareResult
		}
	}
	return 0
}

// searchPrefixes searches a sorted slice of Prefixes in ps for a full
// match on all Datums in the Prefix with the given span, and returns the index
// of the match, or -1 if there is no match.
// The slice must be sorted in ascending order, with longer prefixes sorting
// before short prefixes and sorted by prefix values within each group
// of equal-length prefixes.
// Each equal-length prefix group is searched separately because there could be
// more than one possible match for a given span, and we want to match the
// longest-length prefix possible, because that reflects the actual locality
// of the span's owning range.
// If prefixSearchUpperBound is non-negative, only equal-length prefix groups of
// length prefixSearchUpperBound or less will be searched. A negative value for
// prefixSearchUpperBound means the same as passing the max upper bound of
// math.MaxInt32. A zero value for prefixSearchUpperBound means only match on
// the DEFAULT partition, which has a zero-length prefix.
func searchPrefixes(span *Span, ps *partition.PrefixSorter, prefixSearchUpperBound int) int {
	if prefixSearchUpperBound < 0 {
		prefixSearchUpperBound = math.MaxInt32
	}
	spanPrefix := span.Prefix(ps.EvalCtx)
	i := 0
	// Get the first slice in the PrefixSorter
	prefixSlice, startIndex, ok := ps.Slice(i)

	// return 'prefix >= span' result
	matchFunction := func(i int) bool {
		prefix := prefixSlice[i].Prefix
		// For nonzero-length partition prefixes, the span prefix must be at least
		// as long as it in order to match, whereas zero-length default partitions
		// match anything.
		if len(prefix) > spanPrefix {
			return false
		} else if len(prefix) == 0 {
			return true
		}

		for k, datum := range prefix {
			compareResult := datum.Compare(ps.EvalCtx, span.StartKey().Value(k))
			if compareResult != 0 {
				return compareResult > 0
			}
		}
		return true
	}

	for ; ok; prefixSlice, startIndex, ok = ps.Slice(i) {
		i++

		if len(prefixSlice[0].Prefix) > prefixSearchUpperBound {
			continue
		}

		// Binary search for matching entry or insertion point in the prefix slices.
		index := sort.Search(len(prefixSlice), matchFunction)
		if index >= len(prefixSlice) {
			continue
		}
		// Need to requalify for equality because we might have just found an
		// insertion point instead of an actual match.
		if compare(prefixSlice[index], span, ps) == 0 {
			return index + startIndex
		}
	}
	return -1
}

// FindMatch finds the Entry in PrefixSorter which matches the span prefix on a
// prefix subset of its keys, including a zero-length match in the case of the
// DEFAULT partition.
func FindMatch(span *Span, ps *partition.PrefixSorter) (match *partition.Prefix, ok bool) {
	index := searchPrefixes(span, ps, math.MaxInt32 /* prefixSearchUpperBound*/)
	if index == -1 {
		return nil, false
	}
	return &ps.Entry[index], true
}

// FindMatchOnSingleColumn finds the Entry in PrefixSorter with a prefix length
// of 1 or less which matches the span prefix, including a zero-length match in
// the case of the DEFAULT partition.
func FindMatchOnSingleColumn(
	datum tree.Datum, ps *partition.PrefixSorter,
) (match *partition.Prefix, ok bool) {
	sp := &Span{}
	key := Key{firstVal: datum}
	sp.Init(key, IncludeBoundary, key, IncludeBoundary)
	index := searchPrefixes(sp, ps, 1 /* prefixSearchUpperBound */)
	if index == -1 {
		return nil, false
	}
	return &ps.Entry[index], true
}
