// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/partialidx"
)

// indexRejectFlags contains flags designating types of indexes to skip during
// iteration. For example, the iterator would skip over inverted and partial
// indexes given these flags:
//
//   flags := rejectInvertedIndexes|rejectPartialIndexes
//
type indexRejectFlags int8

const (
	// rejectPrimaryIndex excludes the primary index during iteration.
	rejectPrimaryIndex indexRejectFlags = 1 << iota

	// rejectInvertedIndexes excludes any inverted indexes during iteration.
	rejectInvertedIndexes

	// rejectNonInvertedIndexes excludes any non-inverted indexes during
	// iteration.
	rejectNonInvertedIndexes

	// rejectPartialIndexes excludes any partial indexes during iteration.
	rejectPartialIndexes

	// rejectNonPartialIndexes excludes any non-partial indexes during
	// iteration.
	rejectNonPartialIndexes
)

// scanIndexIter is a helper struct that facilitates iteration over the indexes
// of a Scan operator table.
type scanIndexIter struct {
	mem     *memo.Memo
	im      *partialidx.Implicator
	tabMeta *opt.TableMeta

	// scanPrivate is the private of the scan operator to enumerate indexes for.
	scanPrivate *memo.ScanPrivate

	// originalFilters is filters that are applied after the original scan. If
	// there are no filters applied after the original scan, originalFilters
	// should be set to nil. It is used to determine if a partial index can be
	// enumerated and to generate the filters passed to the enumerateIndexFunc
	// (the originalFilters are passed as-is for non-partial indexes).
	originalFilters memo.FiltersExpr

	// rejectFlags is a set of flags that designate which types of indexes to
	// skip during iteration.
	rejectFlags indexRejectFlags
}

// init initializes a new scanIndexIter.
func (it *scanIndexIter) init(
	mem *memo.Memo,
	im *partialidx.Implicator,
	scanPrivate *memo.ScanPrivate,
	originalFilters memo.FiltersExpr,
	rejectFlags indexRejectFlags,
) {
	it.mem = mem
	it.im = im
	it.tabMeta = mem.Metadata().TableMeta(scanPrivate.Table)
	it.scanPrivate = scanPrivate
	it.originalFilters = originalFilters
	it.rejectFlags = rejectFlags
}

// enumerateIndexFunc defines the callback function for the ForEach and
// ForEachStartingAfter functions. It is invoked for each index enumerated.
//
// The function is called with the enumerated index, the filters that must be
// applied after a scan over the index, the index columns, and a boolean that is
// true if the index covers the scanPrivate's columns. If the index is a partial
// index, the filters are the remaining filters after proving partial index
// implication (see partialidx.Implicator). Otherwise, the filters are the
// originalFilters.
type enumerateIndexFunc func(idx cat.Index, filters memo.FiltersExpr, indexCols opt.ColSet, isCovering bool)

// ForEach calls the given callback function for every index of the Scan
// operator's table in the order they appear in the catalog.
//
// The rejectFlags determine types of indexes to skip, if any.
//
// Partial indexes are skipped if their predicate is not implied by the
// originalFilters. If the originalFilters are nil, then only pseudo-partial
// indexes (a partial index with an expression that always evaluates to true)
// are enumerated. If the originalFilters are reduced during partial index
// implication, the remaining filters are passed to the callback f.
//
// If the ForceIndex flag is set on the scanPrivate, then all indexes except the
// forced index are skipped. The index forced by the ForceIndex flag is not
// guaranteed to be iterated on - it will be skipped if it is rejected by the
// rejectFlags, or if it is a partial index with a predicate that is not implied
// by the originalFilters.
func (it *scanIndexIter) ForEach(f enumerateIndexFunc) {
	it.ForEachStartingAfter(cat.PrimaryIndex-1, f)
}

// ForEachStartingAfter calls the given callback function for every index of the
// Scan operator's table with an ordinal greater than ord.
func (it *scanIndexIter) ForEachStartingAfter(ord int, f enumerateIndexFunc) {
	ord++
	for ; ord < it.tabMeta.Table.IndexCount(); ord++ {
		// Skip over the primary index if rejectPrimaryIndex is set.
		if it.hasRejectFlag(rejectPrimaryIndex) && ord == cat.PrimaryIndex {
			continue
		}

		// If we are forcing a specific index, ignore all other indexes.
		if it.scanPrivate.Flags.ForceIndex && ord != it.scanPrivate.Flags.Index {
			continue
		}

		index := it.tabMeta.Table.Index(ord)

		// Skip over inverted indexes if rejectInvertedIndexes is set.
		if it.hasRejectFlag(rejectInvertedIndexes) && index.IsInverted() {
			continue
		}

		// Skip over non-inverted indexes if rejectNonInvertedIndexes is set.
		if it.hasRejectFlag(rejectNonInvertedIndexes) && !index.IsInverted() {
			continue
		}

		pred, isPartialIndex := it.tabMeta.PartialIndexPredicates[ord]

		// Skip over partial indexes if rejectPartialIndexes is set.
		if it.hasRejectFlag(rejectPartialIndexes) && isPartialIndex {
			continue
		}

		// Skip over non-partial indexes if rejectNonPartialIndexes is set.
		if it.hasRejectFlag(rejectNonPartialIndexes) && !isPartialIndex {
			continue
		}

		filters := it.originalFilters

		// If the index is a partial index, check whether the filters imply the
		// predicate.
		if isPartialIndex {
			predFilters := *pred.(*memo.FiltersExpr)

			// If there are no filters, then skip over any partial indexes that
			// are not pseudo-partial indexes.
			if filters == nil && !predFilters.IsTrue() {
				continue
			}

			if filters != nil {
				remainingFilters, ok := it.im.FiltersImplyPredicate(filters, predFilters)
				if !ok {
					// The originalFilters do not imply the predicate, so skip
					// over the partial index.
					continue
				}

				// Set the filters to the remaining filters which may have been
				// reduced.
				filters = remainingFilters
			}
		}

		indexCols := it.tabMeta.IndexColumns(ord)
		isCovering := it.scanPrivate.Cols.SubsetOf(indexCols)

		f(index, filters, indexCols, isCovering)
	}
}

// hasRejectFlag returns true if the given flag is set in the rejectFlags.
func (it *scanIndexIter) hasRejectFlag(flag indexRejectFlags) bool {
	return it.rejectFlags&flag != 0
}
