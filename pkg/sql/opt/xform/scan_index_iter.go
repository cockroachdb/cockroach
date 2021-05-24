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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/partialidx"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
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
	evalCtx *tree.EvalContext
	f       *norm.Factory
	im      *partialidx.Implicator
	tabMeta *opt.TableMeta

	// scanPrivate is the private of the scan operator to enumerate indexes for.
	scanPrivate *memo.ScanPrivate

	// filters is the filters that are applied after the original scan. If there
	// are no filters applied after the original scan, filters should be set to
	// nil. It is used to determine if a partial index can be enumerated and to
	// generate the filters passed to the enumerateIndexFunc (the filters are
	// passed as-is for non-partial indexes).
	filters memo.FiltersExpr

	// originalFilters is optional, non-reduced filters used to determine if a
	// partial index can be enumerated. See SetOriginalFilters for details.
	originalFilters memo.FiltersExpr

	// rejectFlags is a set of flags that designate which types of indexes to
	// skip during iteration.
	rejectFlags indexRejectFlags

	// filtersMutateChecker is used to assert that filters are not mutated by
	// enumerateIndexFunc callbacks. This check is a no-op in non-test builds.
	filtersMutateChecker memo.FiltersExprMutateChecker

	// originalFiltersMutateChecker is used to assert that originalFilters are
	// not mutated by enumerateIndexFunc callbacks. This check is a no-op in
	// non-test builds.
	originalFiltersMutateChecker memo.FiltersExprMutateChecker
}

// Init initializes a new scanIndexIter.
func (it *scanIndexIter) Init(
	evalCtx *tree.EvalContext,
	f *norm.Factory,
	mem *memo.Memo,
	im *partialidx.Implicator,
	scanPrivate *memo.ScanPrivate,
	filters memo.FiltersExpr,
	rejectFlags indexRejectFlags,
) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*it = scanIndexIter{
		evalCtx:     evalCtx,
		f:           f,
		im:          im,
		tabMeta:     mem.Metadata().TableMeta(scanPrivate.Table),
		scanPrivate: scanPrivate,
		filters:     filters,
		rejectFlags: rejectFlags,
	}
	it.filtersMutateChecker.Init(it.filters)
}

// SetOriginalFilters specifies an optional, non-reduced, original set of
// filters used to determine if a partial index can be enumerated. If the
// filters passed in Init do not imply a partial index predicate, the
// scanIndexIter will check if the original filters imply the predicate and
// enumerate the partial index if successful.
//
// This is useful in nested iteration when two partial indexes have the same or
// similar predicate.
//
// Consider the indexes and query:
//
//   CREATE INDEX idx1 ON t (a) WHERE c > 0
//   CREATE INDEX idx2 ON t (b) WHERE c > 0
//
//   SELECT * FROM t WHERE a = 1 AND b = 2 AND c > 0
//
// The optimal query plan is a zigzag join over idx1 and idx2. Planning a zigzag
// join requires a nested loop over the indexes of a table. The outer loop will
// first enumerate idx1 with remaining filters of (a = 1 AND b = 2). Those
// filters, which are passed to the inner loop Init, do not imply idx2's
// predicate. With those filters alone, idx2 would not be enumerated in the
// inner loop.
//
// But by specifying the original filters of (a = 1 AND b = 2 AND c > 0) for the
// inner loop, implication of idx2's predicate can be proven when implication of
// the filters passed in Init fail to prove implication. Thus, idx2 can be
// enumerated, and a zigzag join can be planned.
//
// Note that simply passing the original filters to the inner loop's Init is
// insufficient. It is necessary to maintain two sets of filters to both handle
// the example above and to reduce the remaining filters as much as possible
// when two partial indexes have different predicates.
//
// Consider the indexes and query:
//
//   CREATE INDEX idx1 ON t (a) WHERE b > 0
//   CREATE INDEX idx2 ON t (c) WHERE d > 0
//
//   SELECT * FROM t WHERE a = 1 AND b > 0 AND c = 2 AND d > 0
//
// The optimal query plan is a zigzag join over idx1 and idx2 with no remaining
// Select filters. If the original filters were passed to the inner loop's Init,
// they would imply idx2's predicate, but the remaining filters would be (a = 1
// AND b > 0 AND c = 2). Only (d > 0) can be removed during implication of
// idx2's predicate. The (b > 0) expression would be needlessly applied in an
// Select that wraps the zigzag join.
func (it *scanIndexIter) SetOriginalFilters(filters memo.FiltersExpr) {
	if it.filters == nil {
		panic(errors.AssertionFailedf("cannot specify originalFilters with nil filters"))
	}
	it.originalFilters = filters
	it.originalFiltersMutateChecker.Init(it.originalFilters)
}

// enumerateIndexFunc defines the callback function for the ForEach and
// ForEachStartingAfter functions. It is invoked for each index enumerated.
//
// The function is called with the enumerated index, the filters that must be
// applied after a scan over the index, and the index columns. The isCovering
// boolean is true if the index covers the scanPrivate's columns, indicating
// that an index join with the primary index is not necessary. A partial index
// may cover columns not actually stored in the index because the predicate
// holds the column constant. In this case, the provided constProj must be
// projected after the scan to produce all required columns.
//
// If the index is a partial index, the filters are the remaining filters after
// proving partial index implication (see partialidx.Implicator). Otherwise, the
// filters are the same filters that were passed to Init.
//
// Note that the filters argument CANNOT be mutated in the callback function
// because these filters are used internally by scanIndexIter for partial index
// implication while iterating over indexes. In tests the filtersMutateChecker
// will detect a callback that mutates filters and panic.
type enumerateIndexFunc func(
	idx cat.Index,
	filters memo.FiltersExpr,
	indexCols opt.ColSet,
	isCovering bool,
	constProj memo.ProjectionsExpr,
)

// ForEach calls the given callback function for every index of the Scan
// operator's table in the order they appear in the catalog.
//
// The rejectFlags determine types of indexes to skip, if any.
//
// Partial indexes are skipped if their predicate is not implied by the filters.
// If the filters are nil, then only pseudo-partial indexes (a partial index
// with an expression that always evaluates to true) are enumerated. If the
// filters are reduced during partial index implication, the remaining filters
// are passed to the callback f.
//
// If the ForceIndex flag is set on the scanPrivate, then all indexes except the
// forced index are skipped. The index forced by the ForceIndex flag is not
// guaranteed to be iterated on - it will be skipped if it is rejected by the
// rejectFlags, or if it is a partial index with a predicate that is not implied
// by the filters.
func (it *scanIndexIter) ForEach(f enumerateIndexFunc) {
	it.ForEachStartingAfter(cat.PrimaryIndex-1, f)
}

// ForEachStartingAfter calls the given callback function for every index of the
// Scan operator's table with an ordinal greater than ord.
func (it *scanIndexIter) ForEachStartingAfter(ord int, f enumerateIndexFunc) {
	ord++
	for ; ord < it.tabMeta.Table.IndexCount(); ord++ {
		// Skip over the primary index if rejectPrimaryIndex is set.
		if it.hasRejectFlags(rejectPrimaryIndex) && ord == cat.PrimaryIndex {
			continue
		}

		// If we are forcing a specific index, ignore all other indexes.
		if it.scanPrivate.Flags.ForceIndex && ord != it.scanPrivate.Flags.Index {
			continue
		}

		index := it.tabMeta.Table.Index(ord)

		// Skip over inverted indexes if rejectInvertedIndexes is set.
		if it.hasRejectFlags(rejectInvertedIndexes) && index.IsInverted() {
			continue
		}

		// Skip over non-inverted indexes if rejectNonInvertedIndexes is set.
		if it.hasRejectFlags(rejectNonInvertedIndexes) && !index.IsInverted() {
			continue
		}

		pred, isPartialIndex := it.tabMeta.PartialIndexPredicate(ord)

		// Skip over partial indexes if rejectPartialIndexes is set.
		if it.hasRejectFlags(rejectPartialIndexes) && isPartialIndex {
			continue
		}

		// Skip over non-partial indexes if rejectNonPartialIndexes is set.
		if it.hasRejectFlags(rejectNonPartialIndexes) && !isPartialIndex {
			continue
		}

		filters := it.filters

		// If the index is a partial index, check whether the filters imply the
		// predicate.
		var predFilters memo.FiltersExpr
		if isPartialIndex {
			predFilters = *pred.(*memo.FiltersExpr)

			// If there are no filters, then skip over any partial indexes that
			// are not pseudo-partial indexes.
			if filters == nil && !predFilters.IsTrue() {
				continue
			}

			if filters != nil {
				remainingFilters, ok := it.filtersImplyPredicate(predFilters)
				if !ok {
					// The predicate is not implied by the filters, so skip over
					// the partial index.
					continue
				}

				// Set the filters to the remaining filters which may have been
				// reduced.
				filters = remainingFilters
			}
		}

		indexCols := it.tabMeta.IndexColumns(ord)
		isCovering := it.scanPrivate.Cols.SubsetOf(indexCols)

		// If the index does not contain all required columns, attempt to use
		// columns held constant in the partial index predicate to cover the
		// columns.
		var constProj memo.ProjectionsExpr
		if !isCovering && len(predFilters) > 0 {
			constCols := it.extractConstNonCompositeColumns(predFilters)
			if !constCols.Empty() && it.scanPrivate.Cols.SubsetOf(indexCols.Union(constCols)) {
				isCovering = true

				// Build a projection only for constant columns not in the
				// index.
				constCols = constCols.Difference(indexCols)
				constProj = it.buildConstProjectionsFromPredicate(predFilters, constCols)
			}
		}

		f(index, filters, indexCols, isCovering, constProj)

		// Verify that f did not mutate filters or originalFilters (in test
		// builds only).
		it.filtersMutateChecker.CheckForMutation(it.filters)
		if it.originalFilters != nil {
			it.originalFiltersMutateChecker.CheckForMutation(it.originalFilters)
		}
	}
}

// filtersImplyPredicate attempts to prove that the filters imply the given
// partial index predicate expression. If the filters or originalFilters imply
// the predicate, it returns the remaining filters that must be applied after a
// scan over the partial index. If the predicate is not implied by the filters,
// ok=false is returned.
func (it *scanIndexIter) filtersImplyPredicate(
	pred memo.FiltersExpr,
) (remainingFilters memo.FiltersExpr, ok bool) {
	// Return the remaining filters if the filters imply the predicate.
	if remainingFilters, ok = it.im.FiltersImplyPredicate(it.filters, pred); ok {
		return remainingFilters, true
	}

	// If the filters do not imply the predicate, but the originalFilters do,
	// return the remaining filters from the implication. We prefer checking the
	// filters before the originalFilters because we want to return the most
	// minimal set of remaining filters possible. The filters are a subset of
	// the originalFilters, therefore the remaining filters from
	// filters-implication are a subset of the remaining filters from
	// originalFilters-implication.
	if it.originalFilters != nil {
		if remainingFilters, ok = it.im.FiltersImplyPredicate(it.originalFilters, pred); ok {
			return remainingFilters, true
		}
	}

	return nil, false
}

// extractConstNonCompositeColumns returns the set of columns held constant by
// the given filters and of types that do not have composite encodings.
func (it *scanIndexIter) extractConstNonCompositeColumns(f memo.FiltersExpr) opt.ColSet {
	constCols := memo.ExtractConstColumns(f, it.evalCtx)
	var constNonCompositeCols opt.ColSet
	for col, ok := constCols.Next(0); ok; col, ok = constCols.Next(col + 1) {
		ord := it.tabMeta.MetaID.ColumnOrdinal(col)
		typ := it.tabMeta.Table.Column(ord).DatumType()
		if !colinfo.HasCompositeKeyEncoding(typ) {
			constNonCompositeCols.Add(col)
		}
	}
	return constNonCompositeCols
}

// buildConstProjectionsFromPredicate builds a ProjectionsExpr that projects
// constant values for the given constCols. The constant values are extracted
// from the given partial index predicate expression. Panics if a constant value
// cannot be extracted from pred for any of the constCols.
func (it *scanIndexIter) buildConstProjectionsFromPredicate(
	pred memo.FiltersExpr, constCols opt.ColSet,
) memo.ProjectionsExpr {
	proj := make(memo.ProjectionsExpr, 0, constCols.Len())
	for col, ok := constCols.Next(0); ok; col, ok = constCols.Next(col + 1) {
		ord := it.tabMeta.MetaID.ColumnOrdinal(col)
		typ := it.tabMeta.Table.Column(ord).DatumType()

		val := memo.ExtractValueForConstColumn(pred, it.evalCtx, col)
		if val == nil {
			panic(errors.AssertionFailedf("could not extract constant value for column %d", col))
		}

		scalar := it.f.ConstructConstVal(val, typ)

		proj = append(proj, it.f.ConstructProjectionsItem(
			scalar,
			col,
		))
	}
	return proj
}

// hasRejectFlags tests whether the given flags are all set.
func (it *scanIndexIter) hasRejectFlags(subset indexRejectFlags) bool {
	return it.rejectFlags&subset == subset
}
