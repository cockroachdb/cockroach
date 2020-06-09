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
	"github.com/cockroachdb/errors"
)

// indexRejectFlags contains flags designating types of indexes to filter out
// during iteration. For example, the iterator would skip over inverted and
// partial indexes given these flags:
//
//   flags := rejectInvertedIndexes|rejectPartialIndexes
//
type indexRejectFlags int8

const (
	// rejectNoIndexes is the default, which includes all indexes during
	// iteration.
	rejectNoIndexes indexRejectFlags = 0

	// rejectPrimaryIndex excludes the primary index during iteration.
	rejectPrimaryIndex indexRejectFlags = 1 << (iota - 1)

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

// scanIndexIter is a helper struct that supports iteration over the indexes
// of a Scan operator table. For example:
//
//   iter := makeScanIndexIter(mem, scanPrivate, rejectNoIndexes)
//   for iter.Next() {
//     index := iter.Index()
//     cols := iter.IndexColumns()
//     isCovering := iter.IsCovering()
//   }
//
type scanIndexIter struct {
	mem         *memo.Memo
	scanPrivate *memo.ScanPrivate
	tabMeta     *opt.TableMeta
	rejectFlags indexRejectFlags
	indexCount  int

	// indexOrd is the ordinal of the current index in the list of the table's
	// indexes.
	indexOrd cat.IndexOrdinal

	// currIndex is the current cat.Index that has been iterated to.
	currIndex cat.Index

	// indexColsCache caches the set of columns included in the index. See
	// IndexColumns for more details.
	indexColsCache opt.ColSet
}

// makeScanIndexIter returns an initialized scanIndexIter.
//
// The rejectFlags determine which types of indexes to skip when iterating.
func makeScanIndexIter(
	mem *memo.Memo, scanPrivate *memo.ScanPrivate, rejectFlags indexRejectFlags,
) scanIndexIter {
	tabMeta := mem.Metadata().TableMeta(scanPrivate.Table)
	return scanIndexIter{
		mem:         mem,
		scanPrivate: scanPrivate,
		tabMeta:     tabMeta,
		indexCount:  tabMeta.Table.IndexCount(),
		indexOrd:    -1,
		rejectFlags: rejectFlags,
	}
}

// StartAfter will cause the iterator to skip over indexes so that the first
// call to Next will iterate to the index directly after the given index
// ordinal, if there is one. StartAfter will panic if Next has already been
// called on the iterator.
func (it *scanIndexIter) StartAfter(i cat.IndexOrdinal) {
	if it.indexOrd != -1 {
		panic(errors.AssertionFailedf("cannot call StartAfter if iteration has started"))
	}
	it.indexOrd = i
}

// Next advances iteration to the next index of the Scan operator's table. This
// is the primary index if it's the first time next is called, or a secondary
// index thereafter. When there are no more indexes to enumerate, next returns
// false. The current index is accessible via the iterator's "index" field.
//
// The rejectFlags set in makeScanIndexIter determine which indexes to skip when
// iterating, if any.
//
// If the ForceIndex flag is set on the scanPrivate, then all indexes except the
// forced index are skipped. Note that the index forced by the ForceIndex flag
// is not guaranteed to be iterated on - it will be skipped if it is rejected by
// the rejectFlags.
func (it *scanIndexIter) Next() bool {
	for {
		it.indexOrd++

		if it.indexOrd >= it.indexCount {
			it.currIndex = nil
			return false
		}

		it.currIndex = it.tabMeta.Table.Index(it.indexOrd)

		// Skip over the primary index if rejectPrimaryIndex is set.
		if it.hasRejectFlag(rejectPrimaryIndex) && it.indexOrd == cat.PrimaryIndex {
			continue
		}

		// Skip over inverted indexes if rejectInvertedIndexes is set.
		if it.hasRejectFlag(rejectInvertedIndexes) && it.currIndex.IsInverted() {
			continue
		}

		// Skip over non-inverted indexes if rejectNonInvertedIndexes is set.
		if it.hasRejectFlag(rejectNonInvertedIndexes) && !it.currIndex.IsInverted() {
			continue
		}

		if it.hasRejectFlag(rejectPartialIndexes | rejectNonPartialIndexes) {
			_, ok := it.currIndex.Predicate()

			// Skip over partial indexes if rejectPartialIndexes is set.
			if it.hasRejectFlag(rejectPartialIndexes) && ok {
				continue
			}

			// Skip over non-partial indexes if rejectNonPartialIndexes is set.
			if it.hasRejectFlag(rejectNonPartialIndexes) && !ok {
				continue
			}
		}

		// If we are forcing a specific index, ignore all other indexes.
		if it.scanPrivate.Flags.ForceIndex && it.scanPrivate.Flags.Index != it.indexOrd {
			continue
		}

		// Reset the cols so they can be recalculated.
		it.indexColsCache = opt.ColSet{}
		return true
	}
}

// IndexOrdinal returns the ordinal of the current index that has been iterated
// to.
func (it *scanIndexIter) IndexOrdinal() int {
	return it.indexOrd
}

// Index returns the current index that has been iterated to.
func (it *scanIndexIter) Index() cat.Index {
	return it.currIndex
}

// IndexColumns returns the set of columns contained in the current index. This
// set includes the columns indexed and stored, as well as the primary key
// columns.
// TODO(mgartner): Caching here will no longer be necessary if we cache in
// TableMeta. See the TODO at TableMeta.IndexColumns.
func (it *scanIndexIter) IndexColumns() opt.ColSet {
	if it.indexColsCache.Empty() {
		it.indexColsCache = it.tabMeta.IndexColumns(it.indexOrd)
	}
	return it.indexColsCache
}

// IsCovering returns true if the current index "covers" all the columns projected
// by the Scan operator. An index covers any columns that it indexes or stores,
// as well as any primary key columns.
func (it *scanIndexIter) IsCovering() bool {
	return it.scanPrivate.Cols.SubsetOf(it.IndexColumns())
}

// hasRejectFlag returns true if the given flag is set in the rejectFlags.
func (it *scanIndexIter) hasRejectFlag(flag indexRejectFlags) bool {
	return it.rejectFlags&flag != 0
}
