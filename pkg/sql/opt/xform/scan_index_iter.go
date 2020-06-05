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
)

// indexRejectFlags contains flags designating types of indexes to filter out
// during iteration. For example, the iterator would skip over inverted and
// partial indexes given these flags:
//
//   flags := rejectInvertedIndexes|rejectPartialIndexes
//
type indexRejectFlags int

const (
	// rejectNoIndexes is the default, which includes all indexes during
	// iteration.
	rejectNoIndexes indexRejectFlags = 0

	// rejectPrimaryIndex excludes the primary index during iteration.
	rejectPrimaryIndex indexRejectFlags = 1 << iota

	// rejectInvertedIndexes excludes any inverted indexes during iteration.
	rejectInvertedIndexes

	// rejectNonInvertedIndexes excludes any non-inverted indexes during
	// iteration.
	rejectNonInvertedIndexes

	// rejectPartialIndexes excludes any partial indexes during iteration.
	rejectPartialIndexes
)

// scanIndexIter is a helper struct that supports iteration over the indexes
// of a Scan operator table. For example:
//
//   iter := makeScanIndexIter(mem, scanPrivate, rejectNoIndexes)
//   for iter.next() {
//     index := iter.index
//     indexOrd := iter.indexOrdinal
//     cols := iter.indexCols()
//     isCovering := iter.isCovering()
//   }
//
type scanIndexIter struct {
	mem          *memo.Memo
	scanPrivate  *memo.ScanPrivate
	tab          cat.Table
	indexOrdinal cat.IndexOrdinal
	index        cat.Index
	cols         opt.ColSet
	rejectFlags  indexRejectFlags
}

// makeScanIndexIter returns an initialized scanIndexIter.
//
// The rejectFlags determine which types of indexes to skip when iterating.
func makeScanIndexIter(
	mem *memo.Memo, scanPrivate *memo.ScanPrivate, rejectFlags indexRejectFlags,
) scanIndexIter {
	return scanIndexIter{
		mem:          mem,
		scanPrivate:  scanPrivate,
		tab:          mem.Metadata().Table(scanPrivate.Table),
		indexOrdinal: -1,
		rejectFlags:  rejectFlags,
	}
}

// next advances iteration to the next index of the Scan operator's table. This
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
func (it *scanIndexIter) next() bool {
	for {
		it.indexOrdinal++

		if it.indexOrdinal >= it.tab.IndexCount() {
			it.index = nil
			return false
		}

		it.index = it.tab.Index(it.indexOrdinal)

		// Skip over the primary index if rejectPrimaryIndex is set.
		if it.hasRejectFlag(rejectPrimaryIndex) && it.indexOrdinal == cat.PrimaryIndex {
			continue
		}

		// Skip over inverted indexes if rejectInvertedIndexes is set.
		if it.hasRejectFlag(rejectInvertedIndexes) && it.index.IsInverted() {
			continue
		}

		// Skip over non-inverted indexes if rejectNonInvertedIndexes is set.
		if it.hasRejectFlag(rejectNonInvertedIndexes) && !it.index.IsInverted() {
			continue
		}

		// Skip over partial indexes if rejectPartialIndexes is set.
		_, ok := it.index.Predicate()
		if it.hasRejectFlag(rejectPartialIndexes) && ok {
			continue
		}

		// If we are forcing a specific index, ignore all other indexes.
		if it.scanPrivate.Flags.ForceIndex && it.scanPrivate.Flags.Index != it.indexOrdinal {
			continue
		}

		// Reset the cols so they can be recalculated.
		it.cols = opt.ColSet{}
		return true
	}
}

// indexCols returns the set of columns contained in the current index.
func (it *scanIndexIter) indexCols() opt.ColSet {
	if it.cols.Empty() {
		it.cols = it.mem.Metadata().TableMeta(it.scanPrivate.Table).IndexColumns(it.indexOrdinal)
	}
	return it.cols
}

// isCovering returns true if the current index contains all columns projected
// by the Scan operator.
func (it *scanIndexIter) isCovering() bool {
	return it.scanPrivate.Cols.SubsetOf(it.indexCols())
}

// hasRejectFlag returns true if the given flag is set in the rejectFlags.
func (it *scanIndexIter) hasRejectFlag(flag indexRejectFlags) bool {
	return it.rejectFlags&flag != 0
}
