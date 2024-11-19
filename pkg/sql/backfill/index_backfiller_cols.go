// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backfill

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

// indexBackfillerCols holds the interned set of columns used by the
// indexBackfiller and the information about which columns will need to
// be evaluated during the backfill.
type indexBackfillerCols struct {
	// colIdxMap maps ColumnIDs to indices into desc.Columns and desc.Mutations.
	colIdxMap catalog.TableColMap

	// cols are all writable (PUBLIC and WRITE_ONLY) columns in
	// the descriptor.
	cols []catalog.Column

	// addedCols are the columns in WRITE_ONLY being added as part of
	// this index which are not computed. The definition of being added is that
	// the index being backfilled is a new primary index and the columns do not
	// exist in the currently public primary index. Note that this should never
	// be populated when running using the legacy schema changer; the legacy
	// schema changer rewrites primary indexes in-place using the column
	// backfiller before creating any secondary indexes.
	addedCols []catalog.Column

	// computedCols are the columns in this index which are computed and do
	// not have concrete values in the source index. This is virtual computed
	// columns and stored computed columns which are non-public and not part
	// of the currently public primary index.
	computedCols []catalog.Column

	// valNeededForCol contains the indexes (into cols) of all columns that we
	// need to fetch values for.
	valNeededForCol intsets.Fast
}

// makeIndexBackfillColumns computes the set of writable columns and
// classifies the two subsets of that set which will need to be computed
// during execution of the backfill. Note that all columns will be included
// in the complete cols slice regardless of whether they appear in
// addedIndexes. Appearance in addedIndexes will affect valNeededForColumn.
// Note that the valNeededForCol set will not be populated with
// requirements due to references in expressions. That needs to be added after
// constructing this information.
func makeIndexBackfillColumns(
	deletableColumns []catalog.Column, sourcePrimaryIndex catalog.Index, addedIndexes []catalog.Index,
) (indexBackfillerCols, error) {

	// We will need to evaluate default or computed expressions for
	// physical columns being added only if this is a primary index backfill
	// and only if the added columns are not in the existing public primary
	// index. We're going to assume that if there is a new column being added
	// to a new primary index that it doesn't appear in any secondary index
	// being simultaneously backfilled, because if that column were needed,
	// and we're adding it now, we'd have a problem.

	var ib indexBackfillerCols
	ib.cols = make([]catalog.Column, 0, len(deletableColumns))

	var computedVirtual, allIndexColumns catalog.TableColSet
	primaryColumns := indexColumns(sourcePrimaryIndex)
	for _, idx := range addedIndexes {
		allIndexColumns.UnionWith(indexColumns(idx))
	}
	for _, column := range deletableColumns {
		if !column.Public() &&
			// Include columns we are adding, in case we are adding them to a
			// new primary index as part of an ADD COLUMN backfill. Later code
			// will validate that we're allowed to be adding this column to the
			// new index.
			!(column.Adding() && column.WriteAndDeleteOnly()) &&
			// Include columns we're dropping if the column is already part of
			// the current primary index and needs to be in the new index. Code
			// elsewhere will enforce that the index we're building including this
			// dropped column is allowed to include the dropped column.
			!(column.Dropped() && primaryColumns.Contains(column.GetID()) &&
				allIndexColumns.Contains(column.GetID())) {
			continue
		}
		if column.IsComputed() && column.IsVirtual() {
			computedVirtual.Add(column.GetID())
			ib.computedCols = append(ib.computedCols, column)
		}
		// Create a map of each column's ID to its ordinal.
		ib.colIdxMap.Set(column.GetID(), len(ib.cols))
		ib.cols = append(ib.cols, column)
	}

	// Find the adding columns which are being added to new primary indexes.
	var addedDefaultOrComputed catalog.TableColSet
	for _, idx := range addedIndexes {
		if idx.GetEncodingType() != catenumpb.PrimaryIndexEncoding {
			if !indexColumns(idx).Difference(computedVirtual).SubsetOf(primaryColumns) {
				return indexBackfillerCols{}, errors.AssertionFailedf(
					"secondary index for backfill contains physical column not present in " +
						"source primary index",
				)
			}
		}
		addedDefaultOrComputed.UnionWith(
			indexColumns(idx).Difference(computedVirtual).Difference(primaryColumns),
		)
	}

	// Account for these new columns being initially backfilled into new primary
	// indexes.
	for _, colID := range addedDefaultOrComputed.Ordered() {
		ord, ok := ib.colIdxMap.Get(colID)
		if !ok {
			return indexBackfillerCols{}, errors.AssertionFailedf(
				"index being backfilled contains non-writable or dropping column")
		}
		col := ib.cols[ord]
		if col.IsComputed() {
			ib.computedCols = append(ib.computedCols, col)
		} else {
			ib.addedCols = append(ib.addedCols, col)
		}
	}

	ib.valNeededForCol = makeInitialValNeededForCol(ib, addedIndexes)
	return ib, nil
}

// makeInitialValNeededForCol computes the initial set of columns whose values
// are known to be needed based solely on index membership. More may be needed
// because of references in expressions.
func makeInitialValNeededForCol(
	ib indexBackfillerCols, addedIndexes []catalog.Index,
) (valNeededForCol intsets.Fast) {
	// Any columns we're going to eval, we don't need values for ahead of time.
	toEval := func() catalog.TableColSet {
		columnIDs := func(columns []catalog.Column) (s catalog.TableColSet) {
			for _, c := range columns {
				s.Add(c.GetID())
			}
			return s
		}

		// The set of columns we'll evaluate are the addedColumns and the computed
		// columns. We don't need values for these columns.
		toEval := columnIDs(ib.addedCols)
		toEval.UnionWith(columnIDs(ib.computedCols))
		return toEval
	}()

	for _, idx := range addedIndexes {
		colIDs := idx.CollectKeyColumnIDs()
		if idx.GetEncodingType() == catenumpb.PrimaryIndexEncoding {
			for _, col := range ib.cols {
				if !col.IsVirtual() {
					colIDs.Add(col.GetID())
				}
			}
		} else {
			colIDs.UnionWith(idx.CollectSecondaryStoredColumnIDs())
			colIDs.UnionWith(idx.CollectKeySuffixColumnIDs())
		}

		for i := range ib.cols {
			if id := ib.cols[i].GetID(); colIDs.Contains(id) && !toEval.Contains(id) {
				valNeededForCol.Add(i)
			}
		}
	}
	return valNeededForCol
}

// indexColumns computes the complete set of column stored in an index.
func indexColumns(idx catalog.Index) (s catalog.TableColSet) {
	s.UnionWith(idx.CollectKeyColumnIDs())
	s.UnionWith(idx.CollectKeySuffixColumnIDs())
	s.UnionWith(idx.CollectPrimaryStoredColumnIDs())
	s.UnionWith(idx.CollectSecondaryStoredColumnIDs())
	return s
}
