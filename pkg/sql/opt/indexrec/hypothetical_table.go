// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package indexrec

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
)

// BuildOptAndHypTableMaps builds a HypotheticalTable for each table in
// indexCandidates. This HypotheticalTable stores a hypothetical index for each
// of the table's index candidates. The function returns a map from each table's
// cat.StableID to its original sql.optTable, as well as a map from each table's
// cat.StableID to its constructed HypotheticalTable. These tables will be used
// to update the table query metadata when making index recommendations.
func BuildOptAndHypTableMaps(
	indexCandidates map[cat.Table][][]cat.IndexColumn,
) (optTables, hypTables map[cat.StableID]cat.Table) {
	numTables := len(indexCandidates)
	hypTables = make(map[cat.StableID]cat.Table, numTables)
	optTables = make(map[cat.StableID]cat.Table, numTables)

	for t, indexes := range indexCandidates {
		hypIndexes := make([]hypotheticalIndex, 0, len(indexes))
		var hypTable HypotheticalTable
		hypTable.init(t)

		for _, indexCols := range indexes {
			indexOrd := hypTable.Table.IndexCount() + len(hypIndexes)
			lastKeyCol := indexCols[len(indexCols)-1]
			inverted := !colinfo.ColumnTypeIsIndexable(lastKeyCol.DatumType())
			if inverted {
				invertedCol := hypTable.addInvertedCol(lastKeyCol.Column)
				indexCols[len(indexCols)-1] = cat.IndexColumn{Column: invertedCol}
			}
			var hypIndex hypotheticalIndex
			hypIndex.init(
				&hypTable,
				tree.Name(fmt.Sprintf("_hyp_%d", indexOrd)),
				indexCols,
				indexOrd,
				inverted,
				t.Zone(),
			)

			// Do not add hypothetical inverted indexes for which there is an existing
			// index with the same key. Inverted indexes do not have stored columns,
			// so we should not make a recommendation if the same index already
			// exists.
			if !inverted || hypTable.existingRedundantIndex(&hypIndex) == nil {
				hypIndexes = append(hypIndexes, hypIndex)
			}
		}

		hypTable.hypotheticalIndexes = hypIndexes
		optTables[t.ID()] = t
		hypTables[t.ID()] = &hypTable
	}

	return optTables, hypTables
}

// HypotheticalTable is a wrapper around cat.Table, used for creating index
// recommendations. The hypotheticalIndexes slice stores fake indexes that could
// potentially speed up queries to this table.
type HypotheticalTable struct {
	cat.Table
	invertedCols         []*cat.Column
	primaryKeyColsOrdSet intsets.Fast
	hypotheticalIndexes  []hypotheticalIndex
}

var _ cat.Table = &HypotheticalTable{}

func (ht *HypotheticalTable) init(table cat.Table) {
	ht.Table = table

	// Get PK column ordinals.
	primaryIndex := ht.Index(cat.PrimaryIndex)
	numPrimaryKeyCols := primaryIndex.KeyColumnCount()
	for i := 0; i < numPrimaryKeyCols; i++ {
		ht.primaryKeyColsOrdSet.Add(primaryIndex.Column(i).Ordinal())
	}
}

// ColumnCount is part of the cat.Table interface.
func (ht *HypotheticalTable) ColumnCount() int {
	return ht.Table.ColumnCount() + len(ht.invertedCols)
}

// Column is part of the cat.Table interface.
func (ht *HypotheticalTable) Column(i int) *cat.Column {
	originalColCount := ht.Table.ColumnCount()
	if i < originalColCount {
		return ht.Table.Column(i)
	}
	return ht.invertedCols[i-originalColCount]
}

// IndexCount is part of the cat.Table interface.
func (ht *HypotheticalTable) IndexCount() int {
	// A HypotheticalTable stores the embedded table's existing indexes in
	// addition to its hypothetical indexes.
	return ht.Table.IndexCount() + len(ht.hypotheticalIndexes)
}

// WritableIndexCount is part of the cat.Table interface.
func (ht *HypotheticalTable) WritableIndexCount() int {
	return ht.IndexCount()
}

// DeletableIndexCount is part of the cat.Table interface.
func (ht *HypotheticalTable) DeletableIndexCount() int {
	return ht.IndexCount()
}

// Index is part of the cat.Table interface.
func (ht *HypotheticalTable) Index(i cat.IndexOrdinal) cat.Index {
	existingIndexCount := ht.Table.IndexCount()
	if i < existingIndexCount {
		return ht.Table.Index(i)
	}
	return &ht.hypotheticalIndexes[i-existingIndexCount]
}

// existingRedundantIndex checks whether a visible index with the same explicit
// columns as the index argument is present in the HypotheticalTable's embedded
// table. If so, it returns the first instance of such an existing index (that
// is not a partial index and visible). Existing partial indexes and
// hypothetical standard indexes are not considered redundant. Otherwise, the
// function returns nil.
func (ht *HypotheticalTable) existingRedundantIndex(index *hypotheticalIndex) cat.Index {
	for i, n := 0, ht.Table.IndexCount(); i < n; i++ {
		existingIndex := ht.Table.Index(i)
		indexExists := index.hasSameExplicitCols(existingIndex, index.IsInverted())
		_, isPartialIndex := existingIndex.Predicate()
		if indexExists && !isPartialIndex && !existingIndex.IsNotVisible() {
			return existingIndex
		}
	}
	return nil
}

// addInvertedCol adds an inverted column corresponding to a source column to
// the HypotheticalTable.
func (ht *HypotheticalTable) addInvertedCol(invertedSourceCol *cat.Column) *cat.Column {
	invertedCol := cat.Column{}

	invertedCol.InitInverted(
		ht.ColumnCount(),
		tree.Name(string(invertedSourceCol.ColName())+"_inverted_key"),
		types.EncodedKey,
		false, /* nullable */
		invertedSourceCol.Ordinal(),
	)

	ht.invertedCols = append(ht.invertedCols, &invertedCol)
	return &invertedCol
}
