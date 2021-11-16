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

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// BuildOptAndHypTableMaps builds a hypotheticalTable for each table in
// indexCandidates. This hypotheticalTable stores a hypothetical index for each
// of the table's index candidates. The function returns a map from each table's
// cat.StableID to its original sql.optTable, as well as a map from each table's
// cat.StableID to its constructed hypotheticalTable. These tables are used to
// update the table query metadata when making index recommendations.
func BuildOptAndHypTableMaps(
	indexCandidates map[cat.Table][][]cat.IndexColumn,
	tableZones map[cat.Table]*zonepb.ZoneConfig,
	existingIndexes map[cat.Table][][]cat.IndexColumn,
) (optTables, hypTables map[cat.StableID]cat.Table) {
	hypTables = make(map[cat.StableID]cat.Table)
	optTables = make(map[cat.StableID]cat.Table)

	for t, indexes := range indexCandidates {
		// Get PK column ordinals.
		primaryIndex := t.Index(cat.PrimaryIndex)
		numPrimaryKeyCols := primaryIndex.KeyColumnCount()
		var tabPrimaryKeyCols util.FastIntSet
		for i := 0; i < numPrimaryKeyCols; i++ {
			tabPrimaryKeyCols.Add(primaryIndex.Column(i).Ordinal())
		}

		hypIndexes := make([]cat.Index, 0, len(indexes))
		hypTable := hypotheticalTable{
			Table:               t,
			primaryKeyCols:      tabPrimaryKeyCols,
			existingIndexes:     existingIndexes[t],
			hypotheticalIndexes: hypIndexes,
		}
		indexCount := t.IndexCount()

		for _, index := range indexes {
			// Do not add the index if it is equivalent to an existing index.
			if hypTable.indexExists(index) {
				continue
			}

			// Build index column ordinal set.
			var indexColsOrdSet util.FastIntSet
			for _, indexCol := range index {
				indexColsOrdSet.Add(indexCol.Ordinal())
			}

			idx := hypotheticalIndex{
				tab:            &hypTable,
				name:           tree.Name(fmt.Sprintf("_hyp_%d", indexCount)),
				cols:           index,
				colsOrdinalSet: indexColsOrdSet,
				indexOrdinal:   indexCount,
				zone:           tableZones[t],
			}
			hypIndexes = append(hypIndexes, &idx)
			indexCount++
		}

		hypTable.hypotheticalIndexes = hypIndexes
		optTables[t.ID()] = t
		hypTables[t.ID()] = &hypTable
	}

	return optTables, hypTables
}

// indexExists checks whether an index is present in the slice of
// existingIndexes containing a table's existing indexes.
func (ht *hypotheticalTable) indexExists(index []cat.IndexColumn) bool {
	for i, n := 0, len(ht.existingIndexes); i < n; i++ {
		existingIndex := ht.existingIndexes[i]
		if len(existingIndex) != len(index) {
			continue
		}
		indexExists := true
		for j, m := 0, len(existingIndex); j < m; j++ {
			if existingIndex[j].ColID() != index[j].ColID() ||
				existingIndex[j].Descending != index[j].Descending {
				indexExists = false
				break
			}
		}
		if indexExists {
			return true
		}
	}
	return false
}

// hypotheticalTable is a wrapper around cat.Table, used for creating index
// recommendations. The hypotheticalIndexes slice stores fake indexes that could
// potentially speed up queries to this table.
type hypotheticalTable struct {
	cat.Table
	primaryKeyCols      util.FastIntSet
	existingIndexes     [][]cat.IndexColumn
	hypotheticalIndexes []cat.Index
}

var _ cat.Table = &hypotheticalTable{}

// IndexCount is part of the cat.Table interface.
func (ht *hypotheticalTable) IndexCount() int {
	return len(ht.hypotheticalIndexes) + ht.Table.IndexCount()
}

// WritableIndexCount is part of the cat.Table interface.
func (ht *hypotheticalTable) WritableIndexCount() int {
	return ht.IndexCount()
}

// DeletableIndexCount is part of the cat.Table interface.
func (ht *hypotheticalTable) DeletableIndexCount() int {
	return ht.IndexCount()
}

// Index is part of the cat.Table interface.
func (ht *hypotheticalTable) Index(i cat.IndexOrdinal) cat.Index {
	existingIndexCount := ht.Table.IndexCount()
	if i < existingIndexCount {
		return ht.Table.Index(i)
	}
	hypOrd := i - existingIndexCount
	return ht.hypotheticalIndexes[hypOrd]
}
