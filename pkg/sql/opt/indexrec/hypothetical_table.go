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
// cat.StableID to its constructed hypotheticalTable. These tables will be used
// to update the table query metadata when making index recommendations.
func BuildOptAndHypTableMaps(
	indexCandidates, existingIndexes map[cat.Table][][]cat.IndexColumn,
) (optTables, hypTables map[cat.StableID]cat.Table) {
	numTables := len(indexCandidates)
	hypTables = make(map[cat.StableID]cat.Table, numTables)
	optTables = make(map[cat.StableID]cat.Table, numTables)

	for t, indexes := range indexCandidates {
		hypIndexes := make([]hypotheticalIndex, 0, len(indexes))
		var hypTable hypotheticalTable
		hypTable.init(t, existingIndexes[t], hypIndexes)

		indexOrd := t.IndexCount()
		for _, index := range indexes {
			// Do not add the index if it is equivalent to an existing index.
			if hypTable.indexExists(index) {
				continue
			}
			var hypIndex hypotheticalIndex
			hypIndex.init(
				&hypTable,
				tree.Name(fmt.Sprintf("_hyp_%d", indexOrd)),
				index,
				indexOrd,
				t.Zone().(*zonepb.ZoneConfig),
			)
			hypIndexes = append(hypIndexes, hypIndex)
			indexOrd++
		}

		hypTable.hypotheticalIndexes = hypIndexes
		optTables[t.ID()] = t
		hypTables[t.ID()] = &hypTable
	}

	return optTables, hypTables
}

// hypotheticalTable is a wrapper around cat.Table, used for creating index
// recommendations. The hypotheticalIndexes slice stores fake indexes that could
// potentially speed up queries to this table.
type hypotheticalTable struct {
	cat.Table
	primaryKeyColsOrdSet util.FastIntSet
	existingIndexes      [][]cat.IndexColumn
	hypotheticalIndexes  []hypotheticalIndex
}

var _ cat.Table = &hypotheticalTable{}

func (ht *hypotheticalTable) init(
	table cat.Table, existingIndexes [][]cat.IndexColumn, hypIndexes []hypotheticalIndex,
) {
	ht.Table = table
	ht.existingIndexes = existingIndexes
	ht.hypotheticalIndexes = hypIndexes

	// Get PK column ordinals.
	primaryIndex := ht.Index(cat.PrimaryIndex)
	numPrimaryKeyCols := primaryIndex.KeyColumnCount()
	for i := 0; i < numPrimaryKeyCols; i++ {
		ht.primaryKeyColsOrdSet.Add(primaryIndex.Column(i).Ordinal())
	}
}

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
	return &ht.hypotheticalIndexes[hypOrd]
}

// indexExists checks whether an index is present in the slice of
// existingIndexes containing a table's existing indexes.
func (ht *hypotheticalTable) indexExists(index []cat.IndexColumn) bool {
	for _, existingIndex := range ht.existingIndexes {
		if len(existingIndex) != len(index) {
			continue
		}
		indexExists := true
		for i := range existingIndex {
			if existingIndex[i].ColID() != index[i].ColID() ||
				existingIndex[i].Descending != index[i].Descending {
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
