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
		hypTable.init(t, hypIndexes)

		for i, index := range indexes {
			indexOrd := i + 1
			var hypIndex hypotheticalIndex
			hypIndex.init(
				&hypTable,
				tree.Name(fmt.Sprintf("_hyp_%d", indexOrd)),
				index,
				indexOrd,
				t.Zone().(*zonepb.ZoneConfig),
			)
			hypIndexes = append(hypIndexes, hypIndex)
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
	primaryKeyColsOrdSet util.FastIntSet
	hypotheticalIndexes  []hypotheticalIndex
}

var _ cat.Table = &HypotheticalTable{}

func (ht *HypotheticalTable) init(table cat.Table, hypIndexes []hypotheticalIndex) {
	ht.Table = table
	ht.hypotheticalIndexes = hypIndexes

	// Get PK column ordinals.
	primaryIndex := ht.Index(cat.PrimaryIndex)
	numPrimaryKeyCols := primaryIndex.KeyColumnCount()
	for i := 0; i < numPrimaryKeyCols; i++ {
		ht.primaryKeyColsOrdSet.Add(primaryIndex.Column(i).Ordinal())
	}
}

// IndexCount is part of the cat.Table interface.
func (ht *HypotheticalTable) IndexCount() int {
	// A HypotheticalTable stores the embedded table's primary index in addition
	// to its hypothetical indexes.
	return len(ht.hypotheticalIndexes) + 1
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
	if i == cat.PrimaryIndex {
		return ht.Table.Index(cat.PrimaryIndex)
	}
	return &ht.hypotheticalIndexes[i-1]
}

// existingRedundantIndex checks whether an index with the same explicit columns
// as the index argument is present in the HypotheticalTable's embedded table.
// If so, it returns the first instance of such an existing index. Otherwise, it
// returns nil.
func (ht *HypotheticalTable) existingRedundantIndex(index *hypotheticalIndex) cat.Index {
	for i, n := 0, ht.Table.IndexCount(); i < n; i++ {
		indexCols := index.cols
		existingIndex := ht.Table.Index(i)
		if existingIndex.ExplicitColumnCount() != len(indexCols) {
			continue
		}
		indexExists := true
		for j, m := 0, existingIndex.ExplicitColumnCount(); j < m; j++ {
			indexCol := existingIndex.Column(j)
			if indexCol != indexCols[j] {
				indexExists = false
				break
			}
		}
		if indexExists {
			return existingIndex
		}
	}
	return nil
}
