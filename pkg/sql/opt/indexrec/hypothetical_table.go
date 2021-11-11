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
)

// BuildHypotheticalTables builds a hypotheticalTable for each table in
// indexCandidates. This hypotheticalTable stores a hypothetical index for each
// of the table's index candidates.
func BuildHypotheticalTables(
	indexCandidates map[cat.Table][][]cat.IndexColumn,
	tableZones map[cat.Table]*zonepb.ZoneConfig,
	existingIndexes map[cat.StableID][][]cat.IndexColumn,
) (oldTables, hypTables map[cat.StableID]cat.Table) {
	hypTables = make(map[cat.StableID]cat.Table)
	oldTables = make(map[cat.StableID]cat.Table)

	for t, indexes := range indexCandidates {
		hypIndexes := make([]cat.Index, 0, len(indexes))
		hypTable := hypotheticalTable{Table: t, hypotheticalIndexes: hypIndexes}
		count := t.IndexCount()

		for _, index := range indexes {
			// Do not add the index if it is equivalent to an existing index.
			if indexOnTableExists(index, existingIndexes[t.ID()]) {
				continue
			}
			idx := hypotheticalIndex{
				tab:          &hypTable,
				name:         tree.Name(fmt.Sprintf("_hyp_%d", count)),
				indexOrdinal: count,
				cols:         index,
				zone:         tableZones[t],
			}
			hypIndexes = append(hypIndexes, &idx)
			count++
		}

		hypTable.hypotheticalIndexes = hypIndexes
		oldTables[t.ID()] = t
		hypTables[t.ID()] = &hypTable
	}

	return oldTables, hypTables
}

// indexOnTableExists checks whether an index is present in the slice of
// existingIndexes containing a table's existing indexes.
func indexOnTableExists(index []cat.IndexColumn, existingIndexes [][]cat.IndexColumn) bool {
	for i, n := 0, len(existingIndexes); i < n; i++ {
		existingIndex := existingIndexes[i]
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
	return ht.hypotheticalIndexes[i-existingIndexCount]
}
