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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
)

func TestBuildOptAndHypTableMaps(t *testing.T) {
	tables, indexCols := testTablesAndIndexCols()
	table1 := tables[0]
	table2 := tables[1]
	indexCandidates := testIndexCandidates1(tables, indexCols)
	existingIndexes := testExistingIndexes(tables, indexCols)

	oldTables, hypTables := BuildOptAndHypTableMaps(
		indexCandidates, existingIndexes,
	)

	if oldTables[table1.ID()] != table1 {
		t.Errorf("expected table1 to be %+v,\n got %+v\n", table1, oldTables[table1.ID()])
	}

	if oldTables[table2.ID()] != table2 {
		t.Errorf("expected table2 to be %+v,\n got %+v\n", table2, oldTables[table2.ID()])
	}

	// Both existing indexes are also index candidates.
	newIndexesTable1 := len(indexCandidates[table1]) - 2

	// Only one existing index is an index candidate.
	newIndexesTable2 := len(indexCandidates[table2]) - 1

	if hypTables[1].IndexCount()-oldTables[1].IndexCount() != newIndexesTable1 {
		t.Errorf(
			"expected table1's index count to be %d, got %d\n",
			hypTables[1].IndexCount()-oldTables[1].IndexCount(),
			newIndexesTable1,
		)
	}

	if hypTables[2].IndexCount()-oldTables[2].IndexCount() != newIndexesTable2 {
		t.Errorf("expected table2's index count to be %d, got %d\n",
			hypTables[2].IndexCount()-oldTables[2].IndexCount(),
			newIndexesTable2,
		)
	}
}

func TestBuildOptAndHypTableMaps_NoExistingIndexes(t *testing.T) {
	tables, indexCols := testTablesAndIndexCols()
	table1 := tables[0]
	table2 := tables[1]
	indexCandidates := testIndexCandidates1(tables, indexCols)

	oldTables, hypTables := BuildOptAndHypTableMaps(
		indexCandidates, make(map[cat.Table][][]cat.IndexColumn),
	)

	if oldTables[table1.ID()] != table1 {
		t.Errorf("expected table1 to be %+v,\n got %+v\n", table1, oldTables[table1.ID()])
	}

	if oldTables[table2.ID()] != table2 {
		t.Errorf("expected table2 to be %+v,\n got %+v\n", table2, oldTables[table2.ID()])
	}

	newIndexesTable1 := len(indexCandidates[table1])
	newIndexesTable2 := len(indexCandidates[table2])

	if hypTables[1].IndexCount()-oldTables[1].IndexCount() != newIndexesTable1 {
		t.Errorf(
			"expected table1's index count to be %d, got %d\n",
			hypTables[1].IndexCount()-oldTables[1].IndexCount(),
			newIndexesTable1,
		)
	}

	if hypTables[2].IndexCount()-oldTables[2].IndexCount() != newIndexesTable2 {
		t.Errorf("expected table2's index count to be %d, got %d\n",
			hypTables[2].IndexCount()-oldTables[2].IndexCount(),
			newIndexesTable2,
		)
	}
}

func testExistingIndexes(
	tables []cat.Table, indexCols []cat.IndexColumn,
) map[cat.Table][][]cat.IndexColumn {
	existingIndexes := make(map[cat.Table][][]cat.IndexColumn)
	table1 := tables[0]
	table2 := tables[1]
	existingIndexes[table1] = [][]cat.IndexColumn{{indexCols[1]}, {indexCols[0], indexCols[1]}}
	existingIndexes[table2] = [][]cat.IndexColumn{{indexCols[0]}, {indexCols[1], indexCols[0]}}
	return existingIndexes
}
