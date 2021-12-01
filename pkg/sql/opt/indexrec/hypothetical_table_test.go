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

import "testing"

func TestBuildOptAndHypTableMaps(t *testing.T) {
	tables, indexCols := testTablesAndIndexCols()
	table1 := tables[0]
	table2 := tables[1]
	indexCandidates := testIndexCandidates1(tables, indexCols)

	oldTables, hypTables := BuildOptAndHypTableMaps(indexCandidates)

	if oldTables[table1.ID()] != table1 {
		t.Errorf("expected table1 to be %+v,\n got %+v\n", table1, oldTables[table1.ID()])
	}

	if oldTables[table2.ID()] != table2 {
		t.Errorf("expected table2 to be %+v,\n got %+v\n", table2, oldTables[table2.ID()])
	}

	// One index candidate is an existing index.
	newIndexesTable1 := len(indexCandidates[table1]) - 1

	// Two index candidates are existing indexes.
	newIndexesTable2 := len(indexCandidates[table2]) - 2

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
