// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package indexrec

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
)

// testIndexCandidatesWithPredicates creates IndexCandidates for testing,
// simulating the new structure returned by FindIndexCandidateSet.
func testIndexCandidatesWithPredicates(
	tables []cat.Table, indexCols []cat.IndexColumn,
) map[cat.Table][]IndexCandidates {
	// Convert the old format to new format with empty predicates
	oldCandidates := testIndexCandidates1(tables, indexCols)
	newCandidates := make(map[cat.Table][]IndexCandidates)

	for table, indexes := range oldCandidates {
		candidates := make([]IndexCandidates, len(indexes))
		for i, indexColumns := range indexes {
			candidates[i] = IndexCandidates{
				columns:   indexColumns,
				predicate: "",
			}
		}
		newCandidates[table] = candidates
	}
	return newCandidates
}

func TestBuildOptAndHypTableMaps(t *testing.T) {
	tables, indexCols := testTablesAndIndexCols()
	table1 := tables[0]
	table2 := tables[1]
	indexCandidates := testIndexCandidatesWithPredicates(tables, indexCols)

	oldTables, hypTables := BuildOptAndHypTableMaps(nil, indexCandidates)

	if oldTables[table1.ID()] != table1 {
		t.Errorf("expected table1 to be %+v,\n got %+v\n", table1, oldTables[table1.ID()])
	}

	if oldTables[table2.ID()] != table2 {
		t.Errorf("expected table2 to be %+v,\n got %+v\n", table2, oldTables[table2.ID()])
	}

	// A hypothetical table's index count is equivalent to its number of index
	// candidates plus the number of existing indexes.
	indexCountTable1 := len(indexCandidates[table1]) + table1.IndexCount()
	indexCountTable2 := len(indexCandidates[table2]) + table2.IndexCount()

	if hypTables[1].IndexCount() != indexCountTable1 {
		t.Errorf(
			"expected table1's index count to be %d, got %d\n",
			hypTables[1].IndexCount(),
			indexCountTable1,
		)
	}

	if hypTables[2].IndexCount() != indexCountTable2 {
		t.Errorf("expected table2's index count to be %d, got %d\n",
			hypTables[2].IndexCount(),
			indexCountTable2,
		)
	}
}
