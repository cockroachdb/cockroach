// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testcat

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// DropIndex is a partial implementation of the DROP INDEX statement.
//
// It only supports dropping a secondary index with an unqualified name.
func (tc *Catalog) DropIndex(stmt *tree.DropIndex) {
	for _, tableIndexName := range stmt.IndexList {
		indexName := tableIndexName.Index.String()

		var foundTab *Table
		var idxOrd int
		for _, tab := range tc.Tables() {
			if idx, ok := findIndex(tab, indexName); ok {
				if foundTab != nil {
					panic(errors.Newf(
						`index name "%s" is ambiguous; dropping ambiguous indexes is not supported in the test catalog`,
						indexName,
					))
				}
				foundTab = tab
				idxOrd = idx.Ordinal()
			}
		}

		if foundTab == nil {
			panic(errors.Newf(`index "%s" does not exist`, indexName))
		}

		if idxOrd == 0 {
			panic(errors.Newf("dropping primary indexes is not supported in the test catalog"))
		}

		// Delete the index from the table.
		var indexes *[]*Index
		switch {
		case idxOrd < foundTab.IndexCount():
			indexes = &foundTab.indexes
		case idxOrd < foundTab.WritableIndexCount():
			indexes = &foundTab.writeOnlyIndexes
			idxOrd = idxOrd - foundTab.IndexCount()
		default:
			indexes = &foundTab.deleteOnlyIndexes
			idxOrd = idxOrd - foundTab.WritableIndexCount()
		}
		numIndexes := len(*indexes)
		(*indexes)[idxOrd] = (*indexes)[numIndexes-1]
		*indexes = (*indexes)[:numIndexes-1]
	}
}

// findIndex returns the first index within tab that has an IdxName equal to
// name. If an index is found it returns the index and true, and nil and false
// otherwise.
func findIndex(tab *Table, name string) (*Index, bool) {
	for i, n := 0, tab.DeletableIndexCount(); i < n; i++ {
		tab.Index(i).Ordinal()
		idx := tab.Index(i).(*Index)
		if idx.IdxName == name {
			return idx, true
		}
	}
	return nil, false
}
