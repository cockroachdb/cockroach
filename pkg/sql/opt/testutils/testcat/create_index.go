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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// CreateIndex is a partial implementation of the CREATE INDEX statement.
func (tc *Catalog) CreateIndex(stmt *tree.CreateIndex, version descpb.IndexDescriptorVersion) {
	tn := stmt.Table
	// Update the table name to include catalog and schema if not provided.
	tc.qualifyTableName(&tn)
	tab, err := tc.LookupTable(&tn)
	var view *View
	if err == nil {
		for _, idx := range tab.Indexes {
			in := stmt.Name.String()
			if idx.IdxName == in {
				panic(errors.Newf(`relation "%s" already exists`, in))
			}
		}
	} else {
		view = tc.View(&tn)
	}

	// Convert stmt to a tree.IndexTableDef so that Table.addIndex can be used
	// to add the index to the table.
	indexTableDef := &tree.IndexTableDef{
		Name:             stmt.Name,
		Columns:          stmt.Columns,
		Sharded:          stmt.Sharded,
		Storing:          stmt.Storing,
		Inverted:         stmt.Inverted,
		PartitionByIndex: stmt.PartitionByIndex,
		Predicate:        stmt.Predicate,
		NotVisible:       stmt.NotVisible,
	}

	idxType := nonUniqueIndex
	if stmt.Unique {
		idxType = uniqueIndex

	}
	if tab != nil {
		tab.addIndexWithVersion(indexTableDef, idxType, version)
	} else if view != nil {
		view.addIndex(indexTableDef)
	}
}
