// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
		Type:             stmt.Type,
		PartitionByIndex: stmt.PartitionByIndex,
		Predicate:        stmt.Predicate,
		Invisibility:     stmt.Invisibility,
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
