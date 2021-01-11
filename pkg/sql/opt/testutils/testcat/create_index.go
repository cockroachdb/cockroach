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
	tab := tc.Table(&tn)

	for _, idx := range tab.Indexes {
		in := stmt.Name.String()
		if idx.IdxName == in {
			panic(errors.Newf(`relation "%s" already exists`, in))
		}
	}

	// Convert stmt to a tree.IndexTableDef so that Table.addIndex can be used
	// to add the index to the table.
	indexTableDef := &tree.IndexTableDef{
		Name:             stmt.Name,
		Columns:          stmt.Columns,
		Sharded:          stmt.Sharded,
		Storing:          stmt.Storing,
		Interleave:       stmt.Interleave,
		Inverted:         stmt.Inverted,
		PartitionByIndex: stmt.PartitionByIndex,
		Predicate:        stmt.Predicate,
	}

	idxType := nonUniqueIndex
	if stmt.Unique {
		idxType = uniqueIndex

	}
	tab.addIndexWithVersion(indexTableDef, idxType, version)
}
