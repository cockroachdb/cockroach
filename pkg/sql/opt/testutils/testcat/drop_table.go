// Copyright 2018 The Cockroach Authors.
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

// DropTable is a partial implementation of the DROP TABLE statement.
func (tc *Catalog) DropTable(stmt *tree.DropTable) {
	for i := range stmt.Names {
		tn := &stmt.Names[i]

		// Update the table name to include catalog and schema if not provided.
		tc.qualifyTableName(tn)

		// Ensure that table with that name exists.
		t := tc.Table(tn)

		// Clean up FKs from tables referenced by t.
		for _, fk := range t.outboundFKs {
			for _, ds := range tc.testSchema.dataSources {
				if ds.ID() == fk.referencedTableID {
					ref := ds.(*Table)
					oldFKs := ref.inboundFKs
					ref.inboundFKs = nil
					for i := range oldFKs {
						if oldFKs[i].originTableID != t.ID() {
							ref.inboundFKs = append(ref.inboundFKs, oldFKs[i])
						}
					}
					break
				}
			}
		}

		if len(t.inboundFKs) > 0 {
			panic(errors.Newf("table %s is referenced by FK constraints", tn))
		}

		// Remove the table from the catalog.
		delete(tc.testSchema.dataSources, tn.FQString())
	}
}
