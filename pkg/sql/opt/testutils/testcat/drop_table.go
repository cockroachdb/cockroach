// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package testcat

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// DropTable is a partial implementation of the DROP TABLE statement.
func (tc *Catalog) DropTable(stmt *tree.DropTable) {
	for i := range stmt.Names {
		tn := &stmt.Names[i]

		// Update the table name to include catalog and schema if not provided.
		tc.qualifyTableName(tn)

		// Ensure that table with that name exists.
		tc.Table(tn)

		// Remove the table from the catalog.
		delete(tc.testSchema.dataSources, tn.FQString())
	}
}
