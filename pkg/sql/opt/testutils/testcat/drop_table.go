// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package testcat

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// DropTable is a partial implementation of the DROP TABLE statement.
func (tc *Catalog) DropTable(stmt *tree.DropTable) {
	for _, ntn := range stmt.Names {
		tn, err := ntn.Normalize()
		if err != nil {
			panic(err)
		}

		// Update the table name to include catalog and schema if not provided.
		tc.qualifyTableName(tn)
		fq := tn.FQString()
		if _, ok := tc.tables[fq]; !ok {
			panic(fmt.Sprintf("cannot find table %q", tree.ErrString(tn)))
		}

		// Remove the table from the catalog.
		delete(tc.tables, fq)
	}
}
