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

package testutils

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// CreateTable creates a test table from a parsed DDL statement and
// adds it to the catalog.
func (c *TestCatalog) CreateTable(stmt *tree.CreateTable) *TestTable {
	tn, err := stmt.Table.Normalize()
	if err != nil {
		panic(fmt.Errorf("%s", err))
	}

	tbl := &TestTable{Name: tn.Table()}
	hasPrimaryKey := false
	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *tree.ColumnTableDef:
			if def.PrimaryKey {
				hasPrimaryKey = true
			}
			tbl.addColumn(def)
		}
		// TODO(rytaft): In the future we will likely want to check for unique
		// constraints, indexes, and foreign key constraints to determine
		// nullability, uniqueness, etc.
	}

	// If there is no primary key, add the hidden rowid column.
	if !hasPrimaryKey {
		rowid := &TestColumn{Name: "rowid", Type: types.Int, Hidden: true}
		tbl.Columns = append(tbl.Columns, rowid)
	}

	// Add the new table to the catalog.
	c.AddTable(tbl)

	return tbl
}

func (t *TestTable) addColumn(def *tree.ColumnTableDef) {
	nullable := !def.PrimaryKey && def.Nullable.Nullability != tree.NotNull
	typ := coltypes.CastTargetToDatumType(def.Type)
	col := &TestColumn{Name: string(def.Name), Type: typ, Nullable: nullable}
	t.Columns = append(t.Columns, col)
}
