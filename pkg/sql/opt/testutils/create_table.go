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
	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

type indexType int

const (
	primaryIndex indexType = iota
	uniqueIndex
	nonUniqueIndex
)

const (
	isStoring    = true
	isNotStoring = false
)

// CreateTable creates a test table from a parsed DDL statement and adds it to
// the catalog. This is intended for testing, and is not a complete (and
// probably not fully correct) implementation. It just has to be "good enough".
func (tc *TestCatalog) CreateTable(stmt *tree.CreateTable) *TestTable {
	tn, err := stmt.Table.Normalize()
	if err != nil {
		panic(fmt.Errorf("%s", err))
	}

	// Add the columns and primary index (if there is one defined).
	tbl := &TestTable{Name: tn.Table()}
	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *tree.ColumnTableDef:
			tbl.addColumn(def)

		case *tree.UniqueConstraintTableDef:
			if def.PrimaryKey {
				tbl.addIndex(&def.IndexTableDef, primaryIndex)
			}
		}
	}

	// If there is no primary index, add the hidden rowid column.
	if tbl.PrimaryIndex == nil {
		rowid := &TestColumn{Name: "rowid", Type: types.Int, Hidden: true}
		tbl.Columns = append(tbl.Columns, rowid)
		tbl.addPrimaryColumnIndex(rowid)
	}

	// Search for other relevant definitions.
	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *tree.UniqueConstraintTableDef:
			if !def.PrimaryKey {
				tbl.addIndex(&def.IndexTableDef, uniqueIndex)
			}

		case *tree.IndexTableDef:
			tbl.addIndex(def, nonUniqueIndex)
		}
		// TODO(rytaft): In the future we will likely want to check for unique
		// constraints, indexes, and foreign key constraints to determine
		// nullability, uniqueness, etc.
	}

	// Add the new table to the catalog.
	tc.AddTable(tbl)

	return tbl
}

func (tt *TestTable) addColumn(def *tree.ColumnTableDef) {
	nullable := !def.PrimaryKey && def.Nullable.Nullability != tree.NotNull
	typ := coltypes.CastTargetToDatumType(def.Type)
	col := &TestColumn{Name: string(def.Name), Type: typ, Nullable: nullable}
	tt.Columns = append(tt.Columns, col)

	if def.PrimaryKey {
		// Add the primary index over the single column.
		tt.addPrimaryColumnIndex(col)
	}
}

func (tt *TestTable) addIndex(def *tree.IndexTableDef, typ indexType) {
	idx := &TestIndex{Name: tt.makeIndexName(def.Name, typ)}

	// Add explicit columns and mark key columns as not null.
	for _, colDef := range def.Columns {
		col := idx.addColumn(tt, string(colDef.Column), colDef.Direction, isNotStoring)

		if typ == primaryIndex {
			col.Nullable = false
		}
	}

	if typ != primaryIndex {
		// Add implicit key columns from primary index.
		for _, idxCol := range tt.PrimaryIndex.Columns {
			found := false
			for _, colDef := range def.Columns {
				if idxCol.Column.ColName() == optbase.ColumnName(colDef.Column) {
					found = true
				}
			}
			for _, name := range def.Storing {
				if idxCol.Column.ColName() == optbase.ColumnName(name) {
					found = true
				}
			}

			if !found {
				// Implicit column is stored if index columns are already unique.
				name := string(idxCol.Column.ColName())
				idx.addColumn(tt, name, tree.Ascending, typ == uniqueIndex)
			}
		}
	}

	// Add storing columns.
	for _, name := range def.Storing {
		idx.addColumn(tt, string(name), tree.Ascending, isStoring)
	}

	if typ == primaryIndex {
		tt.PrimaryIndex = idx
	} else {
		tt.SecondaryIndexes = append(tt.SecondaryIndexes, idx)
	}
}

func (tt *TestTable) makeIndexName(defName tree.Name, typ indexType) string {
	name := string(defName)
	if name == "" {
		if typ == primaryIndex {
			name = "primary"
		} else {
			name = "secondary"
		}
	}
	return name
}

func (ti *TestIndex) addColumn(
	tt *TestTable, name string, direction tree.Direction, storing bool,
) *TestColumn {
	ord := tt.FindOrdinal(name)
	col := tt.Column(ord)
	idxCol := optbase.IndexColumn{
		Column:     col,
		Ordinal:    ord,
		Descending: direction == tree.Descending,
	}
	ti.Columns = append(ti.Columns, idxCol)
	if storing {
		ti.Storing++
	}
	return col.(*TestColumn)
}

func (tt *TestTable) addPrimaryColumnIndex(col *TestColumn) {
	idxCol := optbase.IndexColumn{Column: col}
	tt.PrimaryIndex = &TestIndex{Name: "primary", Columns: []optbase.IndexColumn{idxCol}}
}
