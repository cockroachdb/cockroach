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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

type indexType int

const (
	primaryIndex indexType = iota
	uniqueIndex
	nonUniqueIndex
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
	tab := &TestTable{Name: tn.Table()}
	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *tree.ColumnTableDef:
			tab.addColumn(def)

		case *tree.UniqueConstraintTableDef:
			if def.PrimaryKey {
				tab.addIndex(&def.IndexTableDef, primaryIndex)
			}
		}
	}

	// If there is no primary index, add the hidden rowid column.
	if len(tab.Indexes) == 0 {
		rowid := &TestColumn{Name: "rowid", Type: types.Int, Hidden: true}
		tab.Columns = append(tab.Columns, rowid)
		tab.addPrimaryColumnIndex(rowid)
	}

	// Search for other relevant definitions.
	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *tree.UniqueConstraintTableDef:
			if !def.PrimaryKey {
				tab.addIndex(&def.IndexTableDef, uniqueIndex)
			}

		case *tree.IndexTableDef:
			tab.addIndex(def, nonUniqueIndex)
		}
		// TODO(rytaft): In the future we will likely want to check for unique
		// constraints, indexes, and foreign key constraints to determine
		// nullability, uniqueness, etc.
	}

	// Add the new table to the catalog.
	tc.AddTable(tab)

	return tab
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
		col := idx.addColumn(tt, string(colDef.Column), colDef.Direction, true /* makeUnique */)

		if typ == primaryIndex {
			col.Nullable = false
		}
	}

	if typ != primaryIndex {
		// Add implicit key columns from primary index.
		for _, idxCol := range tt.Indexes[opt.PrimaryIndex].Columns {
			// Only add columns that aren't already part of index.
			found := false
			for _, colDef := range def.Columns {
				if idxCol.Column.ColName() == opt.ColumnName(colDef.Column) {
					found = true
				}
			}

			if !found {
				// Implicit column is only part of the index's set of unique columns
				// if the index *was not* declared as unique in the first place. The
				// implicit columns are added to make the index unique (as well as
				// to "cover" the primary index for lookups).
				name := string(idxCol.Column.ColName())
				makeUnique := typ != uniqueIndex
				idx.addColumn(tt, name, tree.Ascending, makeUnique)
			}
		}
	}

	// Add storing columns.
	for _, name := range def.Storing {
		// Only add storing columns that weren't added as part of adding implicit
		// key columns.
		found := false
		for _, idxCol := range tt.Indexes[opt.PrimaryIndex].Columns {
			if opt.ColumnName(name) == idxCol.Column.ColName() {
				found = true
			}
		}

		if !found {
			idx.addColumn(tt, string(name), tree.Ascending, false)
		}
	}

	if typ == primaryIndex {
		if len(tt.Indexes) != 0 {
			panic("primary index should always be 0th index")
		}
	}
	tt.Indexes = append(tt.Indexes, idx)
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
	tt *TestTable, name string, direction tree.Direction, makeUnique bool,
) *TestColumn {
	ord := tt.FindOrdinal(name)
	col := tt.Column(ord)
	idxCol := opt.IndexColumn{
		Column:     col,
		Ordinal:    ord,
		Descending: direction == tree.Descending,
	}
	ti.Columns = append(ti.Columns, idxCol)
	if makeUnique {
		// Need to add to the index's count of columns that are part of its
		// unique key.
		ti.Unique++
	}
	return col.(*TestColumn)
}

func (tt *TestTable) addPrimaryColumnIndex(col *TestColumn) {
	idxCol := opt.IndexColumn{
		Column:  col,
		Ordinal: tt.FindOrdinal(col.Name),
	}
	tt.Indexes = append(
		tt.Indexes,
		&TestIndex{
			Name:    "primary",
			Columns: []opt.IndexColumn{idxCol},
			Unique:  1,
		},
	)
}
