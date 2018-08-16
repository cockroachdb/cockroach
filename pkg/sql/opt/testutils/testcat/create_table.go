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

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

type indexType int

const (
	primaryIndex indexType = iota
	uniqueIndex
	nonUniqueIndex
)

type colType int

const (
	// keyCol is part of both lax and strict keys.
	keyCol colType = iota
	// strictKeyCol is only part of strict key.
	strictKeyCol
	// nonKeyCol is not part of lax or strict key.
	nonKeyCol
)

// CreateTable creates a test table from a parsed DDL statement and adds it to
// the catalog. This is intended for testing, and is not a complete (and
// probably not fully correct) implementation. It just has to be "good enough".
func (tc *Catalog) CreateTable(stmt *tree.CreateTable) *Table {
	tn, err := stmt.Table.Normalize()
	if err != nil {
		panic(fmt.Errorf("%s", err))
	}

	// Update the table name to include catalog and schema if not provided.
	tc.qualifyTableName(tn)

	tab := &Table{TabName: *tn}

	// Assume that every table in the "system" catalog is a virtual table. This
	// is a simplified assumption for testing purposes.
	if tn.CatalogName == "system" {
		tab.IsVirtual = true
	}

	// Add the columns.
	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *tree.ColumnTableDef:
			tab.addColumn(def)
		}
	}

	// Add the primary index (if there is one defined).
	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *tree.ColumnTableDef:
			if def.PrimaryKey {
				// Add the primary index over the single column.
				tab.addPrimaryColumnIndex(string(def.Name))
			}

		case *tree.UniqueConstraintTableDef:
			if def.PrimaryKey {
				tab.addIndex(&def.IndexTableDef, primaryIndex)
			}
		}
	}

	// If there is no primary index, add the hidden rowid column.
	if len(tab.Indexes) == 0 && !tab.IsVirtual {
		rowid := &Column{Name: "rowid", Type: types.Int, Hidden: true}
		tab.Columns = append(tab.Columns, rowid)
		tab.addPrimaryColumnIndex(rowid.Name)
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

		// TODO(rytaft): In the future we will likely want to check for foreign key
		// constraints.
	}

	// We need to keep track of the tableID from numeric references. 53 is a magic
	// number derived from how CRDB internally stores tables. The first user table
	// is 53. This magic number is used to have tests look consistent.
	tab.tableID = sqlbase.ID(len(tc.dataSources) + 53)
	// Add the new table to the catalog.
	tc.AddTable(tab)

	return tab
}

func (tt *Table) addColumn(def *tree.ColumnTableDef) {
	nullable := !def.PrimaryKey && def.Nullable.Nullability != tree.NotNull
	typ := coltypes.CastTargetToDatumType(def.Type)
	col := &Column{Name: string(def.Name), Type: typ, Nullable: nullable}
	tt.Columns = append(tt.Columns, col)
}

func (tt *Table) addIndex(def *tree.IndexTableDef, typ indexType) {
	idx := &Index{
		Name:     tt.makeIndexName(def.Name, typ),
		Inverted: def.Inverted,
	}

	// Add explicit columns and mark primary key columns as not null.
	notNullIndex := true
	for _, colDef := range def.Columns {
		col := idx.addColumn(tt, string(colDef.Column), colDef.Direction, keyCol)

		if typ == primaryIndex {
			col.Nullable = false
		}

		if col.Nullable {
			notNullIndex = false
		}
	}

	if typ == primaryIndex {
		var pkOrdinals util.FastIntSet
		for _, c := range idx.Columns {
			pkOrdinals.Add(c.Ordinal)
		}
		// Add the rest of the columns in the table.
		for i := range tt.Columns {
			if !pkOrdinals.Contains(i) {
				idx.addColumnByOrdinal(tt, i, tree.Ascending, nonKeyCol)
			}
		}
		if len(tt.Indexes) != 0 {
			panic("primary index should always be 0th index")
		}
		idx.Ordinal = len(tt.Indexes)
		tt.Indexes = append(tt.Indexes, idx)
		return
	}

	// Add implicit key columns from primary index.
	pkCols := tt.Indexes[opt.PrimaryIndex].Columns[:tt.Indexes[opt.PrimaryIndex].KeyCount]
	for _, pkCol := range pkCols {
		// Only add columns that aren't already part of index.
		found := false
		for _, colDef := range def.Columns {
			if pkCol.Column.ColName() == opt.ColumnName(colDef.Column) {
				found = true
			}
		}

		if !found {
			name := string(pkCol.Column.ColName())

			if typ == uniqueIndex {
				// If unique index has no NULL columns, then the implicit columns
				// are added as storing columns. Otherwise, they become part of the
				// strict key, since they're needed to ensure uniqueness (but they
				// are not part of the lax key).
				if notNullIndex {
					idx.addColumn(tt, name, tree.Ascending, nonKeyCol)
				} else {
					idx.addColumn(tt, name, tree.Ascending, strictKeyCol)
				}
			} else {
				// Implicit columns are always added to the key for a non-unique
				// index. In addition, there is no separate lax key, so the lax
				// key column count = key column count.
				idx.addColumn(tt, name, tree.Ascending, keyCol)
			}
		}
	}

	// Add storing columns.
	for _, name := range def.Storing {
		// Only add storing columns that weren't added as part of adding implicit
		// key columns.
		found := false
		for _, pkCol := range pkCols {
			if opt.ColumnName(name) == pkCol.Column.ColName() {
				found = true
			}
		}
		if !found {
			idx.addColumn(tt, string(name), tree.Ascending, nonKeyCol)
		}
	}

	idx.Ordinal = len(tt.Indexes)
	tt.Indexes = append(tt.Indexes, idx)
}

func (tt *Table) makeIndexName(defName tree.Name, typ indexType) string {
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

func (ti *Index) addColumn(
	tt *Table, name string, direction tree.Direction, colType colType,
) *Column {
	return ti.addColumnByOrdinal(tt, tt.FindOrdinal(name), direction, colType)
}

func (ti *Index) addColumnByOrdinal(
	tt *Table, ord int, direction tree.Direction, colType colType,
) *Column {
	col := tt.Column(ord)
	idxCol := opt.IndexColumn{
		Column:     col,
		Ordinal:    ord,
		Descending: direction == tree.Descending,
	}
	ti.Columns = append(ti.Columns, idxCol)

	// Update key column counts.
	switch colType {
	case keyCol:
		// Column is part of both any lax key, as well as the strict key.
		ti.LaxKeyCount++
		ti.KeyCount++

	case strictKeyCol:
		// Column is only part of the strict key.
		ti.KeyCount++
	}
	return col.(*Column)
}

func (tt *Table) addPrimaryColumnIndex(colName string) {
	def := tree.IndexTableDef{
		Columns: tree.IndexElemList{{Column: tree.Name(colName), Direction: tree.Ascending}},
	}
	tt.addIndex(&def, primaryIndex)
}
