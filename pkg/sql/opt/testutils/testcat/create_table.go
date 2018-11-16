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

var uniqueRowIDString = "unique_rowid()"

// CreateTable creates a test table from a parsed DDL statement and adds it to
// the catalog. This is intended for testing, and is not a complete (and
// probably not fully correct) implementation. It just has to be "good enough".
func (tc *Catalog) CreateTable(stmt *tree.CreateTable) *Table {
	stmt.HoistConstraints()

	// Update the table name to include catalog and schema if not provided.
	tc.qualifyTableName(&stmt.Table)

	tab := &Table{TabFingerprint: tc.nextFingerprint(), TabName: stmt.Table, Catalog: tc}

	// Assume that every table in the "system" or "information_schema" catalog
	// is a virtual table. This is a simplified assumption for testing purposes.
	if stmt.Table.CatalogName == "system" || stmt.Table.SchemaName == "information_schema" {
		tab.IsVirtual = true
	}

	// Add columns.
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
		rowid := &Column{
			Name:        "rowid",
			Type:        types.Int,
			Hidden:      true,
			DefaultExpr: &uniqueRowIDString,
		}
		tab.Columns = append(tab.Columns, rowid)
		tab.addPrimaryColumnIndex(rowid.Name)
	}

	// Search for index definitions.
	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *tree.UniqueConstraintTableDef:
			if !def.PrimaryKey {
				tab.addIndex(&def.IndexTableDef, uniqueIndex)
			}

		case *tree.IndexTableDef:
			tab.addIndex(def, nonUniqueIndex)
		}
	}

	// Search for foreign key constraints. We want to process them after first
	// processing all the indexes (otherwise the foreign keys could add
	// unnecessary indexes).
	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *tree.ForeignKeyConstraintTableDef:
			tc.resolveFK(tab, def)
		}
	}

	// We need to keep track of the tableID from numeric references. 53 is a magic
	// number derived from how CRDB internally stores tables. The first user table
	// is 53. This magic number is used to have tests look consistent.
	tab.tableID = sqlbase.ID(len(tc.dataSources) + 53)

	// Add the new table to the catalog.
	tc.AddTable(tab)

	return tab
}

// resolveFK processes a foreign key constraint
func (tc *Catalog) resolveFK(tab *Table, d *tree.ForeignKeyConstraintTableDef) {
	fromCols := make([]int, len(d.FromCols))
	for i, c := range d.FromCols {
		fromCols[i] = tab.FindOrdinal(string(c))
	}

	targetTable := tc.Table(&d.Table)

	toCols := make([]int, len(d.ToCols))
	for i, c := range d.ToCols {
		toCols[i] = targetTable.FindOrdinal(string(c))
	}

	// Foreign keys require indexes in both tables:
	//
	//  1. In the target table, we need an index because adding a new row to the
	//     source table requires looking up whether there is a matching value in
	//     the target table. This index should already exist because a unique
	//     constraint is required on the target table (it's a foreign *key*).
	//
	//  2. In the source table, we need an index because removing a row from the
	//     target table requires looking up whether there would be orphan values
	//     left in the source table. This index does not need to be unique; in
	//     fact, if an existing index has the relevant columns as a prefix, that
	//     is good enough.

	// matches returns true if the key columns in the given index match the given
	// columns. If strict is false, it is acceptable if the given columns are a
	// prefix of the index key columns.
	matches := func(idx *Index, cols []int, strict bool) bool {
		if idx.KeyColumnCount() < len(cols) {
			return false
		}
		if strict && idx.KeyColumnCount() > len(cols) {
			return false
		}
		for i := range cols {
			if idx.Column(i).Ordinal != cols[i] {
				return false
			}
		}
		return true
	}

	// 1. Verify that the target table has a unique index.
	var targetIndex *Index
	for _, idx := range targetTable.Indexes {
		if matches(idx, toCols, true /* strict */) {
			targetIndex = idx
			break
		}
	}
	if targetIndex == nil {
		panic(fmt.Errorf(
			"there is no unique constraint matching given keys for referenced table %s",
			targetTable.Name(),
		))
	}

	// 2. Search for an existing index in the source table; add it if necessary.
	found := false
	for _, idx := range tab.Indexes {
		if matches(idx, fromCols, false /* strict */) {
			found = true
			idx.foreignKey.TableID = targetTable.InternalID()
			idx.foreignKey.IndexID = targetIndex.InternalID()
			idx.foreignKey.PrefixLen = int32(len(fromCols))
			idx.fkSet = true
			break
		}
	}
	if !found {
		// Add a non-unique index on fromCols.
		constraintName := string(d.Name)
		if constraintName == "" {
			constraintName = fmt.Sprintf(
				"fk_%s_ref_%s", string(d.FromCols[0]), targetTable.TabName.Table(),
			)
		}
		idx := tree.IndexTableDef{
			Name:    tree.Name(fmt.Sprintf("%s_auto_index_%s", tab.TabName.Table(), constraintName)),
			Columns: make(tree.IndexElemList, len(fromCols)),
		}
		for i, c := range fromCols {
			idx.Columns[i].Column = tab.Columns[c].ColName()
			idx.Columns[i].Direction = tree.Ascending
		}
		index := tab.addIndex(&idx, nonUniqueIndex)
		index.foreignKey.TableID = targetTable.InternalID()
		index.foreignKey.IndexID = targetIndex.InternalID()
		index.foreignKey.PrefixLen = int32(len(fromCols))
		index.fkSet = true
	}
}

func (tt *Table) addColumn(def *tree.ColumnTableDef) {
	nullable := !def.PrimaryKey && def.Nullable.Nullability != tree.NotNull
	typ := coltypes.CastTargetToDatumType(def.Type)
	col := &Column{Name: string(def.Name), Type: typ, Nullable: nullable}

	if def.DefaultExpr.Expr != nil {
		s := tree.Serialize(def.DefaultExpr.Expr)
		col.DefaultExpr = &s
	}

	if def.Computed.Expr != nil {
		s := tree.Serialize(def.Computed.Expr)
		col.ComputedExpr = &s
	}

	// Add mutation columns to the Mutations list.
	if col.IsMutation() {
		tt.Mutations = append(tt.Mutations, col)
	} else {
		tt.Columns = append(tt.Columns, col)
	}
}

func (tt *Table) addIndex(def *tree.IndexTableDef, typ indexType) *Index {
	idx := &Index{
		Name:     tt.makeIndexName(def.Name, typ),
		Inverted: def.Inverted,
		table:    tt,
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
		return idx
	}

	// Add implicit key columns from primary index.
	pkCols := tt.Indexes[opt.PrimaryIndex].Columns[:tt.Indexes[opt.PrimaryIndex].KeyCount]
	for _, pkCol := range pkCols {
		// Only add columns that aren't already part of index.
		found := false
		for _, colDef := range def.Columns {
			if pkCol.Column.ColName() == colDef.Column {
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
			if name == pkCol.Column.ColName() {
				found = true
			}
		}
		if !found {
			idx.addColumn(tt, string(name), tree.Ascending, nonKeyCol)
		}
	}

	idx.Ordinal = len(tt.Indexes)
	tt.Indexes = append(tt.Indexes, idx)

	return idx
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
