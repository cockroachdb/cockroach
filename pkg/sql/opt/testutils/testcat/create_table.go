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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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

	// Assume that every table in the "system" or "information_schema" catalog
	// is a virtual table. This is a simplified assumption for testing purposes.
	if stmt.Table.CatalogName == "system" || stmt.Table.SchemaName == "information_schema" {
		return tc.createVirtualTable(stmt)
	}

	tab := &Table{TabID: tc.nextStableID(), TabName: stmt.Table, Catalog: tc}

	// TODO(andyk): For now, just remember that the table was interleaved. In the
	// future, it may be necessary to extract additional metadata.
	if stmt.Interleave != nil {
		tab.interleaved = true
	}

	// Add non-mutation columns.
	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *tree.ColumnTableDef:
			if !isMutationColumn(def) {
				tab.addColumn(def)
			}
		}
	}

	// If there is no primary index, add the hidden rowid column.
	hasPrimaryIndex := false
	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *tree.ColumnTableDef:
			if def.PrimaryKey.IsPrimaryKey {
				hasPrimaryIndex = true
			}

		case *tree.UniqueConstraintTableDef:
			if def.PrimaryKey {
				hasPrimaryIndex = true
			}
		}
	}

	if !hasPrimaryIndex {
		rowid := &Column{
			Ordinal:     tab.ColumnCount(),
			Name:        "rowid",
			Type:        types.Int,
			Hidden:      true,
			DefaultExpr: &uniqueRowIDString,
		}
		tab.Columns = append(tab.Columns, rowid)
	}

	// Add any mutation columns (after any hidden rowid column).
	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *tree.ColumnTableDef:
			if isMutationColumn(def) {
				tab.addColumn(def)
			}
		}
	}

	// Add the primary index.
	if hasPrimaryIndex {
		for _, def := range stmt.Defs {
			switch def := def.(type) {
			case *tree.ColumnTableDef:
				if def.PrimaryKey.IsPrimaryKey {
					// Add the primary index over the single column.
					tab.addPrimaryColumnIndex(string(def.Name))
				}

			case *tree.UniqueConstraintTableDef:
				if def.PrimaryKey {
					tab.addIndex(&def.IndexTableDef, primaryIndex)
				}
			}
		}
	} else {
		tab.addPrimaryColumnIndex("rowid")
	}
	if stmt.PartitionBy != nil {
		tab.Indexes[0].partitionBy = stmt.PartitionBy
	}

	// Add check constraints.
	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *tree.CheckConstraintTableDef:
			tab.Checks = append(tab.Checks, cat.CheckConstraint{
				Constraint: serializeTableDefExpr(def.Expr),
				Validated:  validatedCheckConstraint(def),
			})
		}
	}

	// Search for index and family definitions.
	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *tree.UniqueConstraintTableDef:
			if !def.PrimaryKey {
				tab.addIndex(&def.IndexTableDef, uniqueIndex)
			}

		case *tree.IndexTableDef:
			tab.addIndex(def, nonUniqueIndex)

		case *tree.FamilyTableDef:
			tab.addFamily(def)

		case *tree.ColumnTableDef:
			if def.Unique {
				tab.addIndex(
					&tree.IndexTableDef{
						Name:    tree.Name(fmt.Sprintf("%s_%s_key", stmt.Table.ObjectName, def.Name)),
						Columns: tree.IndexElemList{{Column: def.Name}},
					},
					uniqueIndex,
				)
			}
		}
	}

	// If there are columns missing from explicit family definitions, add them
	// to family 0 (ensure that one exists).
	if len(tab.Families) == 0 {
		tab.Families = []*Family{{FamName: "primary", Ordinal: 0, table: tab}}
	}
OuterLoop:
	for colOrd, col := range tab.Columns {
		for _, fam := range tab.Families {
			for _, famCol := range fam.Columns {
				if col.Name == string(famCol.ColName()) {
					continue OuterLoop
				}
			}
		}
		tab.Families[0].Columns = append(tab.Families[0].Columns,
			cat.FamilyColumn{Column: col, Ordinal: colOrd})
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

	// Add the new table to the catalog.
	tc.AddTable(tab)

	return tab
}

func (tc *Catalog) createVirtualTable(stmt *tree.CreateTable) *Table {
	tab := &Table{
		TabID:     tc.nextStableID(),
		TabName:   stmt.Table,
		Catalog:   tc,
		IsVirtual: true,
	}

	// Add the dummy PK column.
	tab.Columns = []*Column{{
		Ordinal:  0,
		Hidden:   true,
		Nullable: false,
		Name:     "crdb_internal_vtable_pk",
		Type:     types.Int,
	}}

	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *tree.ColumnTableDef:
			tab.addColumn(def)
		}
	}

	tab.Families = []*Family{{FamName: "primary", Ordinal: 0, table: tab}}
	for colOrd, col := range tab.Columns {
		tab.Families[0].Columns = append(tab.Families[0].Columns,
			cat.FamilyColumn{Column: col, Ordinal: colOrd})
	}

	tab.addPrimaryColumnIndex(tab.Columns[0].Name)
	return tab
}

// CreateTableAs creates a table in the catalog with the given name and
// columns. It should be used for creating a table from the CREATE TABLE <name>
// AS <query> syntax. In addition to the provided columns, CreateTableAs adds a
// unique rowid column as the primary key. It returns a pointer to the new
// table.
func (tc *Catalog) CreateTableAs(name tree.TableName, columns []*Column) *Table {
	// Update the table name to include catalog and schema if not provided.
	tc.qualifyTableName(&name)

	tab := &Table{TabID: tc.nextStableID(), TabName: name, Catalog: tc, Columns: columns}

	rowid := &Column{
		Ordinal:     tab.ColumnCount(),
		Name:        "rowid",
		Type:        types.Int,
		Hidden:      true,
		DefaultExpr: &uniqueRowIDString,
	}
	tab.Columns = append(tab.Columns, rowid)
	tab.addPrimaryColumnIndex("rowid")

	// Add the new table to the catalog.
	tc.AddTable(tab)

	return tab
}

// resolveFK processes a foreign key constraint.
func (tc *Catalog) resolveFK(tab *Table, d *tree.ForeignKeyConstraintTableDef) {
	fromCols := make([]int, len(d.FromCols))
	for i, c := range d.FromCols {
		fromCols[i] = tab.FindOrdinal(string(c))
	}

	var targetTable *Table
	if d.Table.ObjectName == tab.Name() {
		targetTable = tab
	} else {
		targetTable = tc.Table(&d.Table)
	}

	toCols := make([]int, len(d.ToCols))
	for i, c := range d.ToCols {
		toCols[i] = targetTable.FindOrdinal(string(c))
	}

	constraintName := string(d.Name)
	if constraintName == "" {
		constraintName = fmt.Sprintf(
			"fk_%s_ref_%s", string(d.FromCols[0]), targetTable.TabName.Table(),
		)
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
		if idx.LaxKeyColumnCount() < len(cols) {
			return false
		}
		if strict && idx.LaxKeyColumnCount() > len(cols) {
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
			break
		}
	}
	if !found {
		// Add a non-unique index on fromCols.
		idx := tree.IndexTableDef{
			Name:    tree.Name(fmt.Sprintf("%s_auto_index_%s", tab.TabName.Table(), constraintName)),
			Columns: make(tree.IndexElemList, len(fromCols)),
		}
		for i, c := range fromCols {
			idx.Columns[i].Column = tab.Columns[c].ColName()
			idx.Columns[i].Direction = tree.Ascending
		}
		tab.addIndex(&idx, nonUniqueIndex)
	}

	fk := ForeignKeyConstraint{
		name:                     constraintName,
		originTableID:            tab.ID(),
		referencedTableID:        targetTable.ID(),
		originColumnOrdinals:     fromCols,
		referencedColumnOrdinals: toCols,
		validated:                true,
		matchMethod:              d.Match,
		deleteAction:             d.Actions.Delete,
		updateAction:             d.Actions.Update,
	}
	tab.outboundFKs = append(tab.outboundFKs, fk)
	targetTable.inboundFKs = append(targetTable.inboundFKs, fk)
}

func (tt *Table) addColumn(def *tree.ColumnTableDef) {
	nullable := !def.PrimaryKey.IsPrimaryKey && def.Nullable.Nullability != tree.NotNull
	col := &Column{
		Ordinal:  tt.ColumnCount(),
		Name:     string(def.Name),
		Type:     tree.MustBeStaticallyKnownType(def.Type),
		Nullable: nullable,
	}

	// Look for name suffixes indicating this is a mutation column.
	if name, ok := extractWriteOnlyColumn(def); ok {
		col.Name = name
		tt.writeOnlyColCount++
	} else if name, ok := extractDeleteOnlyColumn(def); ok {
		col.Name = name
		tt.deleteOnlyColCount++
	}

	if def.DefaultExpr.Expr != nil {
		s := serializeTableDefExpr(def.DefaultExpr.Expr)
		col.DefaultExpr = &s
	}

	if def.Computed.Expr != nil {
		s := serializeTableDefExpr(def.Computed.Expr)
		col.ComputedExpr = &s
	}

	tt.Columns = append(tt.Columns, col)
}

func (tt *Table) addIndex(def *tree.IndexTableDef, typ indexType) *Index {
	idx := &Index{
		IdxName:     tt.makeIndexName(def.Name, typ),
		Unique:      typ != nonUniqueIndex,
		Inverted:    def.Inverted,
		IdxZone:     &zonepb.ZoneConfig{},
		table:       tt,
		partitionBy: def.PartitionBy,
	}

	// Look for name suffixes indicating this is a mutation index.
	if name, ok := extractWriteOnlyIndex(def); ok {
		idx.IdxName = name
		tt.writeOnlyIdxCount++
	} else if name, ok := extractDeleteOnlyIndex(def); ok {
		idx.IdxName = name
		tt.deleteOnlyIdxCount++
	}

	// Add explicit columns and mark primary key columns as not null.
	// Add the geoConfig if applicable.
	notNullIndex := true
	for i, colDef := range def.Columns {
		col := idx.addColumn(tt, string(colDef.Column), colDef.Direction, keyCol)

		if typ == primaryIndex {
			col.Nullable = false
		}

		if col.Nullable {
			notNullIndex = false
		}

		if i == 0 && def.Inverted {
			switch col.Type.Family() {
			case types.GeometryFamily:
				// Don't use the default config because it creates a huge number of spans.
				idx.geoConfig = &geoindex.Config{
					S2Geometry: &geoindex.S2GeometryConfig{
						MinX: -5,
						MaxX: 5,
						MinY: -5,
						MaxY: 5,
						S2Config: &geoindex.S2Config{
							MinLevel: 0,
							MaxLevel: 2,
							LevelMod: 1,
							MaxCells: 3,
						},
					},
				}

			case types.GeographyFamily:
				// Don't use the default config because it creates a huge number of spans.
				idx.geoConfig = &geoindex.Config{
					S2Geography: &geoindex.S2GeographyConfig{S2Config: &geoindex.S2Config{
						MinLevel: 0,
						MaxLevel: 2,
						LevelMod: 1,
						MaxCells: 3,
					}},
				}
			}
		}
	}

	if typ == primaryIndex {
		var pkOrdinals util.FastIntSet
		for _, c := range idx.Columns {
			pkOrdinals.Add(c.Ordinal)
		}
		// Add the rest of the columns in the table.
		for i, n := 0, tt.DeletableColumnCount(); i < n; i++ {
			if !pkOrdinals.Contains(i) {
				idx.addColumnByOrdinal(tt, i, tree.Ascending, nonKeyCol)
			}
		}
		if len(tt.Indexes) != 0 {
			panic("primary index should always be 0th index")
		}
		idx.ordinal = len(tt.Indexes)
		tt.Indexes = append(tt.Indexes, idx)
		return idx
	}

	// Add implicit key columns from primary index.
	pkCols := tt.Indexes[cat.PrimaryIndex].Columns[:tt.Indexes[cat.PrimaryIndex].KeyCount]
	for _, pkCol := range pkCols {
		// Only add columns that aren't already part of index.
		found := false
		for _, colDef := range def.Columns {
			if pkCol.ColName() == colDef.Column {
				found = true
			}
		}

		if !found {
			name := string(pkCol.ColName())

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
			if name == pkCol.ColName() {
				found = true
			}
		}
		if !found {
			idx.addColumn(tt, string(name), tree.Ascending, nonKeyCol)
		}
	}

	// Add partial index predicate.
	if def.Predicate != nil {
		idx.predicate = tree.Serialize(def.Predicate)
	}

	idx.ordinal = len(tt.Indexes)
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

func (tt *Table) addFamily(def *tree.FamilyTableDef) {
	// Synthesize name if one was not provided.
	name := string(def.Name)
	if name == "" {
		name = fmt.Sprintf("family%d", len(tt.Families)+1)
	}

	family := &Family{
		FamName: name,
		Ordinal: tt.FamilyCount(),
		table:   tt,
	}

	// Add columns to family.
	for _, defCol := range def.Columns {
		ord := tt.FindOrdinal(string(defCol))
		col := tt.Column(ord)
		family.Columns = append(family.Columns, cat.FamilyColumn{Column: col, Ordinal: ord})
	}

	tt.Families = append(tt.Families, family)
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
	idxCol := cat.IndexColumn{
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

func extractWriteOnlyColumn(def *tree.ColumnTableDef) (name string, ok bool) {
	if !strings.HasSuffix(string(def.Name), ":write-only") {
		return "", false
	}
	return strings.TrimSuffix(string(def.Name), ":write-only"), true
}

func extractDeleteOnlyColumn(def *tree.ColumnTableDef) (name string, ok bool) {
	if !strings.HasSuffix(string(def.Name), ":delete-only") {
		return "", false
	}
	return strings.TrimSuffix(string(def.Name), ":delete-only"), true
}

func isMutationColumn(def *tree.ColumnTableDef) bool {
	if _, ok := extractWriteOnlyColumn(def); ok {
		return true
	}
	if _, ok := extractDeleteOnlyColumn(def); ok {
		return true
	}
	return false
}

func extractWriteOnlyIndex(def *tree.IndexTableDef) (name string, ok bool) {
	if !strings.HasSuffix(string(def.Name), ":write-only") {
		return "", false
	}
	return strings.TrimSuffix(string(def.Name), ":write-only"), true
}

func extractDeleteOnlyIndex(def *tree.IndexTableDef) (name string, ok bool) {
	if !strings.HasSuffix(string(def.Name), ":delete-only") {
		return "", false
	}
	return strings.TrimSuffix(string(def.Name), ":delete-only"), true
}

func validatedCheckConstraint(def *tree.CheckConstraintTableDef) bool {
	return !strings.HasSuffix(string(def.Name), ":unvalidated")
}

func serializeTableDefExpr(expr tree.Expr) string {
	// Disallow any column references that are qualified with the table. The
	// production table creation code verifies them and strips them away, so the
	// stored expressions contain only unqualified column references.
	preFn := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		if vBase, ok := expr.(tree.VarName); ok {
			v, err := vBase.NormalizeVarName()
			if err != nil {
				return false, nil, err
			}
			if c, ok := v.(*tree.ColumnItem); ok && c.TableName != nil {
				return false, nil, fmt.Errorf(
					"expressions in table definitions must not contain qualified column references: %s", c,
				)
			}
		}
		return true, expr, nil
	}
	_, err := tree.SimpleVisit(expr, preFn)
	if err != nil {
		panic(err)
	}
	return tree.Serialize(expr)
}
