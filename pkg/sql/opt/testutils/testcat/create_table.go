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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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

// creteFKIndexes controls whether we automatically create indexes on the
// referencing side of foreign keys (like it was required before 20.2).
const createFKIndexes = false

// CreateTable creates a test table from a parsed DDL statement and adds it to
// the catalog. This is intended for testing, and is not a complete (and
// probably not fully correct) implementation. It just has to be "good enough".
func (tc *Catalog) CreateTable(stmt *tree.CreateTable) *Table {
	stmt.HoistConstraints()

	// Update the table name to include catalog and schema if not provided.
	tc.qualifyTableName(&stmt.Table)

	// Assume that every table in the "system", "information_schema" or
	// "pg_catalog" catalog is a virtual table. This is a simplified assumption
	// for testing purposes.
	if stmt.Table.CatalogName == "system" || stmt.Table.SchemaName == "information_schema" ||
		stmt.Table.SchemaName == "pg_catalog" {
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
		var rowid cat.Column
		ordinal := len(tab.Columns)
		rowid.InitNonVirtual(
			ordinal,
			cat.StableID(1+ordinal),
			"rowid",
			cat.Ordinary,
			types.Int,
			false,              /* nullable */
			true,               /* hidden */
			&uniqueRowIDString, /* defaultExpr */
			nil,                /* computedExpr */
		)
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

	// Add the MVCC timestamp system column.
	var mvcc cat.Column
	ordinal := len(tab.Columns)
	mvcc.InitNonVirtual(
		ordinal,
		cat.StableID(1+ordinal),
		colinfo.MVCCTimestampColumnName,
		cat.System,
		colinfo.MVCCTimestampColumnType,
		true, /* nullable */
		true, /* hidden */
		nil,  /* defaultExpr */
		nil,  /* computedExpr */
	)
	tab.Columns = append(tab.Columns, mvcc)

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
			if def.WithoutIndex {
				panic(pgerror.New(pgcode.FeatureNotSupported,
					"unique constraints without an index are not yet supported",
				))
			}
			if !def.PrimaryKey {
				tab.addIndex(&def.IndexTableDef, uniqueIndex)
			}

		case *tree.IndexTableDef:
			tab.addIndex(def, nonUniqueIndex)

		case *tree.FamilyTableDef:
			tab.addFamily(def)

		case *tree.ColumnTableDef:
			if def.Unique.WithoutIndex {
				panic(pgerror.New(pgcode.FeatureNotSupported,
					"unique constraints without an index are not yet supported",
				))
			}
			if def.Unique.IsUnique {
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
	for colOrd := range tab.Columns {
		col := &tab.Columns[colOrd]
		for _, fam := range tab.Families {
			for _, famCol := range fam.Columns {
				if col.ColName() == famCol.ColName() {
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
	var pk cat.Column
	pk.InitNonVirtual(
		0, /* ordinal */
		0, /* stableID */
		"crdb_internal_vtable_pk",
		cat.Ordinary,
		types.Int,
		false, /* nullable */
		true,  /* hidden */
		nil,   /* defaultExpr */
		nil,   /* computedExpr */
	)

	tab.Columns = []cat.Column{pk}

	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *tree.ColumnTableDef:
			tab.addColumn(def)
		}
	}

	tab.Families = []*Family{{FamName: "primary", Ordinal: 0, table: tab}}
	for colOrd := range tab.Columns {
		tab.Families[0].Columns = append(tab.Families[0].Columns,
			cat.FamilyColumn{Column: &tab.Columns[colOrd], Ordinal: colOrd})
	}

	tab.addPrimaryColumnIndex(string(tab.Columns[0].ColName()))

	// Search for index definitions.
	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *tree.IndexTableDef:
			tab.addIndex(def, nonUniqueIndex)
		}
	}

	// Add the new table to the catalog.
	tc.AddTable(tab)

	return tab
}

// CreateTableAs creates a table in the catalog with the given name and
// columns. It should be used for creating a table from the CREATE TABLE <name>
// AS <query> syntax. In addition to the provided columns, CreateTableAs adds a
// unique rowid column as the primary key. It returns a pointer to the new
// table.
func (tc *Catalog) CreateTableAs(name tree.TableName, columns []cat.Column) *Table {
	// Update the table name to include catalog and schema if not provided.
	tc.qualifyTableName(&name)

	tab := &Table{TabID: tc.nextStableID(), TabName: name, Catalog: tc, Columns: columns}

	var rowid cat.Column
	ordinal := len(columns)
	rowid.InitNonVirtual(
		ordinal,
		cat.StableID(1+ordinal),
		"rowid",
		cat.Ordinary,
		types.Int,
		false,              /* nullable */
		true,               /* hidden */
		&uniqueRowIDString, /* defaultExpr */
		nil,                /* computedExpr */
	)

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
			if idx.Column(i).Ordinal() != cols[i] {
				return false
			}
		}
		if _, isPartialIndex := idx.Predicate(); isPartialIndex {
			return false
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

	if createFKIndexes {
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
	ordinal := len(tt.Columns)
	nullable := !def.PrimaryKey.IsPrimaryKey && def.Nullable.Nullability != tree.NotNull
	typ := tree.MustBeStaticallyKnownType(def.Type)

	name := def.Name
	kind := cat.Ordinary

	// Look for name suffixes indicating this is a mutation column.
	if n, ok := extractWriteOnlyColumn(def); ok {
		name = n
		kind = cat.WriteOnly
	} else if n, ok := extractDeleteOnlyColumn(def); ok {
		name = n
		kind = cat.DeleteOnly
	}

	var defaultExpr, computedExpr *string
	if def.DefaultExpr.Expr != nil {
		s := serializeTableDefExpr(def.DefaultExpr.Expr)
		defaultExpr = &s
	}

	if def.Computed.Expr != nil {
		s := serializeTableDefExpr(def.Computed.Expr)
		computedExpr = &s
	}

	var col cat.Column
	col.InitNonVirtual(
		ordinal,
		cat.StableID(1+ordinal),
		name,
		kind,
		typ,
		nullable,
		false, /* hidden */
		defaultExpr,
		computedExpr,
	)
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
		isLastIndexCol := i == len(def.Columns)-1
		if def.Inverted && isLastIndexCol {
			idx.invertedOrd = i
		}
		col := idx.addColumn(tt, colDef, keyCol, isLastIndexCol)

		if typ == primaryIndex && col.IsNullable() {
			// Reinitialize the column to make it non-nullable.
			// TODO(radu): this is very hacky
			var defaultExpr, computedExpr *string
			if col.HasDefault() {
				e := col.DefaultExprStr()
				defaultExpr = &e
			}
			if col.IsComputed() {
				e := col.ComputedExprStr()
				computedExpr = &e
			}
			col.InitNonVirtual(
				col.Ordinal(),
				col.ColID(),
				col.ColName(),
				col.Kind(),
				col.DatumType(),
				false, /* nullable */
				col.IsHidden(),
				defaultExpr,
				computedExpr,
			)
		}

		if col.IsNullable() {
			notNullIndex = false
		}

		if isLastIndexCol && def.Inverted {
			switch tt.Columns[col.InvertedSourceColumnOrdinal()].DatumType().Family() {
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
			pkOrdinals.Add(c.Ordinal())
		}
		// Add the rest of the columns in the table.
		for i, col := range tt.Columns {
			if !pkOrdinals.Contains(i) && !col.Kind().IsVirtual() {
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
			elem := tree.IndexElem{
				Column:    pkCol.ColName(),
				Direction: tree.Ascending,
			}
			if typ == uniqueIndex {
				// If unique index has no NULL columns, then the implicit columns
				// are added as storing columns. Otherwise, they become part of the
				// strict key, since they're needed to ensure uniqueness (but they
				// are not part of the lax key).
				if notNullIndex {
					idx.addColumn(tt, elem, nonKeyCol, false /* isLastIndexCol */)
				} else {
					idx.addColumn(tt, elem, strictKeyCol, false /* isLastIndexCol */)
				}
			} else {
				// Implicit columns are always added to the key for a non-unique
				// index. In addition, there is no separate lax key, so the lax
				// key column count = key column count.
				idx.addColumn(tt, elem, keyCol, false /* isLastIndexCol */)
			}
		}
	}

	// Add storing columns.
	for _, name := range def.Storing {
		if def.Inverted {
			panic("inverted indexes don't support stored columns")
		}
		// Only add storing columns that weren't added as part of adding implicit
		// key columns.
		found := false
		for _, pkCol := range pkCols {
			if name == pkCol.ColName() {
				found = true
			}
		}
		if !found {
			elem := tree.IndexElem{
				Column:    name,
				Direction: tree.Ascending,
			}
			idx.addColumn(tt, elem, nonKeyCol, false /* isLastIndexCol */)
		}
	}
	if tt.IsVirtual {
		// All indexes of virtual tables automatically STORE all other columns in
		// the table.
		idxCols := idx.Columns
		for _, col := range tt.Columns {
			found := false
			for _, idxCol := range idxCols {
				if col.ColName() == idxCol.ColName() {
					found = true
					break
				}
			}
			if !found {
				elem := tree.IndexElem{
					Column:    col.ColName(),
					Direction: tree.Ascending,
				}
				idx.addColumn(tt, elem, nonKeyCol, false /* isLastIndexCol */)
			}
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

// addColumn adds a column to the index. If necessary, creates a virtual column
// (for inverted and expression-based indexes).
//
// isLastIndexCol indicates if this is the last explicit column in the index as
// specified in the schema; it is used to indicate the inverted column if the
// index is inverted.
func (ti *Index) addColumn(
	tt *Table, elem tree.IndexElem, colType colType, isLastIndexCol bool,
) *cat.Column {
	if elem.Expr != nil {
		if ti.Inverted && isLastIndexCol {
			panic("expression-based inverted column not supported")
		}
		col := columnForIndexElemExpr(tt, elem.Expr)
		return ti.addColumnByOrdinal(tt, col.Ordinal(), elem.Direction, colType)
	}

	ordinal := tt.FindOrdinal(string(elem.Column))
	if ti.Inverted && isLastIndexCol {
		// The last column of an inverted index is special: the index key does not
		// contain values from the column itself, but contains inverted index
		// entries derived from that column. Create a virtual column to be able to
		// refer to it separately.
		var col cat.Column
		// TODO(radu,mjibson): update this when the corresponding type in the real
		// catalog is fixed (see sql.newOptTable).
		typ := tt.Columns[ordinal].DatumType()
		col.InitVirtualInverted(
			len(tt.Columns),
			elem.Column+"_inverted_key",
			typ,
			false,   /* nullable */
			ordinal, /* invertedSourceColumnOrdinal */
		)
		tt.Columns = append(tt.Columns, col)
		ordinal = col.Ordinal()
	}

	return ti.addColumnByOrdinal(tt, ordinal, elem.Direction, colType)
}

// columnForIndexElemExpr returns a VirtualComputed table column that can be
// used as an index column when the index element is an expression. If an
// existing VirtualComputed column with the same expression exists, it is
// reused. Otherwise, a new column is added to the table.
func columnForIndexElemExpr(tt *Table, expr tree.Expr) cat.Column {
	exprStr := serializeTableDefExpr(expr)
	// Find an existing virtual computed column with the same expression.
	for _, col := range tt.Columns {
		if col.Kind() == cat.VirtualComputed &&
			col.ComputedExprStr() == exprStr {
			return col
		}
	}
	// Add a new virtual computed column with a unique name.
	var name tree.Name
	for n, done := 1, false; !done; n++ {
		done = true
		name = tree.Name(fmt.Sprintf("idx_expr_%d", n))
		for _, col := range tt.Columns {
			if col.ColName() == name {
				done = false
				break
			}
		}
	}

	typ := typeCheckTableExpr(expr, tt.Columns)
	var col cat.Column
	col.InitVirtualComputed(
		len(tt.Columns),
		name,
		typ,
		true, /* nullable */
		exprStr,
	)
	tt.Columns = append(tt.Columns, col)
	return col
}

func (ti *Index) addColumnByOrdinal(
	tt *Table, ord int, direction tree.Direction, colType colType,
) *cat.Column {
	col := tt.Column(ord)
	if colType == keyCol || colType == strictKeyCol {
		typ := col.DatumType()
		if col.Kind() == cat.VirtualInverted {
			if !colinfo.ColumnTypeIsInvertedIndexable(typ) {
				panic(fmt.Errorf(
					"column %s of type %s is not allowed as the last column of an inverted index",
					col.ColName(), typ,
				))
			}
		} else if !colinfo.ColumnTypeIsIndexable(typ) {
			panic(fmt.Errorf("column %s of type %s is not indexable", col.ColName(), typ))
		}
	}
	idxCol := cat.IndexColumn{
		Column:     col,
		Descending: direction == tree.Descending,
	}
	ti.Columns = append(ti.Columns, idxCol)

	// Update key column counts.
	switch colType {
	case keyCol:
		// Column is part of both lax and strict keys.
		ti.LaxKeyCount++
		ti.KeyCount++

	case strictKeyCol:
		// Column is only part of the strict key.
		ti.KeyCount++
	}

	return col
}

func (tt *Table) addPrimaryColumnIndex(colName string) {
	def := tree.IndexTableDef{
		Columns: tree.IndexElemList{{Column: tree.Name(colName), Direction: tree.Ascending}},
	}
	tt.addIndex(&def, primaryIndex)
}

func extractWriteOnlyColumn(def *tree.ColumnTableDef) (name tree.Name, ok bool) {
	if !strings.HasSuffix(string(def.Name), ":write-only") {
		return "", false
	}
	return tree.Name(strings.TrimSuffix(string(def.Name), ":write-only")), true
}

func extractDeleteOnlyColumn(def *tree.ColumnTableDef) (name tree.Name, ok bool) {
	if !strings.HasSuffix(string(def.Name), ":delete-only") {
		return "", false
	}
	return tree.Name(strings.TrimSuffix(string(def.Name), ":delete-only")), true
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
