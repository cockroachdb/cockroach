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
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
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

	// Find the PK columns.
	pkCols := make(map[tree.Name]struct{})
	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *tree.ColumnTableDef:
			if def.PrimaryKey.IsPrimaryKey {
				pkCols[def.Name] = struct{}{}
			}

		case *tree.UniqueConstraintTableDef:
			if def.PrimaryKey {
				for i := range def.Columns {
					pkCols[def.Columns[i].Column] = struct{}{}
				}
			}
		}
	}

	// Add non-mutation columns.
	for _, def := range stmt.Defs {
		switch def := def.(type) {
		case *tree.ColumnTableDef:
			if !isMutationColumn(def) {
				if _, isPKCol := pkCols[def.Name]; isPKCol {
					// Force PK columns to be non-nullable and non-virtual.
					def.Nullable.Nullability = tree.NotNull
					if def.Computed.Computed {
						def.Computed.Virtual = false
					}
				}
				tab.addColumn(def)
			}
		}
	}

	// If there is no primary index, add the hidden rowid column.
	hasPrimaryIndex := len(pkCols) > 0
	if !hasPrimaryIndex {
		var rowid cat.Column
		ordinal := len(tab.Columns)
		rowid.Init(
			ordinal,
			cat.StableID(1+ordinal),
			"rowid",
			cat.Ordinary,
			types.Int,
			false, /* nullable */
			cat.Hidden,
			&uniqueRowIDString, /* defaultExpr */
			nil,                /* computedExpr */
			nil,                /* onUpdateExpr */
			cat.NotGeneratedAsIdentity,
			nil, /* generatedAsIdentitySequenceOption */
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
	mvcc.Init(
		ordinal,
		cat.StableID(1+ordinal),
		colinfo.MVCCTimestampColumnName,
		cat.System,
		colinfo.MVCCTimestampColumnType,
		true, /* nullable */
		cat.Hidden,
		nil, /* defaultExpr */
		nil, /* computedExpr */
		nil, /* onUpdateExpr */
		cat.NotGeneratedAsIdentity,
		nil, /* generatedAsIdentitySequenceOption */
	)
	tab.Columns = append(tab.Columns, mvcc)

	// Add the tableoid system column.
	var tableoid cat.Column
	ordinal = len(tab.Columns)
	tableoid.Init(
		ordinal,
		cat.StableID(1+ordinal),
		colinfo.TableOIDColumnName,
		cat.System,
		types.Oid,
		true, /* nullable */
		cat.Hidden,
		nil, /* defaultExpr */
		nil, /* computedExpr */
		nil, /* onUpdateExpr */
		cat.NotGeneratedAsIdentity,
		nil, /* generatedAsIdentitySequenceOption */
	)
	tab.Columns = append(tab.Columns, tableoid)

	// Cache the partitioning statement for the primary index.
	if stmt.PartitionByTable != nil {
		tab.partitionBy = stmt.PartitionByTable.PartitionBy
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
				tab.addUniqueConstraint(def.Name, def.Columns, def.Predicate, def.WithoutIndex)
			} else if !def.PrimaryKey {
				tab.addIndex(&def.IndexTableDef, uniqueIndex)
			}

		case *tree.IndexTableDef:
			tab.addIndex(def, nonUniqueIndex)

		case *tree.FamilyTableDef:
			tab.addFamily(def)

		case *tree.ColumnTableDef:
			if def.Unique.IsUnique {
				if def.Unique.WithoutIndex {
					tab.addUniqueConstraint(
						def.Unique.ConstraintName,
						tree.IndexElemList{{Column: def.Name}},
						nil, /* predicate */
						def.Unique.WithoutIndex,
					)
				} else {
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
	pk.Init(
		0, /* ordinal */
		0, /* stableID */
		"crdb_internal_vtable_pk",
		cat.Ordinary,
		types.Int,
		false, /* nullable */
		cat.Hidden,
		nil, /* defaultExpr */
		nil, /* computedExpr */
		nil, /* onUpdateExpr */
		cat.NotGeneratedAsIdentity,
		nil, /* generatedAsIdentitySequenceOption */
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
	rowid.Init(
		ordinal,
		cat.StableID(1+ordinal),
		"rowid",
		cat.Ordinary,
		types.Int,
		false, /* nullable */
		cat.Hidden,
		&uniqueRowIDString, /* defaultExpr */
		nil,                /* computedExpr */
		nil,                /* onUpdateExpr */
		cat.NotGeneratedAsIdentity,
		nil, /* generatedAsIdentitySequenceOption */
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

	referencedColNames := d.ToCols
	if len(referencedColNames) == 0 {
		// If no columns are specified, attempt to default to PK, ignoring implicit
		// columns.
		idx := targetTable.Index(cat.PrimaryIndex)
		numImplicitCols := idx.ImplicitColumnCount()
		referencedColNames = make(
			tree.NameList,
			0,
			idx.KeyColumnCount()-numImplicitCols,
		)
		for i := numImplicitCols; i < idx.KeyColumnCount(); i++ {
			referencedColNames = append(
				referencedColNames,
				idx.Column(i).ColName(),
			)
		}
	}
	toCols := make([]int, len(referencedColNames))
	for i, c := range referencedColNames {
		toCols[i] = targetTable.FindOrdinal(string(c))
	}

	constraintName := string(d.Name)
	if constraintName == "" {
		constraintName = tabledesc.ForeignKeyConstraintName(
			tab.TabName.Table(),
			d.FromCols.ToStrings(),
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

	// indexMatches returns true if the key columns in the given index match the
	// given columns. If strict is false, it is acceptable if the given columns
	// are a prefix of the index key columns.
	indexMatches := func(idx *Index, cols []int, strict bool) bool {
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

	// uniqueConstraintMatches returns true if the key columns in the given unique
	// constraint match the given columns.
	uniqueConstraintMatches := func(uc *UniqueConstraint, cols []int) bool {
		if colCount := uc.ColumnCount(); colCount < len(cols) || colCount > len(cols) {
			return false
		}
		for i := range cols {
			if uc.columnOrdinals[i] != cols[i] {
				return false
			}
		}
		return true
	}

	// 1. Verify that the target table has a unique index or unique constraint.
	var targetIndex *Index
	var targetUniqueConstraint *UniqueConstraint
	for _, idx := range targetTable.Indexes {
		if indexMatches(idx, toCols, true /* strict */) {
			targetIndex = idx
			break
		}
	}
	if targetIndex == nil {
		for i := range targetTable.uniqueConstraints {
			uc := &targetTable.uniqueConstraints[i]
			if uniqueConstraintMatches(uc, toCols) {
				targetUniqueConstraint = uc
				break
			}
		}
	}
	if targetIndex == nil && targetUniqueConstraint == nil {
		panic(fmt.Errorf(
			"there is no unique constraint matching given keys for referenced table %s",
			targetTable.Name(),
		))
	}

	if createFKIndexes {
		// 2. Search for an existing index in the source table; add it if necessary.
		found := false
		for _, idx := range tab.Indexes {
			if indexMatches(idx, fromCols, false /* strict */) {
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

func (tt *Table) addUniqueConstraint(
	name tree.Name, columns tree.IndexElemList, predicate tree.Expr, withoutIndex bool,
) {
	// We don't currently use unique constraints with an index (those are already
	// tracked with unique indexes), so don't bother adding them.
	// NB: This should stay consistent with opt_catalog.go.
	if !withoutIndex {
		return
	}
	cols := make([]int, len(columns))
	for i, c := range columns {
		cols[i] = tt.FindOrdinal(string(c.Column))
	}
	sort.Ints(cols)

	// Don't add duplicate constraints.
	for _, c := range tt.uniqueConstraints {
		if reflect.DeepEqual(c.columnOrdinals, cols) && c.withoutIndex == withoutIndex {
			return
		}
	}

	// We didn't find an existing constraint, so add a new one.
	u := UniqueConstraint{
		name:           tt.makeUniqueConstraintName(name, columns),
		tabID:          tt.TabID,
		columnOrdinals: cols,
		withoutIndex:   withoutIndex,
		validated:      true,
	}
	// Add partial unique constraint predicate.
	if predicate != nil {
		u.predicate = tree.Serialize(predicate)
	}
	tt.uniqueConstraints = append(tt.uniqueConstraints, u)
}

func (tt *Table) addColumn(def *tree.ColumnTableDef) {
	ordinal := len(tt.Columns)
	nullable := !def.PrimaryKey.IsPrimaryKey && def.Nullable.Nullability != tree.NotNull
	typ, err := tree.ResolveType(context.Background(), def.Type, tt.Catalog)
	if err != nil {
		panic(err)
	}

	name := def.Name
	kind := cat.Ordinary
	visibility := cat.Visible

	if def.IsSerial {
		// Here we only take care of the case where
		// serial_normalization == SerialUsesRowID.
		def.DefaultExpr.Expr = generateDefExprForSerialCol(tt.TabName, name, sessiondatapb.SerialUsesRowID)
	}

	// Look for name suffixes indicating this is a special column.
	if n, ok := extractInaccessibleColumn(def); ok {
		name = n
		visibility = cat.Inaccessible
	} else if n, ok := extractWriteOnlyColumn(def); ok {
		name = n
		kind = cat.WriteOnly
		visibility = cat.Inaccessible
	} else if n, ok := extractDeleteOnlyColumn(def); ok {
		name = n
		kind = cat.DeleteOnly
		visibility = cat.Inaccessible
	}

	var defaultExpr, computedExpr, onUpdateExpr, generatedAsIdentitySequenceOption *string
	if def.DefaultExpr.Expr != nil {
		s := serializeTableDefExpr(def.DefaultExpr.Expr)
		defaultExpr = &s
	}

	if def.Computed.Expr != nil {
		s := serializeTableDefExpr(def.Computed.Expr)
		computedExpr = &s
	}

	if def.OnUpdateExpr.Expr != nil {
		s := serializeTableDefExpr(def.OnUpdateExpr.Expr)
		onUpdateExpr = &s
	}

	generatedAsIdentityType := cat.NotGeneratedAsIdentity
	if def.GeneratedIdentity.IsGeneratedAsIdentity {
		switch def.GeneratedIdentity.GeneratedAsIdentityType {
		case tree.GeneratedAlways, tree.GeneratedByDefault:
			def.DefaultExpr.Expr = generateDefExprForGeneratedAsIdentityCol(tt.TabName, name)
			switch def.GeneratedIdentity.GeneratedAsIdentityType {
			case tree.GeneratedAlways:
				generatedAsIdentityType = cat.GeneratedAlwaysAsIdentity
			case tree.GeneratedByDefault:
				generatedAsIdentityType = cat.GeneratedByDefaultAsIdentity
			}
		default:
			panic(fmt.Errorf(
				"column %s is of invalid generated as identity type (neither ALWAYS nor BY DEFAULT)",
				def.Name,
			))
		}
	}

	if def.DefaultExpr.Expr != nil {
		s := serializeTableDefExpr(def.DefaultExpr.Expr)
		defaultExpr = &s
	}

	if def.Computed.Expr != nil {
		s := serializeTableDefExpr(def.Computed.Expr)
		computedExpr = &s
	}

	if def.GeneratedIdentity.SeqOptions != nil {
		s := serializeGeneratedAsIdentitySequenceOption(&def.GeneratedIdentity.SeqOptions)
		generatedAsIdentitySequenceOption = &s
	}

	var col cat.Column
	if def.Computed.Virtual {
		col.InitVirtualComputed(
			ordinal,
			cat.StableID(1+ordinal),
			name,
			typ,
			nullable,
			visibility,
			*computedExpr,
		)
	} else {
		col.Init(
			ordinal,
			cat.StableID(1+ordinal),
			name,
			kind,
			typ,
			nullable,
			visibility,
			defaultExpr,
			computedExpr,
			onUpdateExpr,
			generatedAsIdentityType,
			generatedAsIdentitySequenceOption,
		)
	}
	tt.Columns = append(tt.Columns, col)
}

func (tt *Table) addIndex(def *tree.IndexTableDef, typ indexType) *Index {
	return tt.addIndexWithVersion(def, typ, descpb.PrimaryIndexWithStoredColumnsVersion)
}

func (tt *Table) addIndexWithVersion(
	def *tree.IndexTableDef, typ indexType, version descpb.IndexDescriptorVersion,
) *Index {
	// Add a unique constraint if this is a primary or unique index.
	if typ != nonUniqueIndex {
		tt.addUniqueConstraint(def.Name, def.Columns, def.Predicate, false /* withoutIndex */)
	}

	idx := &Index{
		IdxName:  tt.makeIndexName(def.Name, def.Columns, typ),
		Unique:   typ != nonUniqueIndex,
		Inverted: def.Inverted,
		IdxZone:  &zonepb.ZoneConfig{},
		table:    tt,
		version:  version,
	}

	// Look for name suffixes indicating this is a mutation index.
	if name, ok := extractWriteOnlyIndex(def); ok {
		idx.IdxName = name
		tt.writeOnlyIdxCount++
	} else if name, ok := extractDeleteOnlyIndex(def); ok {
		idx.IdxName = name
		tt.deleteOnlyIdxCount++
	}

	// Add explicit columns. Primary key columns definitions have already been
	// updated to be non-nullable and non-virtual.
	// Add the geoConfig if applicable.
	idx.ExplicitColCount = len(def.Columns)
	notNullIndex := true
	for i, colDef := range def.Columns {
		isLastIndexCol := i == len(def.Columns)-1
		if def.Inverted && isLastIndexCol {
			idx.invertedOrd = i
		}
		col := idx.addColumn(tt, colDef, keyCol, isLastIndexCol)

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

	// Add partitions.
	var partitionBy *tree.PartitionBy
	if def.PartitionByIndex != nil {
		partitionBy = def.PartitionByIndex.PartitionBy
	} else if typ == primaryIndex {
		partitionBy = tt.partitionBy
	}
	if partitionBy != nil {
		ctx := context.Background()
		semaCtx := tree.MakeSemaContext()
		evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

		if len(partitionBy.List) > 0 {
			idx.partitions = make([]Partition, len(partitionBy.List))
			for i := range partitionBy.Fields {
				if i >= len(idx.Columns) || partitionBy.Fields[i] != idx.Columns[i].ColName() {
					panic("partition by columns must be a prefix of the index columns")
				}
			}
			for i := range partitionBy.List {
				p := &partitionBy.List[i]
				idx.partitions[i] = Partition{
					name:   string(p.Name),
					zone:   &zonepb.ZoneConfig{},
					datums: make([]tree.Datums, 0, len(p.Exprs)),
				}

				// Get the partition values.
				for _, e := range p.Exprs {
					d := idx.partitionByListExprToDatums(ctx, &evalCtx, &semaCtx, e)
					if d != nil {
						idx.partitions[i].datums = append(idx.partitions[i].datums, d)
					}
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
			if !pkOrdinals.Contains(i) && col.Kind() != cat.Inverted && !col.IsVirtualComputed() {
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

func (tt *Table) makeIndexName(defName tree.Name, cols tree.IndexElemList, typ indexType) string {
	name := string(defName)
	if name != "" {
		return name
	}

	if typ == primaryIndex {
		return fmt.Sprintf("%s_pkey", tt.TabName.Table())
	}

	var sb strings.Builder
	sb.WriteString(tt.TabName.Table())
	exprCount := 0
	for _, col := range cols {
		sb.WriteRune('_')
		if col.Expr != nil {
			sb.WriteString("expr")
			if exprCount > 0 {
				sb.WriteString(strconv.Itoa(exprCount))
			}
			exprCount++
		} else {
			sb.WriteString(col.Column.String())
		}
	}

	if typ == uniqueIndex {
		sb.WriteString("_key")
	} else {
		sb.WriteString("_idx")
	}

	idxNameExists := func(idxName string) bool {
		for _, idx := range tt.Indexes {
			if idx.IdxName == idxName {
				return true
			}
		}
		return false
	}

	baseName := sb.String()
	name = baseName
	for i := 1; ; i++ {
		if !idxNameExists(name) {
			break
		}
		name = fmt.Sprintf("%s%d", baseName, i)
	}

	return name
}

func (tt *Table) makeUniqueConstraintName(defName tree.Name, columns tree.IndexElemList) string {
	name := string(defName)
	if name == "" {
		var buf bytes.Buffer
		buf.WriteString("unique")
		for i := range columns {
			buf.WriteRune('_')
			buf.WriteString(string(columns[i].Column))
		}
		name = buf.String()
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
// (for inverted and expression indexes).
//
// isLastIndexCol indicates if this is the last explicit column in the index as
// specified in the schema; it is used to indicate the inverted column if the
// index is inverted.
func (ti *Index) addColumn(
	tt *Table, elem tree.IndexElem, colType colType, isLastIndexCol bool,
) *cat.Column {
	var ordinal int
	var colName tree.Name
	if elem.Expr != nil {
		col := columnForIndexElemExpr(tt, elem.Expr)
		ordinal = col.Ordinal()
		colName = col.ColName()
	} else {
		ordinal = tt.FindOrdinal(string(elem.Column))
		colName = elem.Column
	}

	if ti.Inverted && isLastIndexCol {
		// The last column of an inverted index is special: the index key does not
		// contain values from the column itself, but contains inverted index
		// entries derived from that column. Create a virtual column to be able to
		// refer to it separately.
		var col cat.Column
		// TODO(radu,mjibson): update this when the corresponding type in the real
		// catalog is fixed (see sql.newOptTable).
		typ := tt.Columns[ordinal].DatumType()
		col.InitInverted(
			len(tt.Columns),
			colName+"_inverted_key",
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
// existing, inaccessible, VirtualComputed column with the same expression
// exists, it is reused. Otherwise, a new column is added to the table.
func columnForIndexElemExpr(tt *Table, expr tree.Expr) cat.Column {
	exprStr := serializeTableDefExpr(expr)

	// Find an existing, inaccessible, virtual computed column with the same
	// expression.
	for _, col := range tt.Columns {
		if col.IsVirtualComputed() &&
			col.Visibility() == cat.Inaccessible &&
			col.ComputedExprStr() == exprStr {
			return col
		}
	}

	// Add a new virtual computed column with a unique name.
	prefix := "crdb_internal_idx_expr"
	nameExistsFn := func(n tree.Name) bool {
		for _, col := range tt.Columns {
			if col.ColName() == n {
				return true
			}
		}
		return false
	}
	name := tree.Name(prefix)
	for i := 1; nameExistsFn(name); i++ {
		name = tree.Name(fmt.Sprintf("%s_%d", prefix, i))
	}

	typ := typeCheckTableExpr(expr, tt.Columns)
	var col cat.Column
	col.InitVirtualComputed(
		len(tt.Columns),
		cat.StableID(1+len(tt.Columns)),
		name,
		typ,
		true, /* nullable */
		cat.Inaccessible,
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
		if col.Kind() == cat.Inverted {
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

// partitionByListExprToDatums converts an expression from a PARTITION BY LIST
// clause to a list of datums.
func (ti *Index) partitionByListExprToDatums(
	ctx context.Context, evalCtx *tree.EvalContext, semaCtx *tree.SemaContext, e tree.Expr,
) tree.Datums {
	var vals []tree.Expr
	switch t := e.(type) {
	case *tree.Tuple:
		vals = t.Exprs
	default:
		vals = []tree.Expr{e}
	}

	// Cut off at DEFAULT, if present.
	for i := range vals {
		if _, ok := vals[i].(tree.DefaultVal); ok {
			vals = vals[:i]
		}
	}
	if len(vals) == 0 {
		return nil
	}
	d := make(tree.Datums, len(vals))
	for i := range vals {
		c := tree.CastExpr{Expr: vals[i], Type: ti.Columns[i].DatumType()}
		cTyped, err := c.TypeCheck(ctx, semaCtx, types.Any)
		if err != nil {
			panic(err)
		}
		d[i], err = cTyped.Eval(evalCtx)
		if err != nil {
			panic(err)
		}
	}

	// TODO(radu): split into multiple prefixes if Subpartition is also by list.
	// Note that this functionality should be kept in sync with the real catalog
	// implementation (opt_catalog.go).
	return d
}

func extractInaccessibleColumn(def *tree.ColumnTableDef) (name tree.Name, ok bool) {
	if !strings.HasSuffix(string(def.Name), ":inaccessible") {
		return "", false
	}
	return tree.Name(strings.TrimSuffix(string(def.Name), ":inaccessible")), true
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

func serializeGeneratedAsIdentitySequenceOption(seqOpts *tree.SequenceOptions) string {
	return tree.Serialize(seqOpts)
}

// generateDefExprForSequenceBasedCol provides a default expression
// for a column created with an underlying sequence.
func generateDefExprForSequenceBasedCol(
	tableName tree.TableName, colName tree.Name,
) *tree.FuncExpr {
	seqName := tree.NewTableNameWithSchema(
		tableName.CatalogName,
		tableName.SchemaName,
		tree.Name(tableName.Table()+"_"+string(colName)+"_seq"))
	defaultExpr := &tree.FuncExpr{
		Func:  tree.WrapFunction("nextval"),
		Exprs: tree.Exprs{tree.NewStrVal(seqName.String())},
	}
	return defaultExpr
}

// generateDefExprForGeneratedAsIdentityCol provides a default expression
// for an IDENTITY column created with `GENERATED {ALWAYS | BY DEFAULT}
// AS IDENTITY` syntax. The default expression is to show that there is
// an underlying sequence attached to this IDENTITY column.
func generateDefExprForGeneratedAsIdentityCol(
	tableName tree.TableName, colName tree.Name,
) *tree.FuncExpr {
	return generateDefExprForSequenceBasedCol(tableName, colName)
}

// generateDefExprForGeneratedAsIdentityCol provides a default expression
// for an SERIAL column.
func generateDefExprForSerialCol(
	tableName tree.TableName,
	colName tree.Name,
	serialNormalizationMode sessiondatapb.SerialNormalizationMode,
) *tree.FuncExpr {
	switch serialNormalizationMode {
	case sessiondatapb.SerialUsesRowID:
		return &tree.FuncExpr{Func: tree.WrapFunction("unique_rowid")}
	case sessiondatapb.SerialUsesVirtualSequences,
		sessiondatapb.SerialUsesSQLSequences,
		sessiondatapb.SerialUsesCachedSQLSequences:
		return generateDefExprForSequenceBasedCol(tableName, colName)
	default:
		panic(fmt.Errorf("invalid serial normalization mode for col %s in table"+
			" %s", colName, tableName.String()))
	}
}
