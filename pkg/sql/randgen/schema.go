// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package randgen

import (
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

// MakeSchemaName creates a CreateSchema definition.
func MakeSchemaName(ifNotExists bool, schema string, authRole tree.RoleSpec) *tree.CreateSchema {
	return &tree.CreateSchema{
		IfNotExists: ifNotExists,
		Schema: tree.ObjectNamePrefix{
			SchemaName:     tree.Name(schema),
			ExplicitSchema: true,
		},
		AuthRole: authRole,
	}
}

// RandCreateType creates a random CREATE TYPE statement. The resulting
// type's name will be name, and if the type is an enum, the members will
// be random strings generated from alphabet.
func RandCreateType(rng *rand.Rand, name, alphabet string) tree.Statement {
	numLabels := rng.Intn(6) + 1
	labels := make(tree.EnumValueList, numLabels)
	labelsMap := make(map[string]struct{})
	i := 0
	for i < numLabels {
		s := RandString(rng, rng.Intn(6)+1, alphabet)
		if _, ok := labelsMap[s]; !ok {
			labels[i] = tree.EnumValue(s)
			labelsMap[s] = struct{}{}
			i++
		}
	}
	un, err := tree.NewUnresolvedObjectName(1, [3]string{name}, 0)
	if err != nil {
		panic(err)
	}
	return &tree.CreateType{
		TypeName:   un,
		Variety:    tree.Enum,
		EnumLabels: labels,
	}
}

// RandCreateTables creates random table definitions.
func RandCreateTables(
	rng *rand.Rand, prefix string, num int, mutators ...Mutator,
) []tree.Statement {
	if num < 1 {
		panic("at least one table required")
	}

	// Make some random tables.
	tables := make([]tree.Statement, num)
	for i := 0; i < num; i++ {
		t := RandCreateTable(rng, prefix, i+1)
		tables[i] = t
	}

	for _, m := range mutators {
		tables, _ = m.Mutate(rng, tables)
	}

	return tables
}

// RandCreateTable creates a random CreateTable definition.
func RandCreateTable(rng *rand.Rand, prefix string, tableIdx int) *tree.CreateTable {
	return RandCreateTableWithColumnIndexNumberGenerator(rng, prefix, tableIdx, nil /* generateColumnIndexNumber */)
}

// RandCreateTableWithColumnIndexNumberGenerator creates a random CreateTable definition
// using the passed function to generate column index numbers for column names.
func RandCreateTableWithColumnIndexNumberGenerator(
	rng *rand.Rand, prefix string, tableIdx int, generateColumnIndexNumber func() int64,
) *tree.CreateTable {
	// columnDefs contains the list of Columns we'll add to our table.
	nColumns := randutil.RandIntInRange(rng, 1, 20)
	columnDefs := make([]*tree.ColumnTableDef, 0, nColumns)
	// defs contains the list of Columns and other attributes (indexes, column
	// families, etc) we'll add to our table.
	defs := make(tree.TableDefs, 0, len(columnDefs))

	// colIdx generates numbers that are incorporated into column names.
	colIdx := func(ordinal int) int {
		if generateColumnIndexNumber != nil {
			return int(generateColumnIndexNumber())
		}
		return ordinal
	}

	// Make new defs from scratch.
	nComputedColumns := randutil.RandIntInRange(rng, 0, (nColumns+1)/2)
	nNormalColumns := nColumns - nComputedColumns
	for i := 0; i < nNormalColumns; i++ {
		columnDef := randColumnTableDef(rng, tableIdx, colIdx(i))
		columnDefs = append(columnDefs, columnDef)
		defs = append(defs, columnDef)
	}

	// Make defs for computed columns.
	normalColDefs := columnDefs
	for i := nNormalColumns; i < nColumns; i++ {
		columnDef := randComputedColumnTableDef(rng, normalColDefs, tableIdx, colIdx(i))
		columnDefs = append(columnDefs, columnDef)
		defs = append(defs, columnDef)
	}

	// Make a random primary key with high likelihood.
	if rng.Intn(8) != 0 {
		indexDef, ok := randIndexTableDefFromCols(rng, columnDefs, false /* allowExpressions */)
		if ok && !indexDef.Inverted {
			defs = append(defs, &tree.UniqueConstraintTableDef{
				PrimaryKey:    true,
				IndexTableDef: indexDef,
			})
		}
		// Although not necessary for Cockroach to function correctly,
		// but for ease of use for any code that introspects on the
		// AST data structure (instead of the descriptor which doesn't
		// exist yet), explicitly set all PK cols as NOT NULL.
		for _, col := range columnDefs {
			for _, elem := range indexDef.Columns {
				if col.Name == elem.Column {
					col.Nullable.Nullability = tree.NotNull
				}
			}
		}
	}

	// Make indexes.
	nIdxs := rng.Intn(10)
	for i := 0; i < nIdxs; i++ {
		indexDef, ok := randIndexTableDefFromCols(rng, columnDefs, true /* allowExpressions */)
		if !ok {
			continue
		}
		// Make forward indexes unique 50% of the time. Inverted indexes cannot
		// be unique.
		unique := !indexDef.Inverted && rng.Intn(2) == 0
		if unique {
			defs = append(defs, &tree.UniqueConstraintTableDef{
				IndexTableDef: indexDef,
			})
		} else {
			defs = append(defs, &indexDef)
		}
	}

	ret := &tree.CreateTable{
		Table: tree.MakeUnqualifiedTableName(tree.Name(fmt.Sprintf("%s%d", prefix, tableIdx))),
		Defs:  defs,
	}

	// Create some random column families.
	if rng.Intn(2) == 0 {
		ColumnFamilyMutator(rng, ret)
	}

	// Maybe add some storing columns.
	res, _ := IndexStoringMutator(rng, []tree.Statement{ret})
	return res[0].(*tree.CreateTable)
}

func parseCreateStatement(createStmtSQL string) (*tree.CreateTable, error) {
	var p parser.Parser
	stmts, err := p.Parse(createStmtSQL)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, errors.Errorf("parsed CreateStatement string yielded more than one parsed statment")
	}
	tableStmt, ok := stmts[0].AST.(*tree.CreateTable)
	if !ok {
		return nil, errors.Errorf("AST could not be cast to *tree.CreateTable")
	}
	return tableStmt, nil
}

// generateInsertStmtVals generates random data for a string builder thats
// used after the VALUES keyword in an INSERT statement.
func generateInsertStmtVals(rng *rand.Rand, colTypes []*types.T, nullable []bool) string {
	var valBuilder strings.Builder
	valBuilder.WriteString("(")
	comma := ""
	for j := 0; j < len(colTypes); j++ {
		valBuilder.WriteString(comma)
		var d tree.Datum
		if rand.Intn(10) < 4 {
			// 40% of the time, use a corner case value
			d = randInterestingDatum(rng, colTypes[j])
		}
		if colTypes[j] == types.RegType {
			// RandDatum is naive to the constraint that a RegType < len(types.OidToType),
			// at least before linking and user defined types are added.
			d = tree.NewDOid(tree.DInt(rand.Intn(len(types.OidToType))))
		}
		if d == nil {
			d = RandDatum(rng, colTypes[j], nullable[j])
		}
		valBuilder.WriteString(tree.AsStringWithFlags(d, tree.FmtParsable))
		comma = ", "
	}
	valBuilder.WriteString(")")
	return valBuilder.String()
}

// TODO(butler): develop new helper function PopulateDatabaseWithRandData which calls
// PopulateTableWithRandData on each table in the order of the fk
// dependency graph.

// PopulateTableWithRandData populates the provided table by executing exactly
// `numInserts` statements. numRowsInserted <= numInserts because inserting into
// an arbitrary table can fail for reasons which include:
//  - UNIQUE or CHECK constraint violation. RandDatum is naive to these constraints.
//  - Out of range error for a computed INT2 or INT4 column.
//
// If numRowsInserted == 0, PopulateTableWithRandomData or RandDatum couldn't
// handle this table's schema. Consider increasing numInserts or filing a bug.
func PopulateTableWithRandData(
	rng *rand.Rand, db *gosql.DB, tableName string, numInserts int,
) (numRowsInserted int, err error) {
	var createStmtSQL string
	res := db.QueryRow(fmt.Sprintf("SELECT create_statement FROM [SHOW CREATE TABLE %s]", tableName))
	err = res.Scan(&createStmtSQL)
	if err != nil {
		return 0, errors.Wrapf(err, "table does not exist in db")
	}
	createStmt, err := parseCreateStatement(createStmtSQL)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to determine table schema")
	}

	// Find columns subject to a foreign key constraint
	var hasFK = map[string]bool{}
	for _, def := range createStmt.Defs {
		if fk, ok := def.(*tree.ForeignKeyConstraintTableDef); ok {
			for _, col := range fk.FromCols {
				hasFK[col.String()] = true
			}
		}
	}

	// Populate helper objects for insert statement creation and error out if a
	// column's constraints will make it impossible to execute random insert
	// statements.

	colTypes := make([]*types.T, 0)
	nullable := make([]bool, 0)
	var colNameBuilder strings.Builder
	comma := ""
	for _, def := range createStmt.Defs {
		if col, ok := def.(*tree.ColumnTableDef); ok {
			if _, ok := hasFK[col.Name.String()]; ok {
				// Given that this function only populates an individual table without
				// considering other tables in the database, populating a column with a
				// foreign key reference with actual data can be nearly impossible. To
				// make inserts pass more frequently, this function skips populating
				// columns with a foreign key reference. Sadly, if these columns with
				// FKs are also NOT NULL, 0 rows will get inserted.

				// TODO(butler): get the unique values from each foreign key reference and
				// populate the column by sampling the FK's unique values.
				if col.Nullable.Nullability == tree.Null {
					continue
				}
			}
			if col.Computed.Computed || col.Hidden {
				// Cannot insert values into hidden or computed columns, so skip adding
				// them to the list of columns to insert data into.
				continue
			}
			colTypes = append(colTypes, tree.MustBeStaticallyKnownType(col.Type.(*types.T)))
			nullable = append(nullable, col.Nullable.Nullability == tree.Null)

			colNameBuilder.WriteString(comma)
			colNameBuilder.WriteString(col.Name.String())
			comma = ", "
		}
	}

	for i := 0; i < numInserts; i++ {
		insertVals := generateInsertStmtVals(rng, colTypes, nullable)
		insertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;",
			tableName,
			colNameBuilder.String(),
			insertVals)
		_, err := db.Exec(insertStmt)
		if err == nil {
			numRowsInserted++
		}
	}
	return numRowsInserted, nil
}

// GenerateRandInterestingTable takes a gosql.DB connection and creates
// a table with all the types in randInterestingDatums and rows of the
// interesting datums.
func GenerateRandInterestingTable(db *gosql.DB, dbName, tableName string) error {
	var (
		randTypes []*types.T
		colNames  []string
	)
	numRows := 0
	for _, v := range randInterestingDatums {
		colTyp := v[0].ResolvedType()
		randTypes = append(randTypes, colTyp)
		colNames = append(colNames, colTyp.Name())
		if len(v) > numRows {
			numRows = len(v)
		}
	}

	var columns strings.Builder
	comma := ""
	for i, typ := range randTypes {
		columns.WriteString(comma)
		columns.WriteString(colNames[i])
		columns.WriteString(" ")
		columns.WriteString(typ.SQLString())
		comma = ", "
	}

	createStatement := fmt.Sprintf("CREATE TABLE %s.%s (%s)", dbName, tableName, columns.String())
	if _, err := db.Exec(createStatement); err != nil {
		return err
	}

	row := make([]string, len(randTypes))
	for i := 0; i < numRows; i++ {
		for j, typ := range randTypes {
			datums := randInterestingDatums[typ.Family()]
			var d tree.Datum
			if i < len(datums) {
				d = datums[i]
			} else {
				d = tree.DNull
			}
			row[j] = tree.AsStringWithFlags(d, tree.FmtParsable)
		}
		var builder strings.Builder
		comma := ""
		for _, d := range row {
			builder.WriteString(comma)
			builder.WriteString(d)
			comma = ", "
		}
		insertStmt := fmt.Sprintf("INSERT INTO %s.%s VALUES (%s)", dbName, tableName, builder.String())
		if _, err := db.Exec(insertStmt); err != nil {
			return err
		}
	}
	return nil
}

// randColumnTableDef produces a random ColumnTableDef for a non-computed
// column, with a random type and nullability.
func randColumnTableDef(rand *rand.Rand, tableIdx int, colIdx int) *tree.ColumnTableDef {
	columnDef := &tree.ColumnTableDef{
		// We make a unique name for all columns by prefixing them with the table
		// index to make it easier to reference columns from different tables.
		Name: tree.Name(fmt.Sprintf("col%d_%d", tableIdx, colIdx)),
		Type: RandColumnType(rand),
	}
	columnDef.Nullable.Nullability = tree.Nullability(rand.Intn(int(tree.SilentNull) + 1))
	return columnDef
}

// randComputedColumnTableDef produces a random ColumnTableDef for a computed
// column (either STORED or VIRTUAL). The computed expressions refer to columns
// in normalColDefs.
func randComputedColumnTableDef(
	rng *rand.Rand, normalColDefs []*tree.ColumnTableDef, tableIdx int, colIdx int,
) *tree.ColumnTableDef {
	newDef := randColumnTableDef(rng, tableIdx, colIdx)
	newDef.Computed.Computed = true
	newDef.Computed.Virtual = (rng.Intn(2) == 0)

	expr, typ, nullability, _ := randExpr(rng, normalColDefs, true /* nullOk */)
	newDef.Computed.Expr = expr
	newDef.Type = typ
	newDef.Nullable.Nullability = nullability

	return newDef
}

// randIndexTableDefFromCols attempts to create an IndexTableDef with a random
// subset of the given columns and a random direction. If unsuccessful, ok=false
// is returned.
func randIndexTableDefFromCols(
	rng *rand.Rand, columnTableDefs []*tree.ColumnTableDef, allowExpressions bool,
) (def tree.IndexTableDef, ok bool) {
	cpy := make([]*tree.ColumnTableDef, len(columnTableDefs))
	copy(cpy, columnTableDefs)
	rng.Shuffle(len(cpy), func(i, j int) { cpy[i], cpy[j] = cpy[j], cpy[i] })
	nCols := rng.Intn(len(cpy)) + 1

	cols := cpy[:nCols]

	// Expression indexes do not currently support references to computed
	// columns, so we only make expressions with non-computed columns. Also,
	// duplicate expressions in an index are not allowed, so columns are removed
	// from the list of eligible columns when they are referenced in an
	// expression. This ensures that no two expressions reference the same
	// columns, therefore no expressions can be duplicated.
	eligibleExprIndexRefs := nonComputedColumnTableDefs(columnTableDefs)
	removeColsFromExprIndexRefCols := func(cols map[tree.Name]struct{}) {
		i := 0
		for j := range eligibleExprIndexRefs {
			eligibleExprIndexRefs[i] = eligibleExprIndexRefs[j]
			name := eligibleExprIndexRefs[j].Name
			if _, ok := cols[name]; !ok {
				i++
			}
		}
		eligibleExprIndexRefs = eligibleExprIndexRefs[:i]
	}

	def.Columns = make(tree.IndexElemList, 0, len(cols))
	for i := range cols {
		semType := tree.MustBeStaticallyKnownType(cols[i].Type)
		elem := tree.IndexElem{
			Column:    cols[i].Name,
			Direction: tree.Direction(rng.Intn(int(tree.Descending) + 1)),
		}

		// Replace the column with an expression 10% of the time.
		if allowExpressions && len(eligibleExprIndexRefs) > 0 && rng.Intn(10) == 0 {
			var expr tree.Expr
			// Do not allow NULL in expressions to avoid expressions that have
			// an ambiguous type.
			var referencedCols map[tree.Name]struct{}
			expr, semType, _, referencedCols = randExpr(rng, eligibleExprIndexRefs, false /* nullOk */)
			removeColsFromExprIndexRefCols(referencedCols)
			elem.Expr = expr
			elem.Column = ""
		}

		// The non-terminal index columns must be indexable.
		if isLastCol := i == len(cols)-1; !isLastCol && !colinfo.ColumnTypeIsIndexable(semType) {
			return tree.IndexTableDef{}, false
		}

		// The last index column can be inverted-indexable, which makes the
		// index an inverted index.
		if colinfo.ColumnTypeIsInvertedIndexable(semType) {
			def.Inverted = true
		}

		def.Columns = append(def.Columns, elem)
	}

	return def, true
}

// nonComputedColumnTableDefs returns a slice containing all the columns in cols
// that are not computed columns.
func nonComputedColumnTableDefs(cols []*tree.ColumnTableDef) []*tree.ColumnTableDef {
	nonComputedCols := make([]*tree.ColumnTableDef, 0, len(cols))
	for _, col := range cols {
		if !col.Computed.Computed {
			nonComputedCols = append(nonComputedCols, col)
		}
	}
	return nonComputedCols
}

// TestingMakePrimaryIndexKey creates a key prefix that corresponds to
// a table row (in the primary index); it is intended for tests.
//
// It is exported because it is used by tests outside of this package.
//
// The value types must match the primary key columns (or a prefix of them);
// supported types are: - Datum
//  - bool (converts to DBool)
//  - int (converts to DInt)
//  - string (converts to DString)
func TestingMakePrimaryIndexKey(
	desc catalog.TableDescriptor, vals ...interface{},
) (roachpb.Key, error) {
	return TestingMakePrimaryIndexKeyForTenant(desc, keys.SystemSQLCodec, vals...)
}

// TestingMakePrimaryIndexKeyForTenant is the same as TestingMakePrimaryIndexKey, but
// allows specification of the codec to use when encoding keys.
func TestingMakePrimaryIndexKeyForTenant(
	desc catalog.TableDescriptor, codec keys.SQLCodec, vals ...interface{},
) (roachpb.Key, error) {
	index := desc.GetPrimaryIndex()
	if len(vals) > index.NumKeyColumns() {
		return nil, errors.Errorf("got %d values, PK has %d columns", len(vals), index.NumKeyColumns())
	}
	datums := make([]tree.Datum, len(vals))
	for i, v := range vals {
		switch v := v.(type) {
		case bool:
			datums[i] = tree.MakeDBool(tree.DBool(v))
		case int:
			datums[i] = tree.NewDInt(tree.DInt(v))
		case string:
			datums[i] = tree.NewDString(v)
		case tree.Datum:
			datums[i] = v
		default:
			return nil, errors.Errorf("unexpected value type %T", v)
		}
		// Check that the value type matches.
		colID := index.GetKeyColumnID(i)
		col, _ := desc.FindColumnWithID(colID)
		if col != nil && col.Public() {
			colTyp := datums[i].ResolvedType()
			if t := colTyp.Family(); t != col.GetType().Family() {
				return nil, errors.Errorf("column %d of type %s, got value of type %s", i, col.GetType().Family(), t)
			}
		}
	}
	// Create the ColumnID to index in datums slice map needed by
	// MakeIndexKeyPrefix.
	var colIDToRowIndex catalog.TableColMap
	for i := range vals {
		colIDToRowIndex.Set(index.GetKeyColumnID(i), i)
	}

	keyPrefix := rowenc.MakeIndexKeyPrefix(codec, desc.GetID(), index.GetID())
	key, _, err := rowenc.EncodeIndexKey(desc, index, colIDToRowIndex, datums, keyPrefix)
	if err != nil {
		return nil, err
	}
	return key, nil
}

// TestingMakeSecondaryIndexKey creates a key prefix that corresponds to
// a secondary index; it is intended for tests.
//
// It is exported because it is used by tests outside of this package.
//
// The value types must match the secondary key columns,
// supported types are: - Datum
//  - bool (converts to DBool)
//  - int (converts to DInt)
//  - string (converts to DString)
func TestingMakeSecondaryIndexKey(
	desc catalog.TableDescriptor, index catalog.Index, codec keys.SQLCodec, vals ...interface{},
) (roachpb.Key, error) {
	if len(vals) > index.NumKeyColumns() {
		return nil, errors.Errorf("got %d values, index %s has %d columns", len(vals), index.GetName(), index.NumKeyColumns())
	}

	datums := make([]tree.Datum, len(vals))
	for i, v := range vals {
		switch v := v.(type) {
		case bool:
			datums[i] = tree.MakeDBool(tree.DBool(v))
		case int:
			datums[i] = tree.NewDInt(tree.DInt(v))
		case string:
			datums[i] = tree.NewDString(v)
		case tree.Datum:
			datums[i] = v
		default:
			return nil, errors.Errorf("unexpected value type %T", v)
		}
		// Check that the value type matches.
		colID := index.GetKeyColumnID(i)
		col, _ := desc.FindColumnWithID(colID)
		if col != nil && col.Public() {
			colTyp := datums[i].ResolvedType()
			if t := colTyp.Family(); t != col.GetType().Family() {
				return nil, errors.Errorf("column %d of type %s, got value of type %s", i, col.GetType().Family(), t)
			}
		}
	}
	// Create the ColumnID to index in datums slice map needed by
	// MakeIndexKeyPrefix.
	var colIDToRowIndex catalog.TableColMap
	for i := range vals {
		colIDToRowIndex.Set(index.GetKeyColumnID(i), i)
	}

	keyPrefix := rowenc.MakeIndexKeyPrefix(codec, desc.GetID(), index.GetID())
	key, _, err := rowenc.EncodeIndexKey(desc, index, colIDToRowIndex, datums, keyPrefix)

	if err != nil {
		return nil, err
	}
	return key, nil
}
