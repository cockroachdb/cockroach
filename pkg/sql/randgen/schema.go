// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randgen

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
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

// RandCreateEnumType creates a random CREATE TYPE <type_name> AS ENUM statement.
// The resulting type's name will be name, the enum members will
// be random strings generated from alphabet.
func RandCreateEnumType(rng *rand.Rand, name, alphabet string) tree.Statement {
	numLabels := rng.Intn(6) + 1
	labels := make(tree.EnumValueList, numLabels)
	labelsMap := make(map[string]struct{})

	for i := 0; i < numLabels; {
		s := util.RandString(rng, rng.Intn(6)+1, alphabet)
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

// RandCreateCompositeType creates a random composite type statement.
func RandCreateCompositeType(rng *rand.Rand, name, alphabet string) tree.Statement {
	var compositeTypeList []tree.CompositeTypeElem

	numTypes := rng.Intn(6) + 1
	uniqueNames := make(map[string]struct{})

	for i := 0; i < numTypes; {
		randomName := util.RandString(rng, rng.Intn(6)+1, alphabet)
		randomType := RandTypeFromSlice(rng, types.Scalar)

		if _, ok := uniqueNames[randomName]; !ok {
			compositeTypeList = append(compositeTypeList, tree.CompositeTypeElem{Label: tree.Name(randomName), Type: randomType})
			uniqueNames[randomName] = struct{}{}
			i++
		}
	}

	un, err := tree.NewUnresolvedObjectName(1, [3]string{name}, 0)
	if err != nil {
		panic(err)
	}
	return &tree.CreateType{
		TypeName:          un,
		Variety:           tree.Composite,
		CompositeTypeList: compositeTypeList,
	}
}

// RandCreateTables creates random table definitions.
func RandCreateTables(
	ctx context.Context,
	rng *rand.Rand,
	prefix string,
	num int,
	opts []TableOption,
	mutators ...Mutator,
) []tree.Statement {
	if num < 1 {
		panic("at least one table required")
	}

	// Make some random tables.
	tables := make([]tree.Statement, num)
	for i := 0; i < num; i++ {
		t := RandCreateTable(ctx, rng, prefix, i+1, opts)
		tables[i] = t
	}

	for _, m := range mutators {
		tables, _ = m.Mutate(rng, tables)
	}

	return tables
}

// RandCreateTable creates a random CreateTable definition.
func RandCreateTable(
	ctx context.Context, rng *rand.Rand, prefix string, tableIdx int, opts []TableOption,
) *tree.CreateTable {
	return RandCreateTableWithColumnIndexNumberGenerator(
		ctx, rng, prefix, tableIdx, opts, nil, /* generateColumnIndexNumber */
	)
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
		if rng.Intn(10) < 4 {
			// 40% of the time, use a corner case value
			d = randInterestingDatum(rng, colTypes[j])
		}
		if colTypes[j] == types.RegType {
			// RandDatum is naive to the constraint that a RegType < len(types.OidToType),
			// at least before linking and user defined types are added.
			d = tree.NewDOidWithType(oid.Oid(rng.Intn(len(types.OidToType))), types.RegType)
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
//   - UNIQUE or CHECK constraint violation. RandDatum is naive to these constraints.
//   - Out of range error for a computed INT2 or INT4 column.
//
// If a non-nil inserts is provided, it will be populated with the successful
// insert statements.
//
// If numRowsInserted == 0, PopulateTableWithRandomData or RandDatum couldn't
// handle this table's schema. Consider increasing numInserts or filing a bug.
// TODO(harding): Populate data in partitions.
func PopulateTableWithRandData(
	rng *rand.Rand, db *gosql.DB, tableName string, numInserts int, inserts *[]string,
) (numRowsInserted int, err error) {
	var createStmtSQL string
	res := db.QueryRow(fmt.Sprintf("SELECT create_statement FROM [SHOW CREATE TABLE %s]", tree.NameString(tableName)))
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
			if _, ok := col.Type.(*types.T); !ok {
				return 0, errors.Newf("No type for %v", col)
			}
			colTypes = append(colTypes, tree.MustBeStaticallyKnownType(col.Type))
			nullable = append(nullable, col.Nullable.Nullability == tree.Null)

			colNameBuilder.WriteString(comma)
			colNameBuilder.WriteString(col.Name.String())
			comma = ", "
		}
	}

	for i := 0; i < numInserts; i++ {
		insertVals := generateInsertStmtVals(rng, colTypes, nullable)
		insertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;",
			tree.NameString(tableName),
			colNameBuilder.String(),
			insertVals)
		_, err := db.Exec(insertStmt)
		if err == nil {
			numRowsInserted++
			if inserts != nil {
				*inserts = append(*inserts, insertStmt)
			}
		}
	}
	return numRowsInserted, nil
}

func isDuplicateExpr(ctx context.Context, e tree.Exprs, list []tree.ListPartition) (bool, error) {
	// Iterate over the partitions.
	for _, p := range list {
		// Iterate over the tuples of expressions in the partition.
		for _, tpe := range p.Exprs {
			tp, ok := tpe.(*tree.Tuple)
			if !ok || len(tp.Exprs) != len(e) {
				continue
			}
			n := 0
			// Check each expression in the tuple.
			for ; n < len(tp.Exprs); n++ {
				cmp, err := tp.Exprs[n].(tree.Datum).Compare(ctx, &eval.Context{}, e[n].(tree.Datum))
				if err != nil {
					return false, err
				}
				if cmp != 0 {
					break
				}
			}
			if n == len(tp.Exprs) {
				return true, nil
			}
		}
	}
	return false, nil
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
//   - bool (converts to DBool)
//   - int (converts to DInt)
//   - string (converts to DString)
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
		col := catalog.FindColumnByID(desc, colID)
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
//   - bool (converts to DBool)
//   - int (converts to DInt)
//   - string (converts to DString)
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
		col := catalog.FindColumnByID(desc, colID)
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
