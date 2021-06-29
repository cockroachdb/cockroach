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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

// MakeSchemaName creates a CreateSchema definition.
func MakeSchemaName(
	ifNotExists bool, schema string, authRole security.SQLUsername,
) *tree.CreateSchema {
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
		var interleave *tree.CreateTable
		// 50% chance of interleaving past the first table. Interleaving doesn't
		// make anything harder to do for tests - so prefer to do it a lot.
		if i > 0 && rng.Intn(2) == 0 {
			interleave = tables[rng.Intn(i)].(*tree.CreateTable)
		}
		t := RandCreateTableWithInterleave(rng, prefix, i+1, interleave, nil)
		tables[i] = t
	}

	for _, m := range mutators {
		tables, _ = m.Mutate(rng, tables)
	}

	return tables
}

// RandCreateTable creates a random CreateTable definition.
func RandCreateTable(rng *rand.Rand, prefix string, tableIdx int) *tree.CreateTable {
	return RandCreateTableWithInterleave(rng, prefix, tableIdx, nil, nil)
}

// RandCreateTableWithColumnIndexNumberGenerator creates a random CreateTable definition
// using the passed function to generate column index numbers for column names.
func RandCreateTableWithColumnIndexNumberGenerator(
	rng *rand.Rand, prefix string, tableIdx int, generateColumnIndexNumber func() int64,
) *tree.CreateTable {
	return RandCreateTableWithInterleave(rng, prefix, tableIdx, nil, generateColumnIndexNumber)
}

// RandCreateTableWithInterleave creates a random CreateTable definition,
// interleaved into the given other CreateTable definition.
func RandCreateTableWithInterleave(
	rng *rand.Rand,
	prefix string,
	tableIdx int,
	interleaveInto *tree.CreateTable,
	generateColumnIndexNumber func() int64,
) *tree.CreateTable {
	// columnDefs contains the list of Columns we'll add to our table.
	nColumns := randutil.RandIntInRange(rng, 1, 20)
	columnDefs := make([]*tree.ColumnTableDef, 0, nColumns)
	// defs contains the list of Columns and other attributes (indexes, column
	// families, etc) we'll add to our table.
	defs := make(tree.TableDefs, 0, len(columnDefs))

	// Find columnDefs from previous create table.
	interleaveIntoColumnDefs := make(map[tree.Name]*tree.ColumnTableDef)
	var interleaveIntoPK *tree.UniqueConstraintTableDef
	if interleaveInto != nil {
		for i := range interleaveInto.Defs {
			switch d := interleaveInto.Defs[i].(type) {
			case *tree.ColumnTableDef:
				interleaveIntoColumnDefs[d.Name] = d
			case *tree.UniqueConstraintTableDef:
				if d.PrimaryKey {
					interleaveIntoPK = d
				}
			}
		}
	}

	// colIdx generates numbers that are incorporated into column names.
	colIdx := func(ordinal int) int {
		if generateColumnIndexNumber != nil {
			return int(generateColumnIndexNumber())
		}
		return ordinal
	}

	var interleaveDef *tree.InterleaveDef
	if interleaveIntoPK != nil && len(interleaveIntoPK.Columns) > 0 {
		// Make the interleave prefix, which has to be exactly the columns in the
		// parent's primary index.
		prefixLength := len(interleaveIntoPK.Columns)
		fields := make(tree.NameList, prefixLength)
		for i := range interleaveIntoPK.Columns[:prefixLength] {
			def := interleaveIntoColumnDefs[interleaveIntoPK.Columns[i].Column]
			columnDefs = append(columnDefs, def)
			defs = append(defs, def)
			fields[i] = def.Name
		}

		extraCols := make([]*tree.ColumnTableDef, nColumns)
		// Add more columns to the table.
		for i := range extraCols {
			// Loop until we generate an indexable column type.
			var extraCol *tree.ColumnTableDef
			for {
				extraCol = randColumnTableDef(rng, tableIdx, colIdx(i+prefixLength))
				extraColType := tree.MustBeStaticallyKnownType(extraCol.Type)
				if colinfo.ColumnTypeIsIndexable(extraColType) {
					break
				}
			}
			extraCols[i] = extraCol
			columnDefs = append(columnDefs, extraCol)
			defs = append(defs, extraCol)
		}

		rng.Shuffle(nColumns, func(i, j int) {
			extraCols[i], extraCols[j] = extraCols[j], extraCols[i]
		})

		// Create the primary key to interleave, maybe add some new columns to the
		// one we're interleaving.
		pk := &tree.UniqueConstraintTableDef{
			PrimaryKey: true,
			IndexTableDef: tree.IndexTableDef{
				Columns: interleaveIntoPK.Columns[:prefixLength:prefixLength],
			},
		}
		for i := range extraCols[:rng.Intn(len(extraCols))] {
			pk.Columns = append(pk.Columns, tree.IndexElem{
				Column:    extraCols[i].Name,
				Direction: tree.Direction(rng.Intn(int(tree.Descending) + 1)),
			})
		}
		defs = append(defs, pk)
		interleaveDef = &tree.InterleaveDef{
			Parent: interleaveInto.Table,
			Fields: fields,
		}
	} else {
		// Make new defs from scratch.
		nComputedColumns := randutil.RandIntInRange(rng, 0, (nColumns+1)/2)
		nNormalColumns := nColumns - nComputedColumns
		for i := 0; i < nNormalColumns; i++ {
			columnDef := randColumnTableDef(rng, tableIdx, colIdx(i))
			columnDefs = append(columnDefs, columnDef)
			defs = append(defs, columnDef)
		}

		// Make a random primary key with high likelihood.
		if rng.Intn(8) != 0 {
			indexDef, ok := randIndexTableDefFromCols(rng, columnDefs)
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

		// Make defs for computed columns.
		normalColDefs := columnDefs
		for i := nNormalColumns; i < nColumns; i++ {
			columnDef := randComputedColumnTableDef(rng, normalColDefs, tableIdx, colIdx(i))
			columnDefs = append(columnDefs, columnDef)
			defs = append(defs, columnDef)
		}
	}

	// Make indexes.
	nIdxs := rng.Intn(10)
	for i := 0; i < nIdxs; i++ {
		indexDef, ok := randIndexTableDefFromCols(rng, columnDefs)
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
		Table:      tree.MakeUnqualifiedTableName(tree.Name(fmt.Sprintf("%s%d", prefix, tableIdx))),
		Defs:       defs,
		Interleave: interleaveDef,
	}

	// Create some random column families.
	if rng.Intn(2) == 0 {
		ColumnFamilyMutator(rng, ret)
	}

	// Maybe add some storing columns.
	res, _ := IndexStoringMutator(rng, []tree.Statement{ret})
	return res[0].(*tree.CreateTable)
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

	if rng.Intn(2) == 0 {
		// Try to find a set of numeric columns with the same type; the computed
		// expression will be of the form "a+b+c".
		var cols []*tree.ColumnTableDef
		var fam types.Family
		for _, idx := range rng.Perm(len(normalColDefs)) {
			x := normalColDefs[idx]
			xFam := x.Type.(*types.T).Family()

			if len(cols) == 0 {
				switch xFam {
				case types.IntFamily, types.FloatFamily, types.DecimalFamily:
					fam = xFam
					cols = append(cols, x)
				}
			} else if fam == xFam {
				cols = append(cols, x)
				if len(cols) > 1 && rng.Intn(2) == 0 {
					break
				}
			}
		}
		if len(cols) > 1 {
			// If any of the columns are nullable, set the computed column to be
			// nullable.
			for _, x := range cols {
				if x.Nullable.Nullability != tree.NotNull {
					newDef.Nullable.Nullability = x.Nullable.Nullability
					break
				}
			}

			var expr tree.Expr
			expr = tree.NewUnresolvedName(string(cols[0].Name))
			for _, x := range cols[1:] {
				expr = &tree.BinaryExpr{
					Operator: tree.MakeBinaryOperator(tree.Plus),
					Left:     expr,
					Right:    tree.NewUnresolvedName(string(x.Name)),
				}
			}
			newDef.Type = cols[0].Type
			newDef.Computed.Expr = expr
			return newDef
		}
	}

	// Pick a single column and create a computed column that depends on it.
	// The expression is as follows:
	//  - for numeric types (int, float, decimal), the expression is "x+1";
	//  - for string type, the expression is "lower(x)";
	//  - for types that can be cast to string in computed columns, the expression
	//    is "lower(x::string)";
	//  - otherwise, the expression is `CASE WHEN x IS NULL THEN 'foo' ELSE 'bar'`.
	x := normalColDefs[randutil.RandIntInRange(rng, 0, len(normalColDefs))]
	xTyp := x.Type.(*types.T)

	// Match the nullability of the computed column with the nullability of the
	// reference column.
	newDef.Nullable.Nullability = x.Nullable.Nullability
	nullOk := newDef.Nullable.Nullability != tree.NotNull

	switch xTyp.Family() {
	case types.IntFamily, types.FloatFamily, types.DecimalFamily:
		newDef.Type = xTyp
		newDef.Computed.Expr = &tree.BinaryExpr{
			Operator: tree.MakeBinaryOperator(tree.Plus),
			Left:     tree.NewUnresolvedName(string(x.Name)),
			Right:    RandDatum(rng, xTyp, nullOk),
		}

	case types.StringFamily:
		newDef.Type = types.String
		newDef.Computed.Expr = &tree.FuncExpr{
			Func:  tree.WrapFunction("lower"),
			Exprs: tree.Exprs{tree.NewUnresolvedName(string(x.Name))},
		}

	default:
		volatility, ok := tree.LookupCastVolatility(xTyp, types.String)
		if ok && volatility <= tree.VolatilityImmutable {
			// We can cast to string; use lower(x::string)
			newDef.Type = types.String
			newDef.Computed.Expr = &tree.FuncExpr{
				Func: tree.WrapFunction("lower"),
				Exprs: tree.Exprs{
					&tree.CastExpr{
						Expr: tree.NewUnresolvedName(string(x.Name)),
						Type: types.String,
					},
				},
			}
		} else {
			// We cannot cast this type to string in a computed column expression.
			// Use CASE WHEN x IS NULL THEN 'foo' ELSE 'bar'.
			newDef.Type = types.String
			newDef.Computed.Expr = &tree.CaseExpr{
				Whens: []*tree.When{
					{
						Cond: &tree.IsNullExpr{
							Expr: tree.NewUnresolvedName(string(x.Name)),
						},
						Val: RandDatum(rng, types.String, nullOk),
					},
				},
				Else: RandDatum(rng, types.String, nullOk),
			}
		}
	}

	return newDef
}

// randIndexTableDefFromCols attempts to create an IndexTableDef with a random
// subset of the given columns and a random direction. If unsuccessful, ok=false
// is returned.
func randIndexTableDefFromCols(
	rng *rand.Rand, columnTableDefs []*tree.ColumnTableDef,
) (def tree.IndexTableDef, ok bool) {
	cpy := make([]*tree.ColumnTableDef, len(columnTableDefs))
	copy(cpy, columnTableDefs)
	rng.Shuffle(len(cpy), func(i, j int) { cpy[i], cpy[j] = cpy[j], cpy[i] })
	nCols := rng.Intn(len(cpy)) + 1

	cols := cpy[:nCols]

	def.Columns = make(tree.IndexElemList, 0, len(cols))
	for i := range cols {
		semType := tree.MustBeStaticallyKnownType(cols[i].Type)

		// The non-terminal index columns must be indexable.
		if isLastCol := i == len(cols)-1; !isLastCol && !colinfo.ColumnTypeIsIndexable(semType) {
			return tree.IndexTableDef{}, false
		}

		// The last index column can be inverted-indexable, which makes the
		// index an inverted index.
		if colinfo.ColumnTypeIsInvertedIndexable(semType) {
			def.Inverted = true
		}

		def.Columns = append(def.Columns, tree.IndexElem{
			Column:    cols[i].Name,
			Direction: tree.Direction(rng.Intn(int(tree.Descending) + 1)),
		})
	}

	return def, true
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

	keyPrefix := rowenc.MakeIndexKeyPrefix(codec, desc, index.GetID())
	key, _, err := rowenc.EncodeIndexKey(desc, index, colIDToRowIndex, datums, keyPrefix)
	if err != nil {
		return nil, err
	}
	return key, nil
}
