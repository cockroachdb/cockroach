// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randgen

import (
	gosql "database/sql"
	"fmt"
	"math"
	"math/rand"
	"strings"

	apd "github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/randident"
	"github.com/cockroachdb/cockroach/pkg/util/randident/randidentcfg"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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

// RandCreateType creates a random CREATE TYPE statement. The resulting
// type's name will be name, and if the type is an enum, the members will
// be random strings generated from alphabet.
func RandCreateType(rng *rand.Rand, name, alphabet string) tree.Statement {
	numLabels := rng.Intn(6) + 1
	labels := make(tree.EnumValueList, numLabels)
	labelsMap := make(map[string]struct{})
	i := 0
	for i < numLabels {
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

// RandCreateTables creates random table definitions.
func RandCreateTables(
	rng *rand.Rand, prefix string, num int, isMultiRegion bool, mutators ...Mutator,
) []tree.Statement {
	if num < 1 {
		panic("at least one table required")
	}

	// Make some random tables.
	tables := make([]tree.Statement, num)
	for i := 0; i < num; i++ {
		t := RandCreateTable(rng, prefix, i+1, isMultiRegion)
		tables[i] = t
	}

	for _, m := range mutators {
		tables, _ = m.Mutate(rng, tables)
	}

	return tables
}

// RandCreateTable creates a random CreateTable definition.
func RandCreateTable(
	rng *rand.Rand, prefix string, tableIdx int, isMultiRegion bool,
) *tree.CreateTable {
	return RandCreateTableWithColumnIndexNumberGenerator(rng, prefix, tableIdx,
		isMultiRegion, true /* allowPartiallyVisibleIndex */, nil /* generateColumnIndexNumber */)
}

var nameGenCfg = func() randidentcfg.Config {
	cfg := randident.DefaultNameGeneratorConfig()
	cfg.Finalize()
	return cfg
}()

// RandCreateTableWithColumnIndexNumberGenerator creates a random CreateTable definition
// using the passed function to generate column index numbers for column names.
func RandCreateTableWithColumnIndexNumberGenerator(
	rng *rand.Rand,
	prefix string,
	tableIdx int,
	isMultiRegion bool,
	allowPartiallyVisibleIndex bool,
	generateColumnIndexNumber func() int64,
) *tree.CreateTable {
	g := randident.NewNameGenerator(&nameGenCfg, rng, prefix)
	name := g.GenerateOne(tableIdx)
	return RandCreateTableWithColumnIndexNumberGeneratorAndName(
		rng, name, tableIdx, isMultiRegion, allowPartiallyVisibleIndex, generateColumnIndexNumber,
	)
}

func RandCreateTableWithName(
	rng *rand.Rand, tableName string, tableIdx int, isMultiRegion bool,
) *tree.CreateTable {
	return RandCreateTableWithColumnIndexNumberGeneratorAndName(
		rng, tableName, tableIdx, isMultiRegion, true /* allowPartiallyVisibleIndex */, nil,
	)
}

func RandCreateTableWithColumnIndexNumberGeneratorAndName(
	rng *rand.Rand,
	tableName string,
	tableIdx int,
	isMultiRegion bool,
	allowPartiallyVisibleIndex bool,
	generateColumnIndexNumber func() int64,
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
	var pk *tree.IndexTableDef
	if rng.Intn(8) != 0 {
		indexDef, ok := randIndexTableDefFromCols(rng, columnDefs, tableName, true /* isPrimaryIndex */, isMultiRegion)
		if ok {
			pk = &indexDef
		}
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
		indexDef, ok := randIndexTableDefFromCols(rng, columnDefs, tableName, false /* isPrimaryIndex */, isMultiRegion)
		if !ok {
			continue
		}
		if indexDef.Inverted && pk != nil {
			// Inverted indexes aren't permitted to be created on primary key columns.
			col := indexDef.Columns[len(indexDef.Columns)-1]
			foundOverlap := false
			for _, pkCol := range pk.Columns {
				if col.Column == pkCol.Column {
					foundOverlap = true
					break
				}
			}
			if foundOverlap {
				continue
			}
		}
		// Make forward indexes unique 50% of the time. Inverted indexes cannot
		// be unique.
		unique := !indexDef.Inverted && rng.Intn(2) == 0
		if unique {
			defs = append(defs, &tree.UniqueConstraintTableDef{
				IndexTableDef: indexDef,
			})
		} else {
			// Due to parsing issue with creating unique indexes in a CREATE TABLE
			// definition, we are only supporting not visible non-unique indexes for
			// rand. Since not visible indexes are pretty rare, we are assigning index
			// visibility randomly with a float [0.0,1.0) 1/6 of the time.
			indexDef.Invisibility.Value = 0.0
			if notvisible := rng.Intn(6) == 0; notvisible {
				indexDef.Invisibility.Value = 1.0
				if allowPartiallyVisibleIndex {
					if rng.Intn(2) == 0 {
						indexDef.Invisibility.Value = 1 - rng.Float64()
						indexDef.Invisibility.FloatProvided = true
					}
				}
			}

			defs = append(defs, &indexDef)
		}
	}

	ret := &tree.CreateTable{
		Table: tree.MakeUnqualifiedTableName(tree.Name(tableName)),
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
// If numRowsInserted == 0, PopulateTableWithRandomData or RandDatum couldn't
// handle this table's schema. Consider increasing numInserts or filing a bug.
// TODO(harding): Populate data in partitions.
func PopulateTableWithRandData(
	rng *rand.Rand, db *gosql.DB, tableName string, numInserts int,
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

	createStatement := fmt.Sprintf("CREATE TABLE %s.%s (%s)", tree.NameString(dbName), tree.NameString(tableName), columns.String())
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
		insertStmt := fmt.Sprintf("INSERT INTO %s.%s VALUES (%s)", tree.NameString(dbName), tree.NameString(tableName), builder.String())
		if _, err := db.Exec(insertStmt); err != nil {
			return err
		}
	}
	return nil
}

// randColumnTableDef produces a random ColumnTableDef for a non-computed
// column, with a random type and nullability.
func randColumnTableDef(rng *rand.Rand, tableIdx int, colIdx int) *tree.ColumnTableDef {
	g := randident.NewNameGenerator(&nameGenCfg, rng, fmt.Sprintf("col%d_", tableIdx))
	colName := g.GenerateOne(colIdx)
	columnDef := &tree.ColumnTableDef{
		// We make a unique name for all columns by prefixing them with the table
		// index to make it easier to reference columns from different tables.
		Name: tree.Name(colName),
		Type: RandColumnType(rng),
	}
	// Slightly prefer non-nullable columns
	if columnDef.Type.(*types.T).Family() == types.OidFamily {
		// Make all OIDs nullable so they're not part of a PK or unique index.
		// Some OID types have a very narrow range of values they accept, which
		// may cause many duplicate row errors.
		columnDef.Nullable.Nullability = tree.RandomNullability(rng, true /* nullableOnly */)
	} else if rng.Intn(2) == 0 {
		// Slightly prefer non-nullable columns
		columnDef.Nullable.Nullability = tree.NotNull
	} else {
		columnDef.Nullable.Nullability = tree.RandomNullability(rng, false /* nullableOnly */)
	}
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
	newDef.Computed.Virtual = rng.Intn(2) == 0

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
	rng *rand.Rand,
	columnTableDefs []*tree.ColumnTableDef,
	tableName string,
	isPrimaryIndex bool,
	isMultiRegion bool,
) (def tree.IndexTableDef, ok bool) {
	cpy := make([]*tree.ColumnTableDef, len(columnTableDefs))
	copy(cpy, columnTableDefs)
	rng.Shuffle(len(cpy), func(i, j int) { cpy[i], cpy[j] = cpy[j], cpy[i] })

	// Determine the number of indexed columns.
	var nCols int
	r := rng.Intn(100)
	switch {
	case r < 50:
		// Create a single-column index 40% of the time. Single-column indexes
		// are more likely then multi-column indexes to be used in query plans
		// for randomly generated queries, so there is some benefit to
		// guaranteeing that they are generated often.
		nCols = 1
	case r < 75:
		nCols = 2
	case r < 90:
		nCols = 3
	default:
		nCols = rng.Intn(len(cpy)) + 1
	}
	if nCols > len(cpy) {
		// nCols cannot be greater than the length of columnTableDefs.
		nCols = len(cpy)
	}

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
	// prefix is the list of columns in the index up until an inverted column, if
	// one exists. stopPrefix is set to true if we find an inverted columnn in the
	// index, after which we stop adding columns to the prefix.
	var prefix tree.NameList
	var stopPrefix bool
	partitioningNotSupported := false

	def.Columns = make(tree.IndexElemList, 0, len(cols))
	for i := range cols {
		semType := tree.MustBeStaticallyKnownType(cols[i].Type)
		if semType.Family() == types.ArrayFamily {
			partitioningNotSupported = true
		}
		elem := tree.IndexElem{
			Column:    cols[i].Name,
			Direction: tree.Direction(rng.Intn(int(tree.Descending) + 1)),
		}

		// Replace the column with an expression 10% of the time.
		if !isPrimaryIndex && len(eligibleExprIndexRefs) > 0 && rng.Intn(10) == 0 {
			var expr tree.Expr
			// Do not allow NULL in expressions to avoid expressions that have
			// an ambiguous type.
			var referencedCols map[tree.Name]struct{}
			expr, semType, _, referencedCols = randExpr(rng, eligibleExprIndexRefs, false /* nullOk */)
			removeColsFromExprIndexRefCols(referencedCols)
			elem.Expr = expr
			elem.Column = ""
			stopPrefix = true
		}

		isLastCol := i == len(cols)-1

		// The non-terminal index columns must be indexable.
		forwardIndexable := colinfo.ColumnTypeIsIndexable(semType)
		invertedIndexable := colinfo.ColumnTypeIsInvertedIndexable(semType)
		if !isLastCol && !forwardIndexable {
			return tree.IndexTableDef{}, false
		}
		if !forwardIndexable && !invertedIndexable {
			return tree.IndexTableDef{}, false
		}

		// The last index column can be inverted-indexable, which makes the
		// index an inverted index.
		if colinfo.ColumnTypeIsOnlyInvertedIndexable(semType) {
			def.Inverted = true
			stopPrefix = true
		} else if isLastCol && !stopPrefix && invertedIndexable {
			// With 1/4 probability, choose to use an inverted index for a column type
			// that is both inverted indexable and forward indexable.
			if rng.Intn(4) == 0 {
				def.Inverted = true
				stopPrefix = true
				if semType.Family() == types.StringFamily {
					elem.OpClass = "gin_trgm_ops"
				}
			}
		}

		// Last column for inverted indexes must always be ascending.
		if i == nCols-1 && def.Inverted {
			elem.Direction = tree.Ascending
		}

		if !stopPrefix {
			prefix = append(prefix, cols[i].Name)
		}

		def.Columns = append(def.Columns, elem)
	}

	// An inverted index column cannot be DESC, so use either the default
	// direction or ASC.
	if def.Inverted {
		dir := tree.Direction(rng.Intn(int(tree.Ascending) + 1))
		def.Columns[len(def.Columns)-1].Direction = dir
	}

	// Partition the secondary index in ~10% of cases. Multi-region databases do
	// not support partitioning.
	// TODO(harding): Allow partitioning the primary index. This will require
	// massaging the syntax.
	if !isMultiRegion && !isPrimaryIndex && !partitioningNotSupported && len(prefix) > 0 && rng.Intn(10) == 0 {
		def.PartitionByIndex = &tree.PartitionByIndex{PartitionBy: &tree.PartitionBy{}}
		prefixLen := 1 + rng.Intn(len(prefix))
		def.PartitionByIndex.Fields = prefix[:prefixLen]

		g := randident.NewNameGenerator(&nameGenCfg, rng, fmt.Sprintf("%s_part", tableName))
		// Add up to 10 partitions.
		numPartitions := rng.Intn(10) + 1
		numExpressions := rng.Intn(10) + 1
		for i := 0; i < numPartitions; i++ {
			var partition tree.ListPartition
			partition.Name = tree.Name(g.GenerateOne(i))
			// Add up to 10 expressions in each partition.
			for j := 0; j < numExpressions; j++ {
				// Use a tuple to contain the expressions in case there are multiple
				// partitioning columns.
				var t tree.Tuple
				t.Exprs = make([]tree.Expr, prefixLen)
				for k := 0; k < prefixLen; k++ {
					colType := tree.MustBeStaticallyKnownType(cols[k].Type)
					// TODO(#82774): Allow null values once #82774 is addressed.
					t.Exprs[k] = RandDatum(rng, colType, false /* nullOk */)
					// Variable expressions are not supported in partitions, and NaN and
					// infinity are considered variable expressions, so if one is
					// generated then regenerate the value.
					if t.Exprs[k].(tree.Datum) == tree.DNull {
						continue
					}
					switch colType.Family() {
					case types.FloatFamily:
						d := float64(*t.Exprs[k].(tree.Datum).(*tree.DFloat))
						if math.IsNaN(d) || math.IsInf(d, 1) || math.IsInf(d, -1) {
							k--
						}
					case types.DecimalFamily:
						d := t.Exprs[k].(tree.Datum).(*tree.DDecimal).Decimal
						if d.Form == apd.NaN || d.Form == apd.Infinite {
							k--
						}
					}
				}
				// Don't include the partition if it matches previous partition values.
				// Include the expressions from this partition, so we also check for
				// duplicates there.
				isDup, err := isDuplicateExpr(t.Exprs, append(def.PartitionByIndex.List, partition))
				if err != nil {
					return def, false
				}
				if isDup {
					continue
				}
				partition.Exprs = append(partition.Exprs, &t)
			}

			if len(partition.Exprs) > 0 {
				def.PartitionByIndex.List = append(def.PartitionByIndex.List, partition)
			}
		}
		// Add a default partition 50% of the time.
		if rng.Intn(2) == 0 {
			var partition tree.ListPartition
			partition.Name = "DEFAULT"
			var t tree.Tuple
			t.Exprs = make([]tree.Expr, prefixLen)
			for i := 0; i < prefixLen; i++ {
				t.Exprs[i] = tree.DefaultVal{}
			}
			partition.Exprs = append(partition.Exprs, &t)
			def.PartitionByIndex.List = append(def.PartitionByIndex.List, partition)
		}
	}

	return def, true
}

func isDuplicateExpr(e tree.Exprs, list []tree.ListPartition) (bool, error) {
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
				cmp, err := tp.Exprs[n].(tree.Datum).CompareError(&eval.Context{}, e[n].(tree.Datum))
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
