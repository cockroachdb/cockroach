// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randgen

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"

	apd "github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/randident"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// RandCreateTableWithColumnIndexNumberGenerator creates a random CreateTable definition
// using the passed function to generate column index numbers for column names.
func RandCreateTableWithColumnIndexNumberGenerator(
	ctx context.Context,
	rng *rand.Rand,
	prefix string,
	tableIdx int,
	opts []TableOption,
	generateColumnIndexSuffix func() string,
) *tree.CreateTable {
	options := applyOptions(opts)
	var name string
	if options.crazyNames {
		g := randident.NewNameGenerator(&nameGenCfg, rng, prefix)
		name = g.GenerateOne(strconv.Itoa(tableIdx))
	} else {
		name = fmt.Sprintf("%s%d", prefix, tableIdx)
	}
	return randTableWithIndexes(
		ctx, rng, name, tableIdx, opts, generateColumnIndexSuffix,
	)
}

func RandCreateTableWithName(
	ctx context.Context, rng *rand.Rand, tableName string, tableIdx int, opts []TableOption,
) *tree.CreateTable {
	return randTableWithIndexes(
		ctx, rng, tableName, tableIdx, opts, nil, /* generateColumnIndexSuffix */
	)
}

func randTableWithIndexes(
	ctx context.Context,
	rng *rand.Rand,
	tableName string,
	tableIdx int,
	opts []TableOption,
	generateColumnIndexSuffix func() string,
) *tree.CreateTable {
	options := applyOptions(opts)
	// columnDefs contains the list of Columns we'll add to our table.
	nColumns := randutil.RandIntInRange(rng, 1, 20)
	columnDefs := make([]*tree.ColumnTableDef, 0, nColumns)
	// defs contains the list of Columns and other attributes (indexes, column
	// families, etc) we'll add to our table.
	defs := make(tree.TableDefs, 0, len(columnDefs))

	// colIdx generates numbers that are incorporated into column names.
	colSuffix := func(ordinal int) string {
		if generateColumnIndexSuffix != nil {
			return generateColumnIndexSuffix()
		}
		return strconv.Itoa(ordinal)
	}

	// Make new defs from scratch.
	nComputedColumns := randutil.RandIntInRange(rng, 0, (nColumns+1)/2)
	nNormalColumns := nColumns - nComputedColumns
	for i := 0; i < nNormalColumns; i++ {
		columnDef := randColumnTableDef(rng, tableIdx, colSuffix(i), opts)
		columnDefs = append(columnDefs, columnDef)
		defs = append(defs, columnDef)
	}

	// Make defs for computed columns.
	normalColDefs := columnDefs
	for i := nNormalColumns; i < nColumns; i++ {
		columnDef := randComputedColumnTableDef(rng, normalColDefs, tableIdx, colSuffix(i), opts)
		columnDefs = append(columnDefs, columnDef)
		defs = append(defs, columnDef)
	}

	// Make a random primary key with high likelihood.
	var pk *tree.IndexTableDef
	if options.primaryIndexRequired || (rng.Intn(8) != 0) {
		for {
			indexDef, ok := randIndexTableDefFromCols(ctx, rng, columnDefs, tableName, true /* isPrimaryIndex */, opts)
			canUseIndex := ok && indexDef.Type.CanBePrimary()
			if canUseIndex {
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
				pk = &indexDef
				defs = append(defs, &tree.UniqueConstraintTableDef{
					PrimaryKey:    true,
					IndexTableDef: indexDef,
				})
			}
			if canUseIndex || !options.primaryIndexRequired {
				break
			}
		}
	}

	// Make indexes.
	nIdxs := rng.Intn(10)
	for i := 0; i < nIdxs; i++ {
		indexDef, ok := randIndexTableDefFromCols(ctx, rng, columnDefs, tableName, false /* isPrimaryIndex */, opts)
		if !ok {
			continue
		}
		if !indexDef.Type.CanBePrimary() && pk != nil {
			// Inverted/vector indexes aren't permitted to be created on primary
			// key columns.
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
		// Make forward indexes unique 50% of the time. Other index types cannot
		// be unique.
		unique := indexDef.Type.CanBeUnique() && rng.Intn(2) == 0
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
				if options.allowPartiallyVisibleIndex {
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
	if !options.skipColumnFamilyMutations && rng.Intn(2) == 0 {
		ColumnFamilyMutator(rng, ret)
	}

	// Maybe add some storing columns.
	res, _ := IndexStoringMutator(rng, []tree.Statement{ret})
	return res[0].(*tree.CreateTable)
}

// randColumnTableDef produces a random ColumnTableDef for a non-computed
// column, with a random type and nullability.
func randColumnTableDef(
	rng *rand.Rand, tableIdx int, colSuffix string, opts []TableOption,
) *tree.ColumnTableDef {
	options := applyOptions(opts)
	var colName tree.Name
	if options.crazyNames {
		g := randident.NewNameGenerator(&nameGenCfg, rng, fmt.Sprintf("col%d", tableIdx))
		colName = tree.Name(g.GenerateOne(colSuffix))
	} else {
		colName = tree.Name(fmt.Sprintf("col%d_%s", tableIdx, colSuffix))
	}
	columnDef := &tree.ColumnTableDef{
		// We make a unique name for all columns by prefixing them with the table
		// index to make it easier to reference columns from different tables.
		Name: colName,
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
	rng *rand.Rand,
	normalColDefs []*tree.ColumnTableDef,
	tableIdx int,
	colSuffix string,
	opts []TableOption,
) *tree.ColumnTableDef {
	newDef := randColumnTableDef(rng, tableIdx, colSuffix, opts)
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
	ctx context.Context,
	rng *rand.Rand,
	columnTableDefs []*tree.ColumnTableDef,
	tableName string,
	isPrimaryIndex bool,
	opts []TableOption,
) (def tree.IndexTableDef, ok bool) {
	options := applyOptions(opts)
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
			def.Type = idxtype.INVERTED
			stopPrefix = true
		} else if isLastCol && !stopPrefix && invertedIndexable {
			// With 1/4 probability, choose to use an inverted index for a column type
			// that is both inverted indexable and forward indexable.
			if rng.Intn(4) == 0 {
				def.Type = idxtype.INVERTED
				stopPrefix = true
				if semType.Family() == types.StringFamily {
					elem.OpClass = "gin_trgm_ops"
				}
			}
		}

		// Last column for inverted indexes must always be ascending.
		if i == nCols-1 && def.Type == idxtype.INVERTED {
			elem.Direction = tree.Ascending
		}

		if !stopPrefix {
			prefix = append(prefix, cols[i].Name)
		}

		def.Columns = append(def.Columns, elem)
	}

	// An inverted index column cannot be DESC, so use either the default
	// direction or ASC.
	if def.Type == idxtype.INVERTED {
		dir := tree.Direction(rng.Intn(int(tree.Ascending) + 1))
		def.Columns[len(def.Columns)-1].Direction = dir
	}

	// Partition the secondary index in ~10% of cases. Multi-region databases do
	// not support partitioning.
	// TODO(harding): Allow partitioning the primary index. This will require
	// massaging the syntax.
	if !options.multiRegion && !isPrimaryIndex && !partitioningNotSupported && len(prefix) > 0 && rng.Intn(10) == 0 {
		def.PartitionByIndex = &tree.PartitionByIndex{PartitionBy: &tree.PartitionBy{}}
		prefixLen := 1 + rng.Intn(len(prefix))
		def.PartitionByIndex.Fields = prefix[:prefixLen]

		g := randident.NewNameGenerator(&nameGenCfg, rng, fmt.Sprintf("%s_part", tableName))
		// Add up to 10 partitions.
		numPartitions := rng.Intn(10) + 1
		numExpressions := rng.Intn(10) + 1
		for i := 0; i < numPartitions; i++ {
			var partition tree.ListPartition
			if options.crazyNames {
				partition.Name = tree.Name(g.GenerateOne(strconv.Itoa(i)))
			} else {
				partition.Name = tree.Name(fmt.Sprintf("%s_part_%d", tableName, i))
			}
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
				isDup, err := isDuplicateExpr(ctx, t.Exprs, append(def.PartitionByIndex.List, partition))
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
