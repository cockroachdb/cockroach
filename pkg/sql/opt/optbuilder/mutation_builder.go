// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// mutationBuilder is a helper struct that supports building Insert, Update,
// Upsert, and Delete operators in stages.
// TODO(andyk): Add support for Delete.
type mutationBuilder struct {
	b  *Builder
	md *opt.Metadata

	// opName is the statement's name, used in error messages.
	opName string

	// tab is the target table.
	tab cat.Table

	// tabID is the metadata ID of the table.
	tabID opt.TableID

	// alias is the table alias specified in the mutation statement, or just the
	// resolved table name if no alias was specified.
	alias tree.TableName

	// outScope contains the current set of columns that are in scope, as well as
	// the output expression as it is incrementally built. Once the final mutation
	// expression is completed, it will be contained in outScope.expr.
	outScope *scope

	// fetchScope contains the set of columns fetched from the target table.
	fetchScope *scope

	// targetColList is an ordered list of IDs of the table columns into which
	// values will be inserted, or which will be updated with new values. It is
	// incrementally built as the mutation operator is built.
	targetColList opt.ColList

	// targetColSet contains the same column IDs as targetColList, but as a set.
	targetColSet opt.ColSet

	// insertColIDs lists the input column IDs providing values to insert. Its
	// length is always equal to the number of columns in the target table,
	// including mutation columns. Table columns which will not have values
	// inserted are set to 0 (e.g. delete-only mutation columns). insertColIDs
	// is empty if this is not an Insert/Upsert operator.
	insertColIDs opt.OptionalColList

	// fetchColIDs lists the input column IDs storing values which are fetched
	// from the target table in order to provide existing values that will form
	// lookup and update values. Its length is always equal to the number of
	// columns in the target table, including mutation columns. Table columns
	// which do not need to be fetched are set to 0. fetchColIDs is empty if
	// this is an Insert operator.
	fetchColIDs opt.OptionalColList

	// updateColIDs lists the input column IDs providing update values. Its
	// length is always equal to the number of columns in the target table,
	// including mutation columns. Table columns which do not need to be
	// updated are set to 0.
	updateColIDs opt.OptionalColList

	// upsertColIDs lists the input column IDs that choose between an insert or
	// update column using a CASE expression:
	//
	//   CASE WHEN canary_col IS NULL THEN ins_col ELSE upd_col END
	//
	// These columns are used to compute constraints and to return result rows.
	// The length of upsertColIDs is always equal to the number of columns in
	// the target table, including mutation columns. Table columns which do not
	// need to be updated are set to 0. upsertColIDs is empty if this is not
	// an Upsert operator.
	upsertColIDs opt.OptionalColList

	// checkColIDs lists the input column IDs storing the boolean results of
	// evaluating check constraint expressions defined on the target table. Its
	// length is always equal to the number of check constraints on the table
	// (see opt.Table.CheckCount).
	checkColIDs opt.OptionalColList

	// partialIndexPutColIDs lists the input column IDs storing the boolean
	// results of evaluating partial index predicate expressions of the target
	// table. The predicate expressions are evaluated with their variables
	// assigned from newly inserted or updated row values. When these columns
	// evaluate to true, it signifies that the inserted or updated row should be
	// added to the corresponding partial index. The length of
	// partialIndexPutColIDs is always equal to the number of partial indexes on
	// the table.
	partialIndexPutColIDs opt.OptionalColList

	// partialIndexDelColIDs lists the input column IDs storing the boolean
	// results of evaluating partial index predicate expressions of the target
	// table. The predicate expressions are evaluated with their variables
	// assigned from existing row values of deleted or updated rows. When these
	// columns evaluate to true, it signifies that the deleted or updated row
	// should be removed from the corresponding partial index. The length of
	// partialIndexPutColIDs is always equal to the number of partial indexes on
	// the table.
	partialIndexDelColIDs opt.OptionalColList

	// canaryColID is the ID of the column that is used to decide whether to
	// insert or update each row. If the canary column's value is null, then it's
	// an insert; otherwise it's an update.
	canaryColID opt.ColumnID

	// arbiters is the set of indexes and unique constraints that are used to
	// detect conflicts for UPSERT and INSERT ON CONFLICT statements.
	arbiters arbiterSet

	// roundedDecimalCols is the set of columns that have already been rounded.
	// Keeping this set avoids rounding the same column multiple times.
	roundedDecimalCols opt.ColSet

	// subqueries temporarily stores subqueries that were built during initial
	// analysis of SET expressions. They will be used later when the subqueries
	// are joined into larger LEFT OUTER JOIN expressions.
	subqueries []*scope

	// parsedColExprs is a cached set of parsed default and computed expressions
	// from the table schema. These are parsed once and cached for reuse.
	parsedColExprs []tree.Expr

	// parsedIndexExprs is a cached set of parsed partial index predicate
	// expressions from the table schema. These are parsed once and cached for
	// reuse.
	parsedIndexExprs []tree.Expr

	// parsedUniqueConstraintExprs is a cached set of parsed partial unique
	// constraint predicate expressions from the table schema. These are parsed
	// once and cached for reuse.
	parsedUniqueConstraintExprs []tree.Expr

	// uniqueChecks contains unique check queries; see buildUnique* methods.
	uniqueChecks memo.UniqueChecksExpr

	// fkChecks contains foreign key check queries; see buildFK* methods.
	fkChecks memo.FKChecksExpr

	// cascades contains foreign key check cascades; see buildFK* methods.
	cascades memo.FKCascades

	// withID is nonzero if we need to buffer the input for FK or uniqueness
	// checks.
	withID opt.WithID

	// extraAccessibleCols stores all the columns that are available to the
	// mutation that are not part of the target table. This is useful for
	// UPDATE ... FROM queries, as the columns from the FROM tables must be
	// made accessible to the RETURNING clause.
	extraAccessibleCols []scopeColumn

	// fkCheckHelper is used to prevent allocating the helper separately.
	fkCheckHelper fkCheckHelper

	// uniqueCheckHelper is used to prevent allocating the helper separately.
	uniqueCheckHelper uniqueCheckHelper

	// arbiterPredicateHelper is used to prevent allocating the helper
	// separately.
	arbiterPredicateHelper arbiterPredicateHelper
}

func (mb *mutationBuilder) init(b *Builder, opName string, tab cat.Table, alias tree.TableName) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*mb = mutationBuilder{
		b:      b,
		md:     b.factory.Metadata(),
		opName: opName,
		tab:    tab,
		alias:  alias,
	}

	n := tab.ColumnCount()
	mb.targetColList = make(opt.ColList, 0, n)

	// Allocate segmented array of column IDs.
	numPartialIndexes := partialIndexCount(tab)
	colIDs := make(opt.OptionalColList, n*4+tab.CheckCount()+2*numPartialIndexes)
	mb.insertColIDs = colIDs[:n]
	mb.fetchColIDs = colIDs[n : n*2]
	mb.updateColIDs = colIDs[n*2 : n*3]
	mb.upsertColIDs = colIDs[n*3 : n*4]
	mb.checkColIDs = colIDs[n*4 : n*4+tab.CheckCount()]
	mb.partialIndexPutColIDs = colIDs[n*4+tab.CheckCount() : n*4+tab.CheckCount()+numPartialIndexes]
	mb.partialIndexDelColIDs = colIDs[n*4+tab.CheckCount()+numPartialIndexes:]

	// Add the table and its columns (including mutation columns) to metadata.
	mb.tabID = mb.md.AddTable(tab, &mb.alias)
}

// setFetchColIDs sets the list of columns that are fetched in order to provide
// values to the mutation operator. The given columns must come from buildScan.
func (mb *mutationBuilder) setFetchColIDs(cols []scopeColumn) {
	for i := range cols {
		// Ensure that we don't add system columns to the fetch columns.
		if cols[i].kind != cat.System {
			mb.fetchColIDs[cols[i].tableOrdinal] = cols[i].id
		}
	}
}

// buildInputForUpdate constructs a Select expression from the fields in
// the Update operator, similar to this:
//
//   SELECT <cols>
//   FROM <table>
//   WHERE <where>
//   ORDER BY <order-by>
//   LIMIT <limit>
//
// All columns from the table to update are added to fetchColList.
// If a FROM clause is defined, we build out each of the table
// expressions required and JOIN them together (LATERAL joins between
// the tables are allowed). We then JOIN the result with the target
// table (the FROM tables can't reference this table) and apply the
// appropriate WHERE conditions.
//
// It is the responsibility of the user to guarantee that the JOIN
// produces a maximum of one row per row of the target table. If multiple
// are found, an arbitrary one is chosen (this row is not readily
// predictable, consistent with the POSTGRES implementation).
// buildInputForUpdate stores the columns of the FROM tables in the
// mutation builder so they can be made accessible to other parts of
// the query (RETURNING clause).
// TODO(andyk): Do needed column analysis to project fewer columns if possible.
func (mb *mutationBuilder) buildInputForUpdate(
	inScope *scope,
	texpr tree.TableExpr,
	from tree.TableExprs,
	where *tree.Where,
	limit *tree.Limit,
	orderBy tree.OrderBy,
) {
	var indexFlags *tree.IndexFlags
	if source, ok := texpr.(*tree.AliasedTableExpr); ok && source.IndexFlags != nil {
		indexFlags = source.IndexFlags
		telemetry.Inc(sqltelemetry.IndexHintUseCounter)
		telemetry.Inc(sqltelemetry.IndexHintUpdateUseCounter)
	}

	// Fetch columns from different instance of the table metadata, so that it's
	// possible to remap columns, as in this example:
	//
	//   UPDATE abc SET a=b
	//
	// NOTE: Include mutation columns, but be careful to never use them for any
	//       reason other than as "fetch columns". See buildScan comment.
	mb.fetchScope = mb.b.buildScan(
		mb.b.addTable(mb.tab, &mb.alias),
		tableOrdinals(mb.tab, columnKinds{
			includeMutations:       true,
			includeSystem:          true,
			includeVirtualInverted: false,
			includeVirtualComputed: true,
		}),
		indexFlags,
		noRowLocking,
		inScope,
	)

	// Set list of columns that will be fetched by the input expression.
	mb.setFetchColIDs(mb.fetchScope.cols)

	// If there is a FROM clause present, we must join all the tables
	// together with the table being updated.
	fromClausePresent := len(from) > 0
	if fromClausePresent {
		fromScope := mb.b.buildFromTables(from, noRowLocking, inScope)

		// Check that the same table name is not used multiple times.
		mb.b.validateJoinTableNames(mb.fetchScope, fromScope)

		// The FROM table columns can be accessed by the RETURNING clause of the
		// query and so we have to make them accessible.
		mb.extraAccessibleCols = fromScope.cols

		// Add the columns in the FROM scope.
		// We create a new scope so that fetchScope is not modified. It will be
		// used later to build partial index predicate expressions, and we do
		// not want ambiguities with column names in the FROM clause.
		mb.outScope = mb.fetchScope.replace()
		mb.outScope.appendColumnsFromScope(mb.fetchScope)
		mb.outScope.appendColumnsFromScope(fromScope)

		left := mb.fetchScope.expr.(memo.RelExpr)
		right := fromScope.expr.(memo.RelExpr)
		mb.outScope.expr = mb.b.factory.ConstructInnerJoin(left, right, memo.TrueFilter, memo.EmptyJoinPrivate)
	} else {
		mb.outScope = mb.fetchScope
	}

	// WHERE
	mb.b.buildWhere(where, mb.outScope)

	// SELECT + ORDER BY (which may add projected expressions)
	projectionsScope := mb.outScope.replace()
	projectionsScope.appendColumnsFromScope(mb.outScope)
	orderByScope := mb.b.analyzeOrderBy(orderBy, mb.outScope, projectionsScope, tree.RejectGenerators)
	mb.b.buildOrderBy(mb.outScope, projectionsScope, orderByScope)
	mb.b.constructProjectForScope(mb.outScope, projectionsScope)

	// LIMIT
	if limit != nil {
		mb.b.buildLimit(limit, inScope, projectionsScope)
	}

	mb.outScope = projectionsScope

	// Build a distinct on to ensure there is at most one row in the joined output
	// for every row in the table.
	if fromClausePresent {
		var pkCols opt.ColSet

		// We need to ensure that the join has a maximum of one row for every row
		// in the table and we ensure this by constructing a distinct on the primary
		// key columns.
		primaryIndex := mb.tab.Index(cat.PrimaryIndex)
		for i := 0; i < primaryIndex.KeyColumnCount(); i++ {
			// If the primary key column is hidden, then we don't need to use it
			// for the distinct on.
			// TODO(radu): this logic seems fragile, is it assuming that only an
			// implicit `rowid` column can be a hidden PK column?
			if col := primaryIndex.Column(i); col.Visibility() != cat.Hidden {
				pkCols.Add(mb.fetchColIDs[col.Ordinal()])
			}
		}

		if !pkCols.Empty() {
			mb.outScope = mb.b.buildDistinctOn(
				pkCols, mb.outScope, false /* nullsAreDistinct */, "" /* errorOnDup */)
		}
	}
}

// buildInputForDelete constructs a Select expression from the fields in
// the Delete operator, similar to this:
//
//   SELECT <cols>
//   FROM <table>
//   WHERE <where>
//   ORDER BY <order-by>
//   LIMIT <limit>
//
// All columns from the table to update are added to fetchColList.
// TODO(andyk): Do needed column analysis to project fewer columns if possible.
func (mb *mutationBuilder) buildInputForDelete(
	inScope *scope, texpr tree.TableExpr, where *tree.Where, limit *tree.Limit, orderBy tree.OrderBy,
) {
	var indexFlags *tree.IndexFlags
	if source, ok := texpr.(*tree.AliasedTableExpr); ok && source.IndexFlags != nil {
		indexFlags = source.IndexFlags
		telemetry.Inc(sqltelemetry.IndexHintUseCounter)
		telemetry.Inc(sqltelemetry.IndexHintDeleteUseCounter)
	}

	// Fetch columns from different instance of the table metadata, so that it's
	// possible to remap columns, as in this example:
	//
	//   DELETE FROM abc WHERE a=b
	//
	// NOTE: Include mutation columns, but be careful to never use them for any
	//       reason other than as "fetch columns". See buildScan comment.
	// TODO(andyk): Why does execution engine need mutation columns for Delete?
	mb.fetchScope = mb.b.buildScan(
		mb.b.addTable(mb.tab, &mb.alias),
		tableOrdinals(mb.tab, columnKinds{
			includeMutations:       true,
			includeSystem:          true,
			includeVirtualInverted: false,
			includeVirtualComputed: true,
		}),
		indexFlags,
		noRowLocking,
		inScope,
	)
	mb.outScope = mb.fetchScope

	// WHERE
	mb.b.buildWhere(where, mb.outScope)

	// SELECT + ORDER BY (which may add projected expressions)
	projectionsScope := mb.outScope.replace()
	projectionsScope.appendColumnsFromScope(mb.outScope)
	orderByScope := mb.b.analyzeOrderBy(orderBy, mb.outScope, projectionsScope, tree.RejectGenerators)
	mb.b.buildOrderBy(mb.outScope, projectionsScope, orderByScope)
	mb.b.constructProjectForScope(mb.outScope, projectionsScope)

	// LIMIT
	if limit != nil {
		mb.b.buildLimit(limit, inScope, projectionsScope)
	}

	mb.outScope = projectionsScope

	// Set list of columns that will be fetched by the input expression.
	mb.setFetchColIDs(mb.outScope.cols)
}

// addTargetColsByName adds one target column for each of the names in the given
// list.
func (mb *mutationBuilder) addTargetColsByName(names tree.NameList) {
	for _, name := range names {
		// Determine the ordinal position of the named column in the table and
		// add it as a target column.
		if ord := findPublicTableColumnByName(mb.tab, name); ord != -1 {
			// System columns are invalid target columns.
			if mb.tab.Column(ord).Kind() == cat.System {
				panic(pgerror.Newf(pgcode.InvalidColumnReference, "cannot modify system column %q", name))
			}
			mb.addTargetCol(ord)
			continue
		}
		panic(colinfo.NewUndefinedColumnError(string(name)))
	}
}

// addTargetCol adds a target column by its ordinal position in the target
// table. It raises an error if a mutation or computed column is targeted, or if
// the same column is targeted multiple times.
func (mb *mutationBuilder) addTargetCol(ord int) {
	tabCol := mb.tab.Column(ord)

	// Don't allow targeting of mutation columns.
	if tabCol.IsMutation() {
		panic(makeBackfillError(tabCol.ColName()))
	}

	// Computed columns cannot be targeted with input values.
	if tabCol.IsComputed() {
		panic(schemaexpr.CannotWriteToComputedColError(string(tabCol.ColName())))
	}

	// Ensure that the name list does not contain duplicates.
	colID := mb.tabID.ColumnID(ord)
	if mb.targetColSet.Contains(colID) {
		panic(pgerror.Newf(pgcode.Syntax,
			"multiple assignments to the same column %q", tabCol.ColName()))
	}
	mb.targetColSet.Add(colID)

	mb.targetColList = append(mb.targetColList, colID)
}

// extractValuesInput tests whether the given input is a VALUES clause with no
// WITH, ORDER BY, or LIMIT modifier. If so, it's returned, otherwise nil is
// returned.
func (mb *mutationBuilder) extractValuesInput(inputRows *tree.Select) *tree.ValuesClause {
	if inputRows == nil {
		return nil
	}

	// Only extract a simple VALUES clause with no modifiers.
	if inputRows.With != nil || inputRows.OrderBy != nil || inputRows.Limit != nil {
		return nil
	}

	// Discard parentheses.
	if parens, ok := inputRows.Select.(*tree.ParenSelect); ok {
		return mb.extractValuesInput(parens.Select)
	}

	if values, ok := inputRows.Select.(*tree.ValuesClause); ok {
		return values
	}

	return nil
}

// replaceDefaultExprs looks for DEFAULT specifiers in input value expressions
// and replaces them with the corresponding default value expression for the
// corresponding column. This is only possible when the input is a VALUES
// clause. For example:
//
//   INSERT INTO t (a, b) (VALUES (1, DEFAULT), (DEFAULT, 2))
//
// Here, the two DEFAULT specifiers are replaced by the default value expression
// for the a and b columns, respectively.
//
// replaceDefaultExprs returns a VALUES expression with replaced DEFAULT values,
// or just the unchanged input expression if there are no DEFAULT values.
func (mb *mutationBuilder) replaceDefaultExprs(inRows *tree.Select) (outRows *tree.Select) {
	values := mb.extractValuesInput(inRows)
	if values == nil {
		return inRows
	}

	// Ensure that the number of input columns exactly matches the number of
	// target columns.
	numCols := len(values.Rows[0])
	mb.checkNumCols(len(mb.targetColList), numCols)

	var newRows []tree.Exprs
	for irow, tuple := range values.Rows {
		if len(tuple) != numCols {
			reportValuesLenError(numCols, len(tuple))
		}

		// Scan list of tuples in the VALUES row, looking for DEFAULT specifiers.
		var newTuple tree.Exprs
		for itup, val := range tuple {
			if _, ok := val.(tree.DefaultVal); ok {
				// Found DEFAULT, so lazily create new rows and tuple lists.
				if newRows == nil {
					newRows = make([]tree.Exprs, irow, len(values.Rows))
					copy(newRows, values.Rows[:irow])
				}

				if newTuple == nil {
					newTuple = make(tree.Exprs, itup, numCols)
					copy(newTuple, tuple[:itup])
				}

				val = mb.parseDefaultOrComputedExpr(mb.targetColList[itup])
			}
			if newTuple != nil {
				newTuple = append(newTuple, val)
			}
		}

		if newRows != nil {
			if newTuple != nil {
				newRows = append(newRows, newTuple)
			} else {
				newRows = append(newRows, tuple)
			}
		}
	}

	if newRows != nil {
		return &tree.Select{Select: &tree.ValuesClause{Rows: newRows}}
	}
	return inRows
}

// addSynthesizedDefaultCols is a helper method for addSynthesizedColsForInsert
// and addSynthesizedColsForUpdate that scans the list of Ordinary and WriteOnly
// table columns, looking for any that are not computed and do not yet have
// values provided by the input expression. New columns are synthesized for any
// missing columns.
//
// Values are synthesized for columns based on checking these rules, in order:
//   1. If column has a default value specified for it, use that as its value.
//   2. If column is nullable, use NULL as its value.
//   3. If column is currently being added or dropped (i.e. a mutation column),
//      use a default value (0 for INT column, "" for STRING column, etc). Note
//      that the existing "fetched" value returned by the scan cannot be used,
//      since it may not have been initialized yet by the backfiller.
//
// If includeOrdinary is false, then only WriteOnly columns are considered.
//
// NOTE: colIDs is updated with the column IDs of any synthesized columns which
// are added to mb.outScope.
func (mb *mutationBuilder) addSynthesizedDefaultCols(
	colIDs opt.OptionalColList, includeOrdinary bool,
) {
	// We will construct a new Project operator that will contain the newly
	// synthesized column(s).
	pb := makeProjectionBuilder(mb.b, mb.outScope)

	for i, n := 0, mb.tab.ColumnCount(); i < n; i++ {
		tabCol := mb.tab.Column(i)
		if kind := tabCol.Kind(); kind == cat.WriteOnly {
			// Always include WriteOnly columns.
		} else if includeOrdinary && kind == cat.Ordinary {
			// Include Ordinary columns if indicated.
		} else {
			// Wrong kind.
			continue
		}
		if tabCol.IsComputed() {
			continue
		}
		// Skip columns that are already specified.
		if colIDs[i] != 0 {
			continue
		}

		tabColID := mb.tabID.ColumnID(i)
		expr := mb.parseDefaultOrComputedExpr(tabColID)

		// Add synthesized column. It is important to use the real column
		// reference name, as this column may later be referred to by a computed
		// column.
		colName := scopeColName(tabCol.ColName()).WithMetadataName(
			string(tabCol.ColName()) + "_default",
		)
		newCol, _ := pb.Add(colName, expr, tabCol.DatumType())

		// Remember id of newly synthesized column.
		colIDs[i] = newCol

		// Add corresponding target column.
		mb.targetColList = append(mb.targetColList, tabColID)
		mb.targetColSet.Add(tabColID)
	}

	mb.outScope = pb.Finish()
}

// addSynthesizedComputedCols is a helper method for addSynthesizedColsForInsert
// and addSynthesizedColsForUpdate that scans the list of table columns, looking
// for any that are computed and do not yet have values provided by the input
// expression. New columns are synthesized for any missing columns using the
// computed column expression.
//
// NOTE: colIDs is updated with the column IDs of any synthesized columns which
// are added to mb.outScope. If restrict is true, only columns that depend on
// columns that were already in the list (plus all write-only columns) are
// updated.
func (mb *mutationBuilder) addSynthesizedComputedCols(colIDs opt.OptionalColList, restrict bool) {
	// We will construct a new Project operator that will contain the newly
	// synthesized column(s).
	pb := makeProjectionBuilder(mb.b, mb.outScope)
	var updatedColSet opt.ColSet
	if restrict {
		updatedColSet = colIDs.ToSet()
	}

	for i, n := 0, mb.tab.ColumnCount(); i < n; i++ {
		tabCol := mb.tab.Column(i)
		kind := tabCol.Kind()
		if kind != cat.Ordinary && kind != cat.WriteOnly {
			// Wrong kind.
			continue
		}
		if !tabCol.IsComputed() {
			continue
		}

		// Skip columns that are already specified (this is possible for upserts).
		if colIDs[i] != 0 {
			continue
		}

		tabColID := mb.tabID.ColumnID(i)
		expr := mb.parseDefaultOrComputedExpr(tabColID)

		// Add synthesized column.
		colName := scopeColName(tabCol.ColName()).WithMetadataName(
			string(tabCol.ColName()) + "_comp",
		)
		newCol, scalar := pb.Add(colName, expr, tabCol.DatumType())

		if restrict && kind != cat.WriteOnly {
			// Check if any of the columns referred to in the computed column
			// expression are being updated.
			var refCols opt.ColSet
			if scalar == nil {
				// When the expression is a simple column reference, we don't build a
				// new scalar; we just use the same column ID.
				refCols.Add(newCol)
			} else {
				var p props.Shared
				memo.BuildSharedProps(scalar, &p)
				refCols = p.OuterCols
			}
			if !refCols.Intersects(updatedColSet) {
				// Normalization rules will clean up the unnecessary projection.
				continue
			}
		}

		// Remember id of newly synthesized column.
		colIDs[i] = newCol

		// Add corresponding target column.
		mb.targetColList = append(mb.targetColList, tabColID)
		mb.targetColSet.Add(tabColID)
	}

	mb.outScope = pb.Finish()
}

// roundDecimalValues wraps each DECIMAL-related column (including arrays of
// decimals) with a call to the crdb_internal.round_decimal_values function, if
// column values may need to be rounded. This is necessary when mutating table
// columns that have a limited scale (e.g. DECIMAL(10, 1)). Here is the PG docs
// description:
//
//   http://www.postgresql.org/docs/9.5/static/datatype-numeric.html
//   "If the scale of a value to be stored is greater than
//   the declared scale of the column, the system will round the
//   value to the specified number of fractional digits. Then,
//   if the number of digits to the left of the decimal point
//   exceeds the declared precision minus the declared scale, an
//   error is raised."
//
// Note that this function only handles the rounding portion of that. The
// precision check is done by the execution engine. The rounding cannot be done
// there, since it needs to happen before check constraints are computed, and
// before UPSERT joins.
//
// If roundComputedCols is false, then don't wrap computed columns. If true,
// then only wrap computed columns. This is necessary because computed columns
// can depend on other columns mutated by the operation; it is necessary to
// first round those values, then evaluated the computed expression, and then
// round the result of the computation.
//
// roundDecimalValues will only round decimal columns that are part of the
// colIDs list (i.e. are not 0). If a column is rounded, then the list will be
// updated with the column ID of the new synthesized column.
func (mb *mutationBuilder) roundDecimalValues(colIDs opt.OptionalColList, roundComputedCols bool) {
	var projectionsScope *scope

	for i, id := range colIDs {
		if id == 0 {
			// Column not mutated, so nothing to do.
			continue
		}

		// Include or exclude computed columns, depending on the value of
		// roundComputedCols.
		col := mb.tab.Column(i)
		if col.IsComputed() != roundComputedCols {
			continue
		}

		// Check whether the target column's type may require rounding of the
		// input value.
		colType := col.DatumType()
		precision, width := colType.Precision(), colType.Width()
		if colType.Family() == types.ArrayFamily {
			innerType := colType.ArrayContents()
			if innerType.Family() == types.ArrayFamily {
				panic(errors.AssertionFailedf("column type should never be a nested array"))
			}
			precision, width = innerType.Precision(), innerType.Width()
		}

		props, overload := findRoundingFunction(colType, precision)
		if props == nil {
			continue
		}

		// If column has already been rounded, then skip it.
		if mb.roundedDecimalCols.Contains(id) {
			continue
		}

		private := &memo.FunctionPrivate{
			Name:       "crdb_internal.round_decimal_values",
			Typ:        col.DatumType(),
			Properties: props,
			Overload:   overload,
		}
		variable := mb.b.factory.ConstructVariable(id)
		scale := mb.b.factory.ConstructConstVal(tree.NewDInt(tree.DInt(width)), types.Int)
		fn := mb.b.factory.ConstructFunction(memo.ScalarListExpr{variable, scale}, private)

		// Lazily create new scope and update the scope column to be rounded.
		if projectionsScope == nil {
			projectionsScope = mb.outScope.replace()
			projectionsScope.appendColumnsFromScope(mb.outScope)
		}
		scopeCol := projectionsScope.getColumn(id)
		mb.b.populateSynthesizedColumn(scopeCol, fn)

		// Overwrite the input column ID with the new synthesized column ID.
		colIDs[i] = scopeCol.id
		mb.roundedDecimalCols.Add(scopeCol.id)

		// When building an UPDATE..FROM expression the projectionScope may have
		// two columns with different names but the same ID. As a result, the
		// scope column with the correct name (the name of the target column)
		// may not be returned from projectionScope.getColumn. We set the name
		// of the new scope column to the target column name to ensure it is
		// in-scope when building CHECK constraint and partial index PUT
		// expressions. See #61520.
		// TODO(mgartner): Find a less brittle way to manage the scopes of
		// mutations so that this isn't necessary. Ideally the scope produced by
		// addUpdateColumns would not include columns in the FROM clause. Those
		// columns are only in-scope in the RETURNING clause via
		// mb.extraAccessibleCols.
		scopeCol.name = scopeColName(mb.tab.Column(i).ColName())
	}

	if projectionsScope != nil {
		mb.b.constructProjectForScope(mb.outScope, projectionsScope)
		mb.outScope = projectionsScope
	}
}

// findRoundingFunction returns the builtin function overload needed to round
// input values. This is only necessary for DECIMAL or DECIMAL[] types that have
// limited precision, such as:
//
//   DECIMAL(15, 1)
//   DECIMAL(10, 3)[]
//
// If an input decimal value has more than the required number of fractional
// digits, it must be rounded before being inserted into these types.
//
// NOTE: CRDB does not allow nested array storage types, so only one level of
// array nesting needs to be checked.
func findRoundingFunction(
	typ *types.T, precision int32,
) (*tree.FunctionProperties, *tree.Overload) {
	if precision == 0 {
		// Unlimited precision decimal target type never needs rounding.
		return nil, nil
	}

	props, overloads := builtins.GetBuiltinProperties("crdb_internal.round_decimal_values")

	if typ.Equivalent(types.Decimal) {
		return props, &overloads[0]
	}
	if typ.Equivalent(types.DecimalArray) {
		return props, &overloads[1]
	}

	// Not DECIMAL or DECIMAL[].
	return nil, nil
}

// addCheckConstraintCols synthesizes a boolean output column for each check
// constraint defined on the target table. The mutation operator will report a
// constraint violation error if the value of the column is false.
//
// Synthesized check columns are not necessary for UPDATE mutations if the
// columns referenced in the check expression are not being mutated. If isUpdate
// is true, check columns that do not reference mutation columns are not added
// to checkColIDs, which allows pruning normalization rules to remove the
// unnecessary projected column.
func (mb *mutationBuilder) addCheckConstraintCols(isUpdate bool) {
	if mb.tab.CheckCount() != 0 {
		projectionsScope := mb.outScope.replace()
		projectionsScope.appendColumnsFromScope(mb.outScope)
		mutationCols := mb.mutationColumnIDs()

		for i, n := 0, mb.tab.CheckCount(); i < n; i++ {
			expr, err := parser.ParseExpr(mb.tab.Check(i).Constraint)
			if err != nil {
				panic(err)
			}

			texpr := mb.outScope.resolveAndRequireType(expr, types.Bool)

			// Use an anonymous name because the column cannot be referenced
			// in other expressions.
			colName := scopeColName("").WithMetadataName(fmt.Sprintf("check%d", i+1))
			scopeCol := projectionsScope.addColumn(colName, texpr)

			// TODO(ridwanmsharif): Maybe we can avoid building constraints here
			// and instead use the constraints stored in the table metadata.
			referencedCols := &opt.ColSet{}
			mb.b.buildScalar(texpr, mb.outScope, projectionsScope, scopeCol, referencedCols)

			// If the mutation is not an UPDATE, track the synthesized check
			// columns in checkColIDS. If the mutation is an UPDATE, only track
			// the check columns if the columns referenced in the check
			// expression are being mutated.
			if !isUpdate || referencedCols.Intersects(mutationCols) {
				mb.checkColIDs[i] = scopeCol.id
			}
		}

		mb.b.constructProjectForScope(mb.outScope, projectionsScope)
		mb.outScope = projectionsScope
	}
}

// mutationColumnIDs returns the set of all column IDs that will be mutated.
func (mb *mutationBuilder) mutationColumnIDs() opt.ColSet {
	cols := opt.ColSet{}
	for _, col := range mb.insertColIDs {
		cols.Add(col)
	}
	for _, col := range mb.updateColIDs {
		cols.Add(col)
	}
	for _, col := range mb.upsertColIDs {
		cols.Add(col)
	}
	return cols
}

// projectPartialIndexPutCols builds a Project that synthesizes boolean PUT
// columns for each partial index defined on the target table. See
// partialIndexPutColIDs for more info on these columns.
func (mb *mutationBuilder) projectPartialIndexPutCols() {
	mb.projectPartialIndexColsImpl(mb.outScope, nil /* delScope */)
}

// projectPartialIndexDelCols builds a Project that synthesizes boolean PUT
// columns for each partial index defined on the target table. See
// partialIndexDelColIDs for more info on these columns.
func (mb *mutationBuilder) projectPartialIndexDelCols() {
	mb.projectPartialIndexColsImpl(nil /* putScope */, mb.fetchScope)
}

// projectPartialIndexPutAndDelCols builds a Project that synthesizes boolean
// PUT and DEL columns for each partial index defined on the target table. See
// partialIndexPutColIDs and partialIndexDelColIDs for more info on these
// columns.
func (mb *mutationBuilder) projectPartialIndexPutAndDelCols() {
	mb.projectPartialIndexColsImpl(mb.outScope, mb.fetchScope)
}

// projectPartialIndexColsImpl builds a Project that synthesizes boolean PUT and
// DEL columns  for each partial index defined on the target table. PUT columns
// are only projected if putScope is non-nil and DEL columns are only projected
// if delScope is non-nil.
//
// NOTE: This function should only be called via projectPartialIndexPutCols,
// projectPartialIndexDelCols, or projectPartialIndexPutAndDelCols.
func (mb *mutationBuilder) projectPartialIndexColsImpl(putScope, delScope *scope) {
	if partialIndexCount(mb.tab) > 0 {
		projectionScope := mb.outScope.replace()
		projectionScope.appendColumnsFromScope(mb.outScope)

		ord := 0
		for i, n := 0, mb.tab.DeletableIndexCount(); i < n; i++ {
			index := mb.tab.Index(i)

			// Skip non-partial indexes.
			if _, isPartial := index.Predicate(); !isPartial {
				continue
			}

			expr := mb.parsePartialIndexPredicateExpr(i)

			// Build synthesized PUT columns.
			if putScope != nil {
				texpr := putScope.resolveAndRequireType(expr, types.Bool)

				// Use an anonymous name because the column cannot be referenced
				// in other expressions.
				colName := scopeColName("").WithMetadataName(fmt.Sprintf("partial_index_put%d", ord+1))
				scopeCol := projectionScope.addColumn(colName, texpr)

				mb.b.buildScalar(texpr, putScope, projectionScope, scopeCol, nil)
				mb.partialIndexPutColIDs[ord] = scopeCol.id
			}

			// Build synthesized DEL columns.
			if delScope != nil {
				texpr := delScope.resolveAndRequireType(expr, types.Bool)

				// Use an anonymous name because the column cannot be referenced
				// in other expressions.
				colName := scopeColName("").WithMetadataName(fmt.Sprintf("partial_index_del%d", ord+1))
				scopeCol := projectionScope.addColumn(colName, texpr)

				mb.b.buildScalar(texpr, delScope, projectionScope, scopeCol, nil)
				mb.partialIndexDelColIDs[ord] = scopeCol.id
			}

			ord++
		}

		mb.b.constructProjectForScope(mb.outScope, projectionScope)
		mb.outScope = projectionScope
	}
}

// disambiguateColumns ranges over the scope and ensures that at most one column
// has each table column name, and that name refers to the column with the final
// value that the mutation applies.
func (mb *mutationBuilder) disambiguateColumns() {
	// Determine the set of input columns that will have their names preserved.
	var preserve opt.ColSet
	for i, n := 0, mb.tab.ColumnCount(); i < n; i++ {
		if colID := mb.mapToReturnColID(i); colID != 0 {
			preserve.Add(colID)
		}
	}

	// Clear names of all non-preserved columns.
	for i := range mb.outScope.cols {
		if !preserve.Contains(mb.outScope.cols[i].id) {
			mb.outScope.cols[i].clearName()
		}
	}
}

// makeMutationPrivate builds a MutationPrivate struct containing the table and
// column metadata needed for the mutation operator.
func (mb *mutationBuilder) makeMutationPrivate(needResults bool) *memo.MutationPrivate {
	// Helper function that returns nil if there are no non-zero column IDs in a
	// given list. A zero column ID indicates that column does not participate
	// in this mutation operation.
	checkEmptyList := func(colIDs opt.OptionalColList) opt.OptionalColList {
		if colIDs.IsEmpty() {
			return nil
		}
		return colIDs
	}

	private := &memo.MutationPrivate{
		Table:               mb.tabID,
		InsertCols:          checkEmptyList(mb.insertColIDs),
		FetchCols:           checkEmptyList(mb.fetchColIDs),
		UpdateCols:          checkEmptyList(mb.updateColIDs),
		CanaryCol:           mb.canaryColID,
		ArbiterIndexes:      mb.arbiters.IndexOrdinals(),
		ArbiterConstraints:  mb.arbiters.UniqueConstraintOrdinals(),
		CheckCols:           checkEmptyList(mb.checkColIDs),
		PartialIndexPutCols: checkEmptyList(mb.partialIndexPutColIDs),
		PartialIndexDelCols: checkEmptyList(mb.partialIndexDelColIDs),
		FKCascades:          mb.cascades,
	}

	// If we didn't actually plan any checks or cascades, don't buffer the input.
	if len(mb.uniqueChecks) > 0 || len(mb.fkChecks) > 0 || len(mb.cascades) > 0 {
		private.WithID = mb.withID
	}

	if needResults {
		private.ReturnCols = make(opt.OptionalColList, mb.tab.ColumnCount())
		for i, n := 0, mb.tab.ColumnCount(); i < n; i++ {
			if kind := mb.tab.Column(i).Kind(); kind != cat.Ordinary {
				// Only non-mutation and non-system columns are output columns.
				continue
			}
			retColID := mb.mapToReturnColID(i)
			if retColID == 0 {
				panic(errors.AssertionFailedf("column %d is not available in the mutation input", i))
			}
			private.ReturnCols[i] = retColID
		}
	}

	return private
}

// mapToReturnColID returns the ID of the input column that provides the final
// value for the column at the given ordinal position in the table. This value
// might mutate the column, or it might be returned by the mutation statement,
// or it might not be used at all. Columns take priority in this order:
//
//   upsert, update, fetch, insert
//
// If an upsert column is available, then it already combines an update/fetch
// value with an insert value, so it takes priority. If an update column is
// available, then it overrides any fetch value. Finally, the relative priority
// of fetch and insert columns doesn't matter, since they're only used together
// in the upsert case where an upsert column would be available.
//
// If the column is never referenced by the statement, then mapToReturnColID
// returns 0. This would be the case for delete-only columns in an Insert
// statement, because they're neither fetched nor mutated.
func (mb *mutationBuilder) mapToReturnColID(tabOrd int) opt.ColumnID {
	switch {
	case mb.upsertColIDs[tabOrd] != 0:
		return mb.upsertColIDs[tabOrd]

	case mb.updateColIDs[tabOrd] != 0:
		return mb.updateColIDs[tabOrd]

	case mb.fetchColIDs[tabOrd] != 0:
		return mb.fetchColIDs[tabOrd]

	case mb.insertColIDs[tabOrd] != 0:
		return mb.insertColIDs[tabOrd]

	default:
		// Column is never referenced by the statement.
		return 0
	}
}

// buildReturning wraps the input expression with a Project operator that
// projects the given RETURNING expressions.
func (mb *mutationBuilder) buildReturning(returning tree.ReturningExprs) {
	// Handle case of no RETURNING clause.
	if returning == nil {
		expr := mb.outScope.expr
		mb.outScope = mb.b.allocScope()
		mb.outScope.expr = expr
		return
	}

	// Start out by constructing a scope containing one column for each non-
	// mutation column in the target table, in the same order, and with the
	// same names. These columns can be referenced by the RETURNING clause.
	//
	//   1. Project only non-mutation columns.
	//   2. Alias columns to use table column names.
	//   3. Mark hidden columns.
	//   4. Project columns in same order as defined in table schema.
	//
	inScope := mb.outScope.replace()
	inScope.expr = mb.outScope.expr
	inScope.appendOrdinaryColumnsFromTable(mb.md.TableMeta(mb.tabID), &mb.alias)

	// extraAccessibleCols contains all the columns that the RETURNING
	// clause can refer to in addition to the table columns. This is useful for
	// UPDATE ... FROM statements, where all columns from tables in the FROM clause
	// are in scope for the RETURNING clause.
	inScope.appendColumns(mb.extraAccessibleCols)

	// Construct the Project operator that projects the RETURNING expressions.
	outScope := inScope.replace()
	mb.b.analyzeReturningList(returning, nil /* desiredTypes */, inScope, outScope)
	mb.b.buildProjectionList(inScope, outScope)
	mb.b.constructProjectForScope(inScope, outScope)
	mb.outScope = outScope
}

// checkNumCols raises an error if the expected number of columns does not match
// the actual number of columns.
func (mb *mutationBuilder) checkNumCols(expected, actual int) {
	if actual != expected {
		more, less := "expressions", "target columns"
		if actual < expected {
			more, less = less, more
		}

		panic(pgerror.Newf(pgcode.Syntax,
			"%s has more %s than %s, %d expressions for %d targets",
			strings.ToUpper(mb.opName), more, less, actual, expected))
	}
}

// parseDefaultOrComputedExpr parses the default (including nullable) or
// computed value expression for the given table column, and caches it for
// reuse.
func (mb *mutationBuilder) parseDefaultOrComputedExpr(colID opt.ColumnID) tree.Expr {
	if mb.parsedColExprs == nil {
		mb.parsedColExprs = make([]tree.Expr, mb.tab.ColumnCount())
	}

	// Return expression from cache, if it was already parsed previously.
	ord := mb.tabID.ColumnOrdinal(colID)
	if mb.parsedColExprs[ord] != nil {
		return mb.parsedColExprs[ord]
	}

	var exprStr string
	tabCol := mb.tab.Column(ord)
	switch {
	case tabCol.IsComputed():
		exprStr = tabCol.ComputedExprStr()
	case tabCol.HasDefault():
		exprStr = tabCol.DefaultExprStr()
	case tabCol.IsMutation() && !tabCol.IsNullable():
		// Synthesize default value for NOT NULL mutation column so that it can be
		// set when in the write-only state. This is only used when no other value
		// is possible (no default value available, NULL not allowed).
		datum, err := tree.NewDefaultDatum(mb.b.evalCtx, tabCol.DatumType())
		if err != nil {
			panic(err)
		}
		return datum
	default:
		return tree.DNull
	}

	expr, err := parser.ParseExpr(exprStr)
	if err != nil {
		panic(err)
	}

	mb.parsedColExprs[ord] = expr
	return expr
}

// parsePartialIndexPredicateExpr parses the partial index predicate for the
// given index and caches it for reuse. This function panics if the index at the
// given ordinal is not a partial index.
func (mb *mutationBuilder) parsePartialIndexPredicateExpr(idx cat.IndexOrdinal) tree.Expr {
	index := mb.tab.Index(idx)

	predStr, isPartial := index.Predicate()
	if !isPartial {
		panic(errors.AssertionFailedf("index at ordinal %d is not a partial index", idx))
	}

	if mb.parsedIndexExprs == nil {
		mb.parsedIndexExprs = make([]tree.Expr, mb.tab.DeletableIndexCount())
	}

	// Return expression from the cache, if it was already parsed previously.
	if mb.parsedIndexExprs[idx] != nil {
		return mb.parsedIndexExprs[idx]
	}

	expr, err := parser.ParseExpr(predStr)
	if err != nil {
		panic(err)
	}

	mb.parsedIndexExprs[idx] = expr
	return expr
}

// parseUniqueConstraintPredicateExpr parses the predicate of the given partial
// unique constraint and caches it for reuse. This function panics if the unique
// constraint at the given ordinal is not partial.
func (mb *mutationBuilder) parseUniqueConstraintPredicateExpr(uniq cat.UniqueOrdinal) tree.Expr {
	uniqueConstraint := mb.tab.Unique(uniq)

	predStr, isPartial := uniqueConstraint.Predicate()
	if !isPartial {
		panic(errors.AssertionFailedf("unique constraint at ordinal %d is not a partial unique constraint", uniq))
	}

	if mb.parsedUniqueConstraintExprs == nil {
		mb.parsedUniqueConstraintExprs = make([]tree.Expr, mb.tab.UniqueCount())
	}

	// Return expression from the cache, if it was already parsed previously.
	if mb.parsedUniqueConstraintExprs[uniq] != nil {
		return mb.parsedUniqueConstraintExprs[uniq]
	}

	expr, err := parser.ParseExpr(predStr)
	if err != nil {
		panic(err)
	}

	mb.parsedUniqueConstraintExprs[uniq] = expr
	return expr
}

// getIndexLaxKeyOrdinals returns the ordinals of all lax key columns in the
// given index. A column's ordinal is the ordered position of that column in the
// owning table.
func getIndexLaxKeyOrdinals(index cat.Index) util.FastIntSet {
	var keyOrds util.FastIntSet
	for i, n := 0, index.LaxKeyColumnCount(); i < n; i++ {
		keyOrds.Add(index.Column(i).Ordinal())
	}
	return keyOrds
}

// getUniqueConstraintOrdinals returns the ordinals of all columns in the given
// unique constraint. A column's ordinal is the ordered position of that column
// in the owning table.
func getUniqueConstraintOrdinals(tab cat.Table, uc cat.UniqueConstraint) util.FastIntSet {
	var ucOrds util.FastIntSet
	for i, n := 0, uc.ColumnCount(); i < n; i++ {
		ucOrds.Add(uc.ColumnOrdinal(tab, i))
	}
	return ucOrds
}

// getExplicitPrimaryKeyOrdinals returns the ordinals of the primary key
// columns, excluding any implicit partitioning columns in the primary index.
func getExplicitPrimaryKeyOrdinals(tab cat.Table) util.FastIntSet {
	index := tab.Index(cat.PrimaryIndex)
	skipCols := index.ImplicitPartitioningColumnCount()
	var keyOrds util.FastIntSet
	for i, n := skipCols, index.LaxKeyColumnCount(); i < n; i++ {
		keyOrds.Add(index.Column(i).Ordinal())
	}
	return keyOrds
}

// findNotNullIndexCol finds the first not-null column in the given index and
// returns its ordinal position in the owner table. There must always be such a
// column, even if it turns out to be an implicit primary key column.
func findNotNullIndexCol(index cat.Index) int {
	for i, n := 0, index.KeyColumnCount(); i < n; i++ {
		indexCol := index.Column(i)
		if !indexCol.IsNullable() {
			return indexCol.Ordinal()
		}
	}
	panic(errors.AssertionFailedf("should have found not null column in index"))
}

// resultsNeeded determines whether a statement that might have a RETURNING
// clause needs to provide values for result rows for a downstream plan.
func resultsNeeded(r tree.ReturningClause) bool {
	switch t := r.(type) {
	case *tree.ReturningExprs:
		return true
	case *tree.ReturningNothing, *tree.NoReturningClause:
		return false
	default:
		panic(errors.AssertionFailedf("unexpected ReturningClause type: %T", t))
	}
}

// checkDatumTypeFitsColumnType verifies that a given scalar value type is valid
// to be stored in a column of the given column type.
//
// For the purpose of this analysis, column type aliases are not considered to
// be different (eg. TEXT and VARCHAR will fit the same scalar type String).
//
// This is used by the UPDATE, INSERT and UPSERT code.
func checkDatumTypeFitsColumnType(col *cat.Column, typ *types.T) {
	if typ.Equivalent(col.DatumType()) {
		return
	}

	colName := string(col.ColName())
	err := pgerror.Newf(pgcode.DatatypeMismatch,
		"value type %s doesn't match type %s of column %q",
		typ, col.DatumType(), tree.ErrNameString(colName))
	err = errors.WithHint(err, "you will need to rewrite or cast the expression")
	panic(err)
}

// partialIndexCount returns the number of public, write-only, and delete-only
// partial indexes defined on the table.
func partialIndexCount(tab cat.Table) int {
	count := 0
	for i, n := 0, tab.DeletableIndexCount(); i < n; i++ {
		if _, ok := tab.Index(i).Predicate(); ok {
			count++
		}
	}
	return count
}

type checkInputScanType uint8

const (
	checkInputScanNewVals checkInputScanType = iota
	checkInputScanFetchedVals
)

// buildCheckInputScan constructs a WithScan that iterates over the input to the
// mutation operator. Used in expressions that generate rows for checking for FK
// and uniqueness violations.
//
// The WithScan expression will scan either the new values or the fetched values
// for the given table ordinals (which correspond to FK or unique columns).
//
// Returns a scope containing the WithScan expression and the output columns
// from the WithScan. The output columns map 1-to-1 to tabOrdinals. Also returns
// the subset of these columns that can be assumed to be not null (either
// because they are not null in the mutation input or because they are
// non-nullable table columns).
//
func (mb *mutationBuilder) buildCheckInputScan(
	typ checkInputScanType, tabOrdinals []int,
) (withScanScope *scope, notNullOutCols opt.ColSet) {
	// inputCols are the column IDs from the mutation input that we are scanning.
	inputCols := make(opt.ColList, len(tabOrdinals))

	withScanScope = mb.b.allocScope()
	withScanScope.cols = make([]scopeColumn, len(inputCols))

	for i, tabOrd := range tabOrdinals {
		if typ == checkInputScanNewVals {
			inputCols[i] = mb.mapToReturnColID(tabOrd)
		} else {
			inputCols[i] = mb.fetchColIDs[tabOrd]
		}
		if inputCols[i] == 0 {
			panic(errors.AssertionFailedf("no value for check input column (tabOrd=%d)", tabOrd))
		}

		// Synthesize a new output column for the input column, using the name
		// of the column in the underlying table. The table's column names are
		// used because partial unique constraint checks must filter the
		// WithScan rows with a predicate expression that references the table's
		// columns.
		tableCol := mb.b.factory.Metadata().Table(mb.tabID).Column(tabOrd)
		outCol := mb.md.AddColumn(string(tableCol.ColName()), tableCol.DatumType())
		withScanScope.cols[i] = scopeColumn{
			id:   outCol,
			name: scopeColName(tableCol.ColName()),
			typ:  tableCol.DatumType(),
		}

		// If a table column is not nullable, NULLs cannot be inserted (the
		// mutation will fail). So for the purposes of checks, we can treat
		// these columns as not null.
		if mb.outScope.expr.Relational().NotNullCols.Contains(inputCols[i]) ||
			!mb.tab.Column(tabOrd).IsNullable() {
			notNullOutCols.Add(outCol)
		}
	}

	withScanScope.expr = mb.b.factory.ConstructWithScan(&memo.WithScanPrivate{
		With:    mb.withID,
		InCols:  inputCols,
		OutCols: withScanScope.colList(),
		ID:      mb.b.factory.Metadata().NextUniqueID(),
	})
	return withScanScope, notNullOutCols
}
