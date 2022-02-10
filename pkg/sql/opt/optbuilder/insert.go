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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// duplicateUpsertErrText is error text used when a row is modified twice by
// an upsert statement.
const duplicateUpsertErrText = "UPSERT or INSERT...ON CONFLICT command cannot affect row a second time"

// excludedTableName is the name of a special Upsert data source. When a row
// cannot be inserted due to a conflict, the "excluded" data source contains
// that row, so that its columns can be referenced in the conflict clause:
//
//   INSERT INTO ab VALUES (1, 2) ON CONFLICT (a) DO UPDATE b=excluded.b+1
//
// It is located in the special crdb_internal schema so that it never overlaps
// with user data sources.
var excludedTableName tree.TableName

func init() {
	// Clear explicit schema and catalog so that they're not printed in error
	// messages.
	excludedTableName = tree.MakeTableNameWithSchema("", "crdb_internal", "excluded")
	excludedTableName.ExplicitSchema = false
	excludedTableName.ExplicitCatalog = false
}

// buildInsert builds a memo group for an InsertOp or UpsertOp expression. To
// begin, an input expression is constructed which outputs these columns to
// insert into the target table:
//
//   1. Columns explicitly specified by the user in SELECT or VALUES expression.
//
//   2. Columns not specified by the user, but having a default value declared
//      in schema (or being nullable).
//
//   3. Computed columns.
//
//   4. Mutation columns which are being added or dropped by an online schema
//      change.
//
// buildInsert starts by constructing the input expression, and then wraps it
// with Project operators which add default, computed, and mutation columns. The
// final input expression will project values for all columns in the target
// table. For example, if this is the schema and INSERT statement:
//
//   CREATE TABLE abcd (
//     a INT PRIMARY KEY,
//     b INT,
//     c INT DEFAULT(10),
//     d INT AS (b+c) STORED
//   )
//   INSERT INTO abcd (a) VALUES (1)
//
// Then an input expression equivalent to this would be built:
//
//   SELECT ins_a, ins_b, ins_c, ins_b + ins_c AS ins_d
//   FROM (VALUES (1, NULL, 10)) AS t(ins_a, ins_b, ins_c)
//
// If an ON CONFLICT clause is present (or if it was an UPSERT statement), then
// additional columns are added to the input expression:
//
//   1. Columns containing existing values fetched from the target table and
//      used to detect conflicts and to formulate the key/value update commands.
//
//   2. Columns containing updated values to set when a conflict is detected, as
//      specified by the user.
//
//   3. Computed columns which will be updated when a conflict is detected and
//      that are dependent on one or more updated columns.
//
// A LEFT OUTER JOIN associates each row to insert with the corresponding
// existing row (#1 above). If the row does not exist, then the existing columns
// will be null-extended, per the semantics of LEFT OUTER JOIN. This behavior
// allows the execution engine to test whether a given insert row conflicts with
// an existing row in the table. One of the existing columns that is declared as
// NOT NULL in the table schema is designated as a "canary column". When the
// canary column is null after the join step, then it must have been null-
// extended by the LEFT OUTER JOIN. Therefore, there is no existing row, and no
// conflict. If the canary column is not null, then there is an existing row,
// and a conflict.
//
// The canary column is used in CASE statements to toggle between the insert and
// update values for each row. If there is no conflict, the insert value is
// used. Otherwise, the update value is used (or the existing value if there is
// no update value for that column).
//
// In addition, upsert cases have another complication that arises from the
// requirement that no mutation statement updates the same row more than once.
// Primary key violations prevent INSERT statements from inserting the same row
// twice. DELETE statements do not encounter a problem because their input never
// contains duplicate rows. And UPDATE statements are equivalent to DELETE
// followed by an INSERT, so they're safe as well. By contrast, UPSERT and
// INSERT..ON CONFLICT statements can have duplicate input rows that trigger
// updates of the same row after insertion conflicts.
//
// Detecting (and raising an error) or ignoring (in case of DO NOTHING)
// duplicate rows requires wrapping the input with one or more DISTINCT ON
// operators that ensure the input is distinct on at least one unique index.
// Because the input is distinct on a unique index of the target table, the
// statement will never attempt to update the same row twice.
//
// Putting it all together, if this is the schema and INSERT..ON CONFLICT
// statement:
//
//   CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT)
//   INSERT INTO abc VALUES (1, 2), (1, 3) ON CONFLICT (a) DO UPDATE SET b=10
//
// Then an input expression roughly equivalent to this would be built (note that
// the DISTINCT ON is really the UpsertDistinctOn operator, which behaves a bit
// differently than the DistinctOn operator):
//
//   SELECT
//     fetch_a,
//     fetch_b,
//     fetch_c,
//     CASE WHEN fetch_a IS NULL ins_a ELSE fetch_a END AS ups_a,
//     CASE WHEN fetch_a IS NULL ins_b ELSE 10 END AS ups_b,
//     CASE WHEN fetch_a IS NULL ins_c ELSE fetch_c END AS ups_c,
//   FROM (
//     SELECT DISTINCT ON (ins_a) *
//     FROM (VALUES (1, 2, NULL), (1, 3, NULL)) AS ins(ins_a, ins_b, ins_c)
//   )
//   LEFT OUTER JOIN abc AS fetch(fetch_a, fetch_b, fetch_c)
//   ON ins_a = fetch_a
//
// Here, the fetch_a column has been designated as the canary column, since it
// is NOT NULL in the schema. It is used as the CASE condition to decide between
// the insert and update values for each row. The CASE expressions will often
// prevent the unnecessary evaluation of the update expression in the case where
// an insertion needs to occur. In addition, it simplifies logical property
// calculation, since a 1:1 mapping to each target table column from a
// corresponding input column is maintained.
//
// If the ON CONFLICT clause contains a DO NOTHING clause, then each UNIQUE
// index on the target table requires its own DISTINCT ON to ensure that the
// input has no duplicates, and an ANTI JOIN to check whether a conflict exists.
// For example:
//
//   CREATE TABLE ab (a INT PRIMARY KEY, b INT)
//   INSERT INTO ab (a, b) VALUES (1, 2), (1, 3) ON CONFLICT DO NOTHING
//
// Then an input expression roughly equivalent to this would be built:
//
//   SELECT x, y
//   FROM (SELECT DISTINCT ON (x) * FROM (VALUES (1, 2), (1, 3))) AS input(x, y)
//   WHERE NOT EXISTS(
//     SELECT ab.a WHERE input.x = ab.a
//   )
//
// Note that an ordered input to the INSERT does not provide any guarantee about
// the order in which mutations are applied, or the order of any returned rows
// (i.e. it won't become a physical property required of the Insert or Upsert
// operator). Not propagating input orderings avoids an extra sort when the
// ON CONFLICT clause is present, since it joins a new set of rows to the input
// and thereby scrambles the input ordering.
func (b *Builder) buildInsert(ins *tree.Insert, inScope *scope) (outScope *scope) {
	// Find which table we're working on, check the permissions.
	tab, depName, alias, refColumns := b.resolveTableForMutation(ins.Table, privilege.INSERT)

	// It is possible to insert into specific columns using table reference
	// syntax:
	// INSERT INTO [<table_id>(<col1_id>,<col2_id>) AS <alias>] ...
	// is equivalent to
	// INSERT INTO [<table_id> AS <alias>] (col1_name, col2_name) ...
	if refColumns != nil {
		if len(ins.Columns) != 0 {
			panic(pgerror.Newf(pgcode.Syntax,
				"cannot specify both a list of column IDs and a list of column names"))
		}

		ins.Columns = make(tree.NameList, len(refColumns))
		for i, ord := range resolveNumericColumnRefs(tab, refColumns) {
			ins.Columns[i] = tab.Column(ord).ColName()
		}
	}

	if ins.OnConflict != nil {
		// UPSERT and INDEX ON CONFLICT will read from the table to check for
		// duplicates.
		b.checkPrivilege(depName, tab, privilege.SELECT)

		if !ins.OnConflict.DoNothing {
			// UPSERT and INDEX ON CONFLICT DO UPDATE may modify rows if the
			// DO NOTHING clause is not present.
			b.checkPrivilege(depName, tab, privilege.UPDATE)
		}
	}

	// Check if this table has already been mutated in another subquery.
	b.checkMultipleMutations(tab, ins.OnConflict == nil /* simpleInsert */)

	var mb mutationBuilder
	if ins.OnConflict != nil && ins.OnConflict.IsUpsertAlias() {
		mb.init(b, "upsert", tab, alias)
	} else {
		mb.init(b, "insert", tab, alias)
	}

	// Compute target columns in two cases:
	//
	//   1. When explicitly specified by name:
	//
	//        INSERT INTO <table> (<col1>, <col2>, ...) ...
	//
	//   2. When implicitly targeted by VALUES expression:
	//
	//        INSERT INTO <table> VALUES (...)
	//
	// Target columns for other cases can't be derived until the input expression
	// is built, at which time the number of input columns is known. At the same
	// time, the input expression cannot be built until DEFAULT expressions are
	// replaced and named target columns are known. So this step must come first.
	if len(ins.Columns) != 0 {
		// Target columns are explicitly specified by name.
		mb.addTargetNamedColsForInsert(ins.Columns)
	} else {
		values := mb.extractValuesInput(ins.Rows)
		if values != nil {
			// Target columns are implicitly targeted by VALUES expression in the
			// same order they appear in the target table schema.
			mb.addTargetTableColsForInsert(len(values.Rows[0]))
		}
	}

	// Build the input rows expression if one was specified:
	//
	//   INSERT INTO <table> VALUES ...
	//   INSERT INTO <table> SELECT ... FROM ...
	//
	// or initialize an empty input if inserting default values (default values
	// will be added later):
	//
	//   INSERT INTO <table> DEFAULT VALUES
	//
	if !ins.DefaultValues() {
		// Replace any DEFAULT expressions in the VALUES clause, if a VALUES clause
		// exists:
		//
		//   INSERT INTO <table> VALUES (..., DEFAULT, ...)
		//
		rows := mb.replaceDefaultExprs(ins.Rows)

		mb.buildInputForInsert(inScope, rows)
	} else {
		mb.buildInputForInsert(inScope, nil /* rows */)
	}

	// Add default columns that were not explicitly specified by name or
	// implicitly targeted by input columns. Also add any computed columns. In
	// both cases, include columns undergoing mutations in the write-only state.
	mb.addSynthesizedColsForInsert()

	var returning tree.ReturningExprs
	if resultsNeeded(ins.Returning) {
		returning = *ins.Returning.(*tree.ReturningExprs)
	}

	switch {
	// Case 1: Simple INSERT statement.
	case ins.OnConflict == nil:
		// Build the final insert statement, including any returned expressions.
		mb.buildInsert(returning)

	// Case 2: INSERT..ON CONFLICT DO NOTHING.
	case ins.OnConflict.DoNothing:
		// Wrap the input in one ANTI JOIN per UNIQUE index, and filter out rows
		// that have conflicts. See the buildInputForDoNothing comment for more
		// details.
		mb.buildInputForDoNothing(inScope, ins.OnConflict)

		// Since buildInputForDoNothing filters out rows with conflicts, always
		// insert rows that are not filtered.
		mb.buildInsert(returning)

	// Case 3: UPSERT statement.
	case ins.OnConflict.IsUpsertAlias():
		// Add columns which will be updated by the Upsert when a conflict occurs.
		// These are derived from the insert columns.
		mb.setUpsertCols(ins.Columns)

		// Check whether the existing rows need to be fetched in order to detect
		// conflicts.
		if mb.needExistingRows() {
			// Left-join each input row to the target table, using conflict columns
			// derived from the primary index as the join condition.
			mb.buildInputForUpsert(inScope, nil /* onConflict */, nil /* whereClause */)

			// Add additional columns for computed expressions that may depend on any
			// updated columns, as well as mutation columns with default values.
			mb.addSynthesizedColsForUpdate()
		}

		// Build the final upsert statement, including any returned expressions.
		mb.buildUpsert(returning)

	// Case 4: INSERT..ON CONFLICT..DO UPDATE statement.
	default:
		// Left-join each input row to the target table, using the conflict columns
		// as the join condition.
		mb.buildInputForUpsert(inScope, ins.OnConflict, ins.OnConflict.Where)

		// Derive the columns that will be updated from the SET expressions.
		mb.addTargetColsForUpdate(ins.OnConflict.Exprs)

		// Build each of the SET expressions.
		mb.addUpdateCols(ins.OnConflict.Exprs)

		// Build the final upsert statement, including any returned expressions.
		mb.buildUpsert(returning)
	}

	return mb.outScope
}

// needExistingRows returns true if an Upsert statement needs to fetch existing
// rows in order to detect conflicts. In some cases, it is not necessary to
// fetch existing rows, and then the KV Put operation can be used to blindly
// insert a new record or overwrite an existing record. This is possible when:
//
//   1. There are no secondary indexes. Existing values are needed to delete
//      secondary index rows when the update causes them to move.
//   2. There are no implicit partitioning columns in the primary index.
//   3. All non-key columns (including mutation columns) have insert and update
//      values specified for them.
//   4. Each update value is the same as the corresponding insert value.
//   5. There are no inbound foreign keys containing non-key columns.
//
// TODO(andyk): The fast path is currently only enabled when the UPSERT alias
// is explicitly selected by the user. It's possible to fast path some queries
// of the form INSERT ... ON CONFLICT, but the utility is low and there are lots
// of edge cases (that caused real correctness bugs #13437 #13962). As a result,
// this support was removed and needs to re-enabled. See #14482.
func (mb *mutationBuilder) needExistingRows() bool {
	if mb.tab.DeletableIndexCount() > 1 {
		return true
	}

	// If there are any implicit partitioning columns in the primary index,
	// these columns will need to be fetched.
	primaryIndex := mb.tab.Index(cat.PrimaryIndex)
	// TODO(mgartner): It should be possible to perform an UPSERT fast path for a
	// non-partitioned hash-sharded primary index, but this restriction disallows
	// it.
	if primaryIndex.ImplicitColumnCount() > 0 {
		return true
	}

	// Key columns are never updated and are assumed to be the same as the insert
	// values.
	// TODO(andyk): This is not true in the case of composite key encodings. See
	// issue #34518.
	keyOrds := getIndexLaxKeyOrdinals(mb.tab.Index(cat.PrimaryIndex))
	for i, n := 0, mb.tab.ColumnCount(); i < n; i++ {
		if keyOrds.Contains(i) {
			// #1: Don't consider key columns.
			continue
		}
		if kind := mb.tab.Column(i).Kind(); kind == cat.System || kind == cat.Inverted {
			// #2: Don't consider system or inverted columns.
			continue
		}
		insertColID := mb.insertColIDs[i]
		if insertColID == 0 {
			// #2: Non-key column does not have insert value specified.
			return true
		}
		if insertColID != mb.updateColIDs[i] {
			// #3: Update value is not same as corresponding insert value.
			return true
		}
	}

	// If there are inbound foreign key constraints that contain any non-key
	// columns, we need the existing values.
	for i, n := 0, mb.tab.InboundForeignKeyCount(); i < n; i++ {
		for j, m := 0, mb.tab.InboundForeignKey(i).ColumnCount(); j < m; j++ {
			ord := mb.tab.InboundForeignKey(i).ReferencedColumnOrdinal(mb.tab, j)
			if !keyOrds.Contains(ord) {
				return true
			}
		}
	}

	return false
}

// addTargetNamedColsForInsert adds a list of user-specified column names to the
// list of table columns that are the target of the Insert operation.
func (mb *mutationBuilder) addTargetNamedColsForInsert(names tree.NameList) {
	if len(mb.targetColList) != 0 {
		panic(errors.AssertionFailedf("addTargetNamedColsForInsert cannot be called more than once"))
	}

	// Add target table columns by the names specified in the Insert statement.
	mb.addTargetColsByName(names)

	// Ensure that primary key columns are in the target column list, or that
	// they have default values.
	mb.checkPrimaryKeyForInsert()

	// Ensure that foreign keys columns are in the target column list, or that
	// they have default values.
	mb.checkForeignKeysForInsert()
}

// checkPrimaryKeyForInsert ensures that the columns of the primary key are
// either assigned values by the INSERT statement, or else have default/computed
// values. If neither condition is true, checkPrimaryKeyForInsert raises an
// error.
func (mb *mutationBuilder) checkPrimaryKeyForInsert() {
	primary := mb.tab.Index(cat.PrimaryIndex)
	for i, n := 0, primary.KeyColumnCount(); i < n; i++ {
		col := primary.Column(i)
		if col.HasDefault() || col.IsComputed() {
			// The column has a default or computed value.
			continue
		}

		colID := mb.tabID.ColumnID(col.Ordinal())
		if mb.targetColSet.Contains(colID) {
			// The column is explicitly specified in the target name list.
			continue
		}

		panic(pgerror.Newf(pgcode.InvalidForeignKey,
			"missing %q primary key column", col.ColName()))
	}
}

// checkForeignKeysForInsert ensures that all composite foreign keys that
// specify the matching method as MATCH FULL have all of their columns assigned
// values by the INSERT statement, or else have default/computed values.
// Alternatively, all columns can be unspecified. If neither condition is true,
// checkForeignKeys raises an error. Here is an example:
//
//   CREATE TABLE orders (
//     id INT,
//     cust_id INT,
//     state STRING,
//     FOREIGN KEY (cust_id, state) REFERENCES customers (id, state) MATCH FULL
//   )
//
//   INSERT INTO orders (cust_id) VALUES (1)
//
// This INSERT statement would trigger a static error, because only cust_id is
// specified in the INSERT statement. Either the state column must be specified
// as well, or else neither column can be specified.
func (mb *mutationBuilder) checkForeignKeysForInsert() {
	for i, n := 0, mb.tab.OutboundForeignKeyCount(); i < n; i++ {
		fk := mb.tab.OutboundForeignKey(i)
		numCols := fk.ColumnCount()

		// This check should only be performed on composite foreign keys that use
		// the MATCH FULL method.
		if numCols < 2 || fk.MatchMethod() != tree.MatchFull {
			continue
		}

		var missingCols []string
		allMissing := true
		for j := 0; j < numCols; j++ {
			ord := fk.OriginColumnOrdinal(mb.tab, j)
			col := mb.tab.Column(ord)
			if col.HasDefault() || col.IsComputed() {
				// The column has a default value.
				allMissing = false
				continue
			}

			colID := mb.tabID.ColumnID(ord)
			if mb.targetColSet.Contains(colID) {
				// The column is explicitly specified in the target name list.
				allMissing = false
				continue
			}

			missingCols = append(missingCols, string(col.ColName()))
		}
		if allMissing {
			continue
		}

		switch len(missingCols) {
		case 0:
			// Do nothing.
		case 1:
			panic(pgerror.Newf(pgcode.ForeignKeyViolation,
				"missing value for column %q in multi-part foreign key", missingCols[0]))
		default:
			sort.Strings(missingCols)
			panic(pgerror.Newf(pgcode.ForeignKeyViolation,
				"missing values for columns %q in multi-part foreign key", missingCols))
		}
	}
}

// addTargetTableColsForInsert adds up to maxCols columns to the list of columns
// that will be set by an INSERT operation. Non-mutation columns are added from
// the target table in the same order they appear in its schema. This method is
// used when the target columns are not explicitly specified in the INSERT
// statement:
//
//   INSERT INTO t VALUES (1, 2, 3)
//
// In this example, the first three columns of table t would be added as target
// columns.
func (mb *mutationBuilder) addTargetTableColsForInsert(maxCols int) {
	if len(mb.targetColList) != 0 {
		panic(errors.AssertionFailedf("addTargetTableColsForInsert cannot be called more than once"))
	}

	// Only consider non-mutation columns, since mutation columns are hidden from
	// the SQL user.
	numCols := 0
	for i, n := 0, mb.tab.ColumnCount(); i < n && numCols < maxCols; i++ {
		// Skip mutation, hidden or system columns.
		col := mb.tab.Column(i)
		if col.Kind() != cat.Ordinary || col.Visibility() != cat.Visible {
			continue
		}

		mb.addTargetCol(i)
		numCols++
	}

	// Ensure that the number of input columns does not exceed the number of
	// target columns.
	mb.checkNumCols(len(mb.targetColList), maxCols)
}

// buildInputForInsert constructs the memo group for the input expression and
// constructs a new output scope containing that expression's output columns.
func (mb *mutationBuilder) buildInputForInsert(inScope *scope, inputRows *tree.Select) {
	// Handle DEFAULT VALUES case by creating a single empty row as input.
	if inputRows == nil {
		mb.outScope = inScope.push()
		mb.outScope.expr = mb.b.factory.ConstructValues(memo.ScalarListWithEmptyTuple, &memo.ValuesPrivate{
			Cols: opt.ColList{},
			ID:   mb.md.NextUniqueID(),
		})
		return
	}

	// If there are already required target columns, then those will provide
	// desired input types. Otherwise, input columns are mapped to the table's
	// non-hidden columns by corresponding ordinal position. Exclude hidden
	// columns to prevent this statement from writing hidden columns:
	//
	//   INSERT INTO <table> VALUES (...)
	//
	// However, hidden columns can be written if the target columns were
	// explicitly specified:
	//
	//   INSERT INTO <table> (...) VALUES (...)
	//
	var desiredTypes []*types.T
	if len(mb.targetColList) != 0 {
		desiredTypes = make([]*types.T, len(mb.targetColList))
		for i, colID := range mb.targetColList {
			desiredTypes[i] = mb.md.ColumnMeta(colID).Type
		}
	} else {
		desiredTypes = make([]*types.T, 0, mb.tab.ColumnCount())
		for i, n := 0, mb.tab.ColumnCount(); i < n; i++ {
			if tabCol := mb.tab.Column(i); tabCol.Visibility() == cat.Visible && tabCol.Kind() == cat.Ordinary {
				desiredTypes = append(desiredTypes, tabCol.DatumType())
			}
		}
	}

	mb.outScope = mb.b.buildStmt(inputRows, desiredTypes, inScope)

	if len(mb.targetColList) != 0 {
		// Target columns already exist, so ensure that the number of input
		// columns exactly matches the number of target columns.
		mb.checkNumCols(len(mb.targetColList), len(mb.outScope.cols))
	} else {
		// No target columns have been added by previous steps, so add columns
		// that are implicitly targeted by the input expression.
		mb.addTargetTableColsForInsert(len(mb.outScope.cols))
	}

	// Loop over input columns and:
	//   1. Type check each column
	//   2. Check if the INSERT violates a GENERATED ALWAYS AS IDENTITY column.
	//   2. Assign name to each column
	//   3. Add column ID to the insertColIDs list.
	for i := range mb.outScope.cols {
		inCol := &mb.outScope.cols[i]
		ord := mb.tabID.ColumnOrdinal(mb.targetColList[i])

		// Raise an error if the target column is a `GENERATED ALWAYS AS
		// IDENTITY` column. Such a column is not allowed to be explicitly
		// written to.
		//
		// TODO(janexing): Implement the OVERRIDING SYSTEM VALUE syntax for
		// INSERT which allows a GENERATED ALWAYS AS IDENTITY column to be
		// overwritten.
		// See https://github.com/cockroachdb/cockroach/issues/68201.
		if col := mb.tab.Column(ord); col.IsGeneratedAlwaysAsIdentity() {
			colName := string(col.ColName())
			panic(sqlerrors.NewGeneratedAlwaysAsIdentityColumnOverrideError(colName))
		}

		// Assign name of input column.
		inCol.name = scopeColName(tree.Name(mb.md.ColumnMeta(mb.targetColList[i]).Alias))

		// Record the ID of the column that contains the value to be inserted
		// into the corresponding target table column.
		mb.insertColIDs[ord] = inCol.id
	}

	// Add assignment casts for insert columns.
	mb.addAssignmentCasts(mb.insertColIDs)
}

// addSynthesizedColsForInsert wraps an Insert input expression with a Project
// operator containing any default (or nullable) columns and any computed
// columns that are not yet part of the target column list. This includes all
// write-only mutation columns, since they must always have default or computed
// values.
func (mb *mutationBuilder) addSynthesizedColsForInsert() {
	// Start by adding non-computed columns that have not already been explicitly
	// specified in the query. Do this before adding computed columns, since those
	// may depend on non-computed columns.
	mb.addSynthesizedDefaultCols(
		mb.insertColIDs,
		true,  /* includeOrdinary */
		false, /* applyOnUpdate */
	)

	// Add assignment casts for default column values.
	mb.addAssignmentCasts(mb.insertColIDs)

	// Now add all computed columns.
	mb.addSynthesizedComputedCols(mb.insertColIDs, false /* restrict */)

	// Add assignment casts for computed column values.
	mb.addAssignmentCasts(mb.insertColIDs)
}

// buildInsert constructs an Insert operator, possibly wrapped by a Project
// operator that corresponds to the given RETURNING clause.
func (mb *mutationBuilder) buildInsert(returning tree.ReturningExprs) {
	// Disambiguate names so that references in any expressions, such as a
	// check constraint, refer to the correct columns.
	mb.disambiguateColumns()

	// Add any check constraint boolean columns to the input.
	mb.addCheckConstraintCols(false /* isUpdate */)

	// Project partial index PUT boolean columns.
	mb.projectPartialIndexPutCols()

	mb.buildUniqueChecksForInsert()

	mb.buildFKChecksForInsert()

	private := mb.makeMutationPrivate(returning != nil)
	mb.outScope.expr = mb.b.factory.ConstructInsert(
		mb.outScope.expr, mb.uniqueChecks, mb.fkChecks, private,
	)

	mb.buildReturning(returning)
}

// buildInputForDoNothing wraps the input expression in ANTI JOIN expressions,
// one for each arbiter on the target table. See the comment header for
// Builder.buildInsert for an example.
func (mb *mutationBuilder) buildInputForDoNothing(inScope *scope, onConflict *tree.OnConflict) {
	// Determine the set of arbiter indexes and constraints to use to check for
	// conflicts.
	mb.arbiters = mb.findArbiters(onConflict)
	insertColScope := mb.outScope.replace()
	insertColScope.appendColumnsFromScope(mb.outScope)

	// Ignore any ordering requested by the input.
	// TODO(andyk): do we need to do more here?
	mb.outScope.ordering = nil

	// Create an anti-join for each arbiter.
	mb.arbiters.ForEach(func(name string, conflictOrds util.FastIntSet, pred tree.Expr, canaryOrd int) {
		mb.buildAntiJoinForDoNothingArbiter(inScope, conflictOrds, pred)
	})

	// Create an UpsertDistinctOn for each arbiter. This must happen after all
	// conflicting rows are removed with the anti-joins created above, to avoid
	// removing valid rows (see #59125).
	mb.arbiters.ForEach(func(name string, conflictOrds util.FastIntSet, pred tree.Expr, canaryOrd int) {
		// If the arbiter has a partial predicate, project a new column that
		// allows the UpsertDistinctOn to only de-duplicate insert rows that
		// satisfy the predicate. See projectPartialArbiterDistinctColumn for
		// more details.
		var partialDistinctCol *scopeColumn
		if pred != nil {
			partialDistinctCol = mb.projectPartialArbiterDistinctColumn(
				insertColScope, pred, name,
			)
		}

		mb.buildDistinctOnForArbiter(insertColScope, conflictOrds, partialDistinctCol, "" /* errorOnDup */)
	})

	mb.targetColList = make(opt.ColList, 0, mb.tab.ColumnCount())
	mb.targetColSet = opt.ColSet{}
}

// buildInputForUpsert assumes that the output scope already contains the insert
// columns. It left-joins each insert row to the target table, using the given
// conflict columns as the join condition. It also selects one of the table
// columns to be a "canary column" that can be tested to determine whether a
// given insert row conflicts with an existing row in the table. If it is null,
// then there is no conflict.
func (mb *mutationBuilder) buildInputForUpsert(
	inScope *scope, onConflict *tree.OnConflict, whereClause *tree.Where,
) {
	// Determine the set of arbiter indexes and constraints to use to check for
	// conflicts.
	mb.arbiters = mb.findArbiters(onConflict)
	// TODO(mgartner): Add support for multiple arbiter indexes or constraints,
	//  similar to buildInputForDoNothing.
	if mb.arbiters.Len() > 1 {
		panic(unimplemented.NewWithIssue(53170,
			"there are multiple unique or exclusion constraints matching the ON CONFLICT specification"))
	}

	insertColScope := mb.outScope.replace()
	insertColScope.appendColumnsFromScope(mb.outScope)

	// Ignore any ordering requested by the input.
	mb.outScope.ordering = nil

	// Create an UpsertDistinctOn and a left-join for the single arbiter.
	var canaryCol *scopeColumn
	mb.arbiters.ForEach(func(name string, conflictOrds util.FastIntSet, pred tree.Expr, canaryOrd int) {
		// If the arbiter has a partial predicate, project a new column that
		// allows the UpsertDistinctOn to only de-duplicate insert rows that
		// satisfy the predicate. See projectPartialArbiterDistinctColumn for
		// more details.
		var partialDistinctCol *scopeColumn
		if pred != nil {
			partialDistinctCol = mb.projectPartialArbiterDistinctColumn(
				insertColScope, pred, name,
			)
		}

		// Ensure that input is distinct on the conflict columns. Otherwise, the
		// Upsert could affect the same row more than once, which can lead to
		// index corruption. See issue #44466 for more context.
		//
		// Ignore any ordering requested by the input. Since the
		// EnsureUpsertDistinctOn operator does not allow multiple rows in
		// distinct groupings, the internal ordering is meaningless (and can
		// trigger a misleading error in buildDistinctOn if present).
		mb.buildDistinctOnForArbiter(insertColScope, conflictOrds, partialDistinctCol, duplicateUpsertErrText)

		// Re-alias all INSERT columns so that they are accessible as if they
		// were part of a special data source named "crdb_internal.excluded".
		for i := range mb.outScope.cols {
			mb.outScope.cols[i].table = excludedTableName
		}

		// Create a left-join for the arbiter.
		mb.buildLeftJoinForUpsertArbiter(
			inScope, conflictOrds, pred,
		)

		// Record a not-null "canary" column. After the left-join, this will be
		// null if no conflict has been detected, or not null otherwise. At
		// least one not-null column must exist, since primary key columns are
		// not-null.
		canaryCol = &mb.fetchScope.cols[canaryOrd]
		mb.canaryColID = canaryCol.id
	})

	// Add a filter from the WHERE clause if one exists.
	if whereClause != nil {
		where := &tree.Where{
			Type: whereClause.Type,
			Expr: &tree.OrExpr{
				Left: &tree.ComparisonExpr{
					Operator: treecmp.MakeComparisonOperator(treecmp.IsNotDistinctFrom),
					Left:     canaryCol,
					Right:    tree.DNull,
				},
				Right: whereClause.Expr,
			},
		}
		mb.b.buildWhere(where, mb.outScope)
	}

	mb.targetColList = make(opt.ColList, 0, mb.tab.ColumnCount())
	mb.targetColSet = opt.ColSet{}
}

// setUpsertCols sets the list of columns to be updated in case of conflict.
// There are two cases to handle:
//
//   1. Target columns are explicitly specified:
//        UPSERT INTO abc (col1, col2, ...) <input-expr>
//
//   2. Target columns are implicitly derived:
//        UPSERT INTO abc <input-expr>
//
// In case #1, only the columns that were specified by the user will be updated.
// In case #2, all non-mutation columns in the table will be updated.
//
// Note that primary key columns (i.e. the conflict detection columns) are never
// updated. This can have an impact in unusual cases where equal SQL values have
// different representations. For example:
//
//   CREATE TABLE abc (a DECIMAL PRIMARY KEY, b DECIMAL)
//   INSERT INTO abc VALUES (1, 2.0)
//   UPSERT INTO abc VALUES (1.0, 2)
//
// The UPSERT statement will update the value of column "b" from 2 => 2.0, but
// will not modify column "a".
func (mb *mutationBuilder) setUpsertCols(insertCols tree.NameList) {
	if len(insertCols) != 0 {
		for _, name := range insertCols {
			// Table column must exist, since existence of insertCols has already
			// been checked previously.
			ord := findPublicTableColumnByName(mb.tab, name)
			mb.updateColIDs[ord] = mb.insertColIDs[ord]
		}
	} else {
		copy(mb.updateColIDs, mb.insertColIDs)
	}

	// Never update mutation or system columns.
	for i, n := 0, mb.tab.ColumnCount(); i < n; i++ {
		col := mb.tab.Column(i)
		if col.IsMutation() || col.Kind() == cat.System {
			mb.updateColIDs[i] = 0
		}
	}

	// Never update primary key columns. Implicit partitioning columns are not
	// considered part of the primary key in this case.
	conflictIndex := mb.tab.Index(cat.PrimaryIndex)
	skipCols := conflictIndex.ImplicitColumnCount()
	for i, n := skipCols, conflictIndex.KeyColumnCount(); i < n; i++ {
		mb.updateColIDs[conflictIndex.Column(i).Ordinal()] = 0
	}
}

// buildUpsert constructs an Upsert operator, possibly wrapped by a Project
// operator that corresponds to the given RETURNING clause.
func (mb *mutationBuilder) buildUpsert(returning tree.ReturningExprs) {
	// Merge input insert and update columns using CASE expressions.
	mb.projectUpsertColumns()

	// Disambiguate names so that references in any expressions, such as a
	// check constraint, refer to the correct columns.
	mb.disambiguateColumns()

	// Add any check constraint boolean columns to the input.
	mb.addCheckConstraintCols(false /* isUpdate */)

	// Add the partial index predicate expressions to the table metadata.
	// These expressions are used to prune fetch columns during
	// normalization.
	mb.b.addPartialIndexPredicatesForTable(mb.md.TableMeta(mb.tabID), nil /* scan */)

	// Project partial index PUT and DEL boolean columns.
	//
	// In some cases existing rows may not be fetched for an UPSERT (see
	// mutationBuilder.needExistingRows for more details). In theses cases
	// there is no need to project partial index DEL columns and
	// mb.fetchScope will be nil. Therefore, we only project partial index
	// PUT columns.
	if mb.needExistingRows() {
		mb.projectPartialIndexPutAndDelCols()
	} else {
		mb.projectPartialIndexPutCols()
	}

	mb.buildUniqueChecksForUpsert()

	mb.buildFKChecksForUpsert()

	private := mb.makeMutationPrivate(returning != nil)
	mb.outScope.expr = mb.b.factory.ConstructUpsert(
		mb.outScope.expr, mb.uniqueChecks, mb.fkChecks, private,
	)

	mb.buildReturning(returning)
}

// projectUpsertColumns projects a set of merged columns that will be either
// inserted into the target table, or else used to update an existing row,
// depending on whether the canary column is null. For example:
//
//   UPSERT INTO ab VALUES (ins_a, ins_b) ON CONFLICT (a) DO UPDATE SET b=upd_b
//
// will cause the columns to be projected:
//
//   SELECT
//     fetch_a,
//     fetch_b,
//     CASE WHEN fetch_a IS NULL ins_a ELSE fetch_a END AS ups_a,
//     CASE WHEN fetch_b IS NULL ins_b ELSE upd_b END AS ups_b,
//   FROM (SELECT ins_a, ins_b, upd_b, fetch_a, fetch_b FROM ...)
//
// For each column, a CASE expression is created that toggles between the insert
// and update values depending on whether the canary column is null. These
// columns can then feed into any constraint checking expressions, which operate
// on the final result values.
func (mb *mutationBuilder) projectUpsertColumns() {
	projectionsScope := mb.outScope.replace()
	projectionsScope.appendColumnsFromScope(mb.outScope)

	// Add a new column for each target table column that needs to be upserted.
	// This can include mutation columns.
	for i, n := 0, mb.tab.ColumnCount(); i < n; i++ {
		insertColID := mb.insertColIDs[i]
		updateColID := mb.updateColIDs[i]
		if updateColID == 0 {
			updateColID = mb.fetchColIDs[i]
		}

		// Skip columns that will only be inserted or only updated.
		if insertColID == 0 || updateColID == 0 {
			continue
		}

		// Skip columns where the insert value and update value are the same.
		if insertColID == updateColID {
			continue
		}

		col := mb.tab.Column(i)
		// Skip system columns.
		if col.Kind() == cat.System {
			continue
		}

		// Generate CASE that toggles between insert and update column.
		caseExpr := mb.b.factory.ConstructCase(
			memo.TrueSingleton,
			memo.ScalarListExpr{
				mb.b.factory.ConstructWhen(
					mb.b.factory.ConstructIs(
						mb.b.factory.ConstructVariable(mb.canaryColID),
						memo.NullSingleton,
					),
					mb.b.factory.ConstructVariable(insertColID),
				),
			},
			mb.b.factory.ConstructVariable(updateColID),
		)

		name := scopeColName(col.ColName()).WithMetadataName(
			fmt.Sprintf("upsert_%s", mb.tab.Column(i).ColName()),
		)
		typ := mb.md.ColumnMeta(insertColID).Type
		scopeCol := mb.b.synthesizeColumn(projectionsScope, name, typ, nil /* expr */, caseExpr)

		// Update the scope ordinals for the update columns that are involved in
		// the Upsert. The new columns will be used by the Upsert operator in place
		// of the original columns. Also set the scope ordinals for the upsert
		// columns, as those columns can be used by RETURNING columns.
		if mb.updateColIDs[i] != 0 {
			mb.updateColIDs[i] = scopeCol.id
		}
		mb.upsertColIDs[i] = scopeCol.id
	}

	mb.b.constructProjectForScope(mb.outScope, projectionsScope)
	mb.outScope = projectionsScope
}
