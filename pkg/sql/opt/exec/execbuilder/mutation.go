// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execbuilder

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

func (b *Builder) buildMutationInput(
	inputExpr memo.RelExpr, colList opt.ColList, p *memo.MutationPrivate,
) (execPlan, error) {
	input, err := b.buildRelational(inputExpr)
	if err != nil {
		return execPlan{}, err
	}

	input, err = b.ensureColumns(input, colList, nil, inputExpr.ProvidedPhysical().Ordering)
	if err != nil {
		return execPlan{}, err
	}

	if p.WithID != 0 {
		label := fmt.Sprintf("buffer %d", p.WithID)
		input.root, err = b.factory.ConstructBuffer(input.root, label)
		if err != nil {
			return execPlan{}, err
		}

		b.addBuiltWithExpr(p.WithID, input.outputCols, input.root)
	}
	return input, nil
}

func (b *Builder) buildInsert(ins *memo.InsertExpr) (execPlan, error) {
	if ep, ok, err := b.tryBuildFastPathInsert(ins); err != nil || ok {
		return ep, err
	}
	// Construct list of columns that only contains columns that need to be
	// inserted (e.g. delete-only mutation columns don't need to be inserted).
	colList := make(opt.ColList, 0, len(ins.InsertCols)+len(ins.CheckCols))
	colList = appendColsWhenPresent(colList, ins.InsertCols)
	colList = appendColsWhenPresent(colList, ins.CheckCols)
	input, err := b.buildMutationInput(ins.Input, colList, &ins.MutationPrivate)
	if err != nil {
		return execPlan{}, err
	}

	// Construct the Insert node.
	tab := b.mem.Metadata().Table(ins.Table)
	insertOrds := ordinalSetFromColList(ins.InsertCols)
	checkOrds := ordinalSetFromColList(ins.CheckCols)
	returnOrds := ordinalSetFromColList(ins.ReturnCols)
	// If we planned FK checks, disable the execution code for FK checks.
	disableExecFKs := !ins.FKFallback
	node, err := b.factory.ConstructInsert(
		input.root,
		tab,
		insertOrds,
		returnOrds,
		checkOrds,
		b.allowAutoCommit && len(ins.Checks) == 0,
		disableExecFKs,
	)
	if err != nil {
		return execPlan{}, err
	}
	// Construct the output column map.
	ep := execPlan{root: node}
	if ins.NeedResults() {
		ep.outputCols = mutationOutputColMap(ins)
	}

	if err := b.buildFKChecks(ins.Checks); err != nil {
		return execPlan{}, err
	}

	return ep, nil
}

// tryBuildFastPathInsert attempts to construct an insert using the fast path,
// checking all required conditions. See exec.Factory.ConstructInsertFastPath.
func (b *Builder) tryBuildFastPathInsert(ins *memo.InsertExpr) (_ execPlan, ok bool, _ error) {
	// If FKFallback is set, the optimizer-driven FK checks are disabled. We must
	// use the legacy path.
	if !b.allowInsertFastPath || ins.FKFallback {
		return execPlan{}, false, nil
	}

	// Conditions from ConstructFastPathInsert:
	//
	//  - there are no other mutations in the statement, and the output of the
	//    insert is not processed through side-effecting expressions (i.e. we can
	//    auto-commit);
	if !b.allowAutoCommit {
		return execPlan{}, false, nil
	}

	//  - the input is Values with at most InsertFastPathMaxRows, and there are no
	//    subqueries;
	values, ok := ins.Input.(*memo.ValuesExpr)
	if !ok || values.ChildCount() > exec.InsertFastPathMaxRows || values.Relational().HasSubquery {
		return execPlan{}, false, nil
	}

	md := b.mem.Metadata()
	tab := md.Table(ins.Table)

	//  - there are no self-referencing foreign keys;
	//  - all FK checks can be performed using direct lookups into unique indexes.
	fkChecks := make([]exec.InsertFastPathFKCheck, len(ins.Checks))
	for i := range ins.Checks {
		c := &ins.Checks[i]
		if md.Table(c.ReferencedTable).ID() == md.Table(ins.Table).ID() {
			// Self-referencing FK.
			return execPlan{}, false, nil
		}
		fk := tab.OutboundForeignKey(c.FKOrdinal)
		lookupJoin, isLookupJoin := c.Check.(*memo.LookupJoinExpr)
		if !isLookupJoin || lookupJoin.JoinType != opt.AntiJoinOp {
			// Not a lookup anti-join.
			return execPlan{}, false, nil
		}
		if len(lookupJoin.On) > 0 ||
			len(lookupJoin.KeyCols) != fk.ColumnCount() {
			return execPlan{}, false, nil
		}
		inputExpr := lookupJoin.Input
		// Ignore any select (used to deal with NULLs).
		if sel, isSelect := inputExpr.(*memo.SelectExpr); isSelect {
			inputExpr = sel.Input
		}
		withScan, isWithScan := inputExpr.(*memo.WithScanExpr)
		if !isWithScan {
			return execPlan{}, false, nil
		}
		if withScan.With != ins.WithID {
			return execPlan{}, false, nil
		}

		out := &fkChecks[i]
		out.InsertCols = make([]exec.ColumnOrdinal, len(lookupJoin.KeyCols))
		findCol := func(cols opt.ColList, col opt.ColumnID) int {
			res, ok := cols.Find(col)
			if !ok {
				panic(errors.AssertionFailedf("cannot find column %d", col))
			}
			return res
		}
		for i, keyCol := range lookupJoin.KeyCols {
			// The keyCol comes from the WithScan operator. We must find the matching
			// column in the mutation input.
			withColOrd := findCol(withScan.OutCols, keyCol)
			inputCol := withScan.InCols[withColOrd]
			out.InsertCols[i] = exec.ColumnOrdinal(findCol(ins.InsertCols, inputCol))
		}

		out.ReferencedTable = md.Table(lookupJoin.Table)
		out.ReferencedIndex = out.ReferencedTable.Index(lookupJoin.Index)
		out.MatchMethod = fk.MatchMethod()
		out.MkErr = func(values tree.Datums) error {
			if len(values) != len(out.InsertCols) {
				return errors.AssertionFailedf("invalid FK violation values")
			}
			// This is a little tricky. The column ordering might not match between
			// the FK reference and the index we're looking up. We have to reshuffle
			// the values to fix that.
			fkVals := make(tree.Datums, len(values))
			for i, ordinal := range out.InsertCols {
				for j := range out.InsertCols {
					if fk.OriginColumnOrdinal(tab, j) == int(ordinal) {
						fkVals[j] = values[i]
						break
					}
				}
			}
			for i := range fkVals {
				if fkVals[i] == nil {
					return errors.AssertionFailedf("invalid column mapping")
				}
			}
			return mkFKCheckErr(md, c, fkVals)
		}
	}

	colList := make(opt.ColList, 0, len(ins.InsertCols)+len(ins.CheckCols))
	colList = appendColsWhenPresent(colList, ins.InsertCols)
	colList = appendColsWhenPresent(colList, ins.CheckCols)
	if !colList.Equals(values.Cols) {
		// We have a Values input, but the columns are not in the right order. For
		// example:
		//   INSERT INTO ab (SELECT y, x FROM (VALUES (1, 10)) AS v (x, y))
		//
		// TODO(radu): we could rearrange the columns of the rows below, or add
		// a normalization rule that adds a Project to rearrange the Values node
		// columns.
		return execPlan{}, false, nil
	}

	rows, err := b.buildValuesRows(values)
	if err != nil {
		return execPlan{}, false, err
	}

	// Construct the InsertFastPath node.
	insertOrds := ordinalSetFromColList(ins.InsertCols)
	checkOrds := ordinalSetFromColList(ins.CheckCols)
	returnOrds := ordinalSetFromColList(ins.ReturnCols)
	node, err := b.factory.ConstructInsertFastPath(
		rows,
		tab,
		insertOrds,
		returnOrds,
		checkOrds,
		fkChecks,
	)
	if err != nil {
		return execPlan{}, false, err
	}
	// Construct the output column map.
	ep := execPlan{root: node}
	if ins.NeedResults() {
		ep.outputCols = mutationOutputColMap(ins)
	}
	return ep, true, nil
}

func (b *Builder) buildUpdate(upd *memo.UpdateExpr) (execPlan, error) {
	// Currently, the execution engine requires one input column for each fetch
	// and update expression, so use ensureColumns to map and reorder columns so
	// that they correspond to target table columns. For example:
	//
	//   UPDATE xyz SET x=1, y=1
	//
	// Here, the input has just one column (because the constant is shared), and
	// so must be mapped to two separate update columns.
	//
	// TODO(andyk): Using ensureColumns here can result in an extra Render.
	// Upgrade execution engine to not require this.
	cnt := len(upd.FetchCols) + len(upd.UpdateCols) + len(upd.PassthroughCols) + len(upd.CheckCols)
	colList := make(opt.ColList, 0, cnt)
	colList = appendColsWhenPresent(colList, upd.FetchCols)
	colList = appendColsWhenPresent(colList, upd.UpdateCols)

	// The RETURNING clause of the Update can refer to the columns
	// in any of the FROM tables. As a result, the Update may need
	// to passthrough those columns so the projection above can use
	// them.
	if upd.NeedResults() {
		colList = appendColsWhenPresent(colList, upd.PassthroughCols)
	}

	colList = appendColsWhenPresent(colList, upd.CheckCols)
	input, err := b.buildMutationInput(upd.Input, colList, &upd.MutationPrivate)
	if err != nil {
		return execPlan{}, err
	}

	// Construct the Update node.
	md := b.mem.Metadata()
	tab := md.Table(upd.Table)
	fetchColOrds := ordinalSetFromColList(upd.FetchCols)
	updateColOrds := ordinalSetFromColList(upd.UpdateCols)
	returnColOrds := ordinalSetFromColList(upd.ReturnCols)
	checkOrds := ordinalSetFromColList(upd.CheckCols)

	// Construct the result columns for the passthrough set.
	var passthroughCols sqlbase.ResultColumns
	if upd.NeedResults() {
		for _, passthroughCol := range upd.PassthroughCols {
			colMeta := b.mem.Metadata().ColumnMeta(passthroughCol)
			passthroughCols = append(passthroughCols, sqlbase.ResultColumn{Name: colMeta.Alias, Typ: colMeta.Type})
		}
	}

	disableExecFKs := !upd.FKFallback
	node, err := b.factory.ConstructUpdate(
		input.root,
		tab,
		fetchColOrds,
		updateColOrds,
		returnColOrds,
		checkOrds,
		passthroughCols,
		b.allowAutoCommit && len(upd.Checks) == 0,
		disableExecFKs,
	)
	if err != nil {
		return execPlan{}, err
	}

	if err := b.buildFKChecks(upd.Checks); err != nil {
		return execPlan{}, err
	}

	// Construct the output column map.
	ep := execPlan{root: node}
	if upd.NeedResults() {
		ep.outputCols = mutationOutputColMap(upd)
	}
	return ep, nil
}

func (b *Builder) buildUpsert(ups *memo.UpsertExpr) (execPlan, error) {
	// Currently, the execution engine requires one input column for each insert,
	// fetch, and update expression, so use ensureColumns to map and reorder
	// columns so that they correspond to target table columns. For example:
	//
	//   INSERT INTO xyz (x, y) VALUES (1, 1)
	//   ON CONFLICT (x) DO UPDATE SET x=2, y=2
	//
	// Here, both insert values and update values come from the same input column
	// (because the constants are shared), and so must be mapped to separate
	// output columns.
	//
	// If CanaryCol = 0, then this is the "blind upsert" case, which uses a KV
	// "Put" to insert new rows or blindly overwrite existing rows. Existing rows
	// do not need to be fetched or separately updated (i.e. ups.FetchCols and
	// ups.UpdateCols are both empty).
	//
	// TODO(andyk): Using ensureColumns here can result in an extra Render.
	// Upgrade execution engine to not require this.
	cnt := len(ups.InsertCols) + len(ups.FetchCols) + len(ups.UpdateCols) + len(ups.CheckCols) + 1
	colList := make(opt.ColList, 0, cnt)
	colList = appendColsWhenPresent(colList, ups.InsertCols)
	colList = appendColsWhenPresent(colList, ups.FetchCols)
	colList = appendColsWhenPresent(colList, ups.UpdateCols)
	if ups.CanaryCol != 0 {
		colList = append(colList, ups.CanaryCol)
	}
	colList = appendColsWhenPresent(colList, ups.CheckCols)
	input, err := b.buildMutationInput(ups.Input, colList, &ups.MutationPrivate)
	if err != nil {
		return execPlan{}, err
	}

	// Construct the Upsert node.
	md := b.mem.Metadata()
	tab := md.Table(ups.Table)
	canaryCol := exec.ColumnOrdinal(-1)
	if ups.CanaryCol != 0 {
		canaryCol = input.getColumnOrdinal(ups.CanaryCol)
	}
	insertColOrds := ordinalSetFromColList(ups.InsertCols)
	fetchColOrds := ordinalSetFromColList(ups.FetchCols)
	updateColOrds := ordinalSetFromColList(ups.UpdateCols)
	returnColOrds := ordinalSetFromColList(ups.ReturnCols)
	checkOrds := ordinalSetFromColList(ups.CheckCols)
	disableExecFKs := !ups.FKFallback
	node, err := b.factory.ConstructUpsert(
		input.root,
		tab,
		canaryCol,
		insertColOrds,
		fetchColOrds,
		updateColOrds,
		returnColOrds,
		checkOrds,
		b.allowAutoCommit && len(ups.Checks) == 0,
		disableExecFKs,
	)
	if err != nil {
		return execPlan{}, err
	}

	if err := b.buildFKChecks(ups.Checks); err != nil {
		return execPlan{}, err
	}

	// If UPSERT returns rows, they contain all non-mutation columns from the
	// table, in the same order they're defined in the table. Each output column
	// value is taken from an insert, fetch, or update column, depending on the
	// result of the UPSERT operation for that row.
	ep := execPlan{root: node}
	if ups.NeedResults() {
		ep.outputCols = mutationOutputColMap(ups)
	}
	return ep, nil
}

func (b *Builder) buildDelete(del *memo.DeleteExpr) (execPlan, error) {
	// Check for the fast-path delete case that can use a range delete.
	if b.canUseDeleteRange(del) {
		return b.buildDeleteRange(del)
	}

	// Ensure that order of input columns matches order of target table columns.
	//
	// TODO(andyk): Using ensureColumns here can result in an extra Render.
	// Upgrade execution engine to not require this.
	colList := make(opt.ColList, 0, len(del.FetchCols))
	colList = appendColsWhenPresent(colList, del.FetchCols)
	input, err := b.buildMutationInput(del.Input, colList, &del.MutationPrivate)
	if err != nil {
		return execPlan{}, err
	}

	// Construct the Delete node.
	md := b.mem.Metadata()
	tab := md.Table(del.Table)
	fetchColOrds := ordinalSetFromColList(del.FetchCols)
	returnColOrds := ordinalSetFromColList(del.ReturnCols)
	disableExecFKs := !del.FKFallback
	node, err := b.factory.ConstructDelete(
		input.root,
		tab,
		fetchColOrds,
		returnColOrds,
		b.allowAutoCommit && len(del.Checks) == 0,
		disableExecFKs,
	)
	if err != nil {
		return execPlan{}, err
	}

	if err := b.buildFKChecks(del.Checks); err != nil {
		return execPlan{}, err
	}

	// Construct the output column map.
	ep := execPlan{root: node}
	if del.NeedResults() {
		ep.outputCols = mutationOutputColMap(del)
	}

	return ep, nil
}

// canUseDeleteRange checks whether a logical Delete operator can be implemented
// by a fast delete range execution operator. This logic should be kept in sync
// with canDeleteFast.
func (b *Builder) canUseDeleteRange(del *memo.DeleteExpr) bool {
	// If rows need to be returned from the Delete operator (i.e. RETURNING
	// clause), no fast path is possible, because row values must be fetched.
	if del.NeedResults() {
		return false
	}

	tab := b.mem.Metadata().Table(del.Table)
	if tab.DeletableIndexCount() > 1 {
		// Any secondary index prevents fast path, because separate delete batches
		// must be formulated to delete rows from them.
		return false
	}
	if tab.IsInterleaved() {
		// There is a separate fast path for interleaved tables in sql/delete.go.
		return false
	}
	if tab.InboundForeignKeyCount() > 0 {
		// If the table is referenced by other tables' foreign keys, no fast path
		// is possible, because the integrity of those references must be checked.
		return false
	}

	// Check for simple Scan input operator without a limit; anything else is not
	// supported by a range delete.
	if scan, ok := del.Input.(*memo.ScanExpr); !ok || scan.HardLimit != 0 {
		return false
	}

	return true
}

// buildDeleteRange constructs a DeleteRange operator that deletes contiguous
// rows in the primary index. canUseDeleteRange should have already been called.
func (b *Builder) buildDeleteRange(del *memo.DeleteExpr) (execPlan, error) {
	// canUseDeleteRange has already validated that input is a Scan operator.
	scan := del.Input.(*memo.ScanExpr)
	tab := b.mem.Metadata().Table(scan.Table)
	needed, _ := b.getColumns(scan.Cols, scan.Table)
	// Calculate the maximum number of keys that the scan could return by
	// multiplying the number of possible result rows by the number of column
	// families of the table. The execbuilder needs this information to determine
	// whether or not allowAutoCommit can be enabled.
	maxKeys := int(b.indexConstraintMaxResults(scan)) * tab.FamilyCount()
	root, err := b.factory.ConstructDeleteRange(
		tab,
		needed,
		scan.Constraint,
		maxKeys,
		b.allowAutoCommit && len(del.Checks) == 0,
	)
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{root: root}, nil
}

// appendColsWhenPresent appends non-zero column IDs from the src list into the
// dst list, and returns the possibly grown list.
func appendColsWhenPresent(dst, src opt.ColList) opt.ColList {
	for _, col := range src {
		if col != 0 {
			dst = append(dst, col)
		}
	}
	return dst
}

// ordinalSetFromColList returns the set of ordinal positions of each non-zero
// column ID in the given list. This is used with mutation operators, which
// maintain lists that correspond to the target table, with zero column IDs
// indicating columns that are not involved in the mutation.
func ordinalSetFromColList(colList opt.ColList) exec.ColumnOrdinalSet {
	var res util.FastIntSet
	if colList == nil {
		return res
	}
	for i, col := range colList {
		if col != 0 {
			res.Add(i)
		}
	}
	return res
}

// mutationOutputColMap constructs a ColMap for the execPlan that maps from the
// opt.ColumnID of each output column to the ordinal position of that column in
// the result.
func mutationOutputColMap(mutation memo.RelExpr) opt.ColMap {
	private := mutation.Private().(*memo.MutationPrivate)
	tab := mutation.Memo().Metadata().Table(private.Table)
	outCols := mutation.Relational().OutputCols

	var colMap opt.ColMap
	ord := 0
	for i, n := 0, tab.DeletableColumnCount(); i < n; i++ {
		colID := private.Table.ColumnID(i)
		if outCols.Contains(colID) {
			colMap.Set(int(colID), ord)
			ord++
		}
	}

	// The output columns of the mutation will also include all
	// columns it allowed to pass through.
	for _, colID := range private.PassthroughCols {
		if colID != 0 {
			colMap.Set(int(colID), ord)
			ord++
		}
	}

	return colMap
}

func (b *Builder) buildFKChecks(checks memo.FKChecksExpr) error {
	md := b.mem.Metadata()
	for i := range checks {
		c := &checks[i]
		// Construct the query that returns FK violations.
		query, err := b.buildRelational(c.Check)
		if err != nil {
			return err
		}
		// Wrap the query in an error node.
		mkErr := func(row tree.Datums) error {
			keyVals := make(tree.Datums, len(c.KeyCols))
			for i, col := range c.KeyCols {
				keyVals[i] = row[query.getColumnOrdinal(col)]
			}
			return mkFKCheckErr(md, c, keyVals)
		}
		node, err := b.factory.ConstructErrorIfRows(query.root, mkErr)
		if err != nil {
			return err
		}
		b.postqueries = append(b.postqueries, node)
	}
	return nil
}

// mkFKCheckErr generates a user-friendly error describing a foreign key
// violation. The keyVals are the values that correspond to the
// cat.ForeignKeyConstraint columns.
func mkFKCheckErr(md *opt.Metadata, c *memo.FKChecksItem, keyVals tree.Datums) error {
	origin := md.TableMeta(c.OriginTable)
	referenced := md.TableMeta(c.ReferencedTable)

	var msg, details bytes.Buffer
	if c.FKOutbound {
		// Generate an error of the form:
		//   ERROR:  insert on table "child" violates foreign key constraint "foo"
		//   DETAIL: Key (child_p)=(2) is not present in table "parent".
		fk := origin.Table.OutboundForeignKey(c.FKOrdinal)
		fmt.Fprintf(&msg, "%s on table ", c.OpName)
		lex.EncodeEscapedSQLIdent(&msg, string(origin.Alias.TableName))
		msg.WriteString(" violates foreign key constraint ")
		lex.EncodeEscapedSQLIdent(&msg, fk.Name())

		details.WriteString("Key (")
		for i := 0; i < fk.ColumnCount(); i++ {
			if i > 0 {
				details.WriteString(", ")
			}
			col := origin.Table.Column(fk.OriginColumnOrdinal(origin.Table, i))
			details.WriteString(string(col.ColName()))
		}
		details.WriteString(")=(")
		sawNull := false
		for i, d := range keyVals {
			if i > 0 {
				details.WriteString(", ")
			}
			if d == tree.DNull {
				// If we see a NULL, this must be a MATCH FULL failure (otherwise the
				// row would have been filtered out).
				sawNull = true
				break
			}
			details.WriteString(d.String())
		}
		if sawNull {
			details.Reset()
			details.WriteString("MATCH FULL does not allow mixing of null and nonnull key values.")
		} else {
			details.WriteString(") is not present in table ")
			lex.EncodeEscapedSQLIdent(&details, string(referenced.Alias.TableName))
			details.WriteByte('.')
		}
	} else {
		// Generate an error of the form:
		//   ERROR:  delete on table "parent" violates foreign key constraint
		//           "child_child_p_fkey" on table "child"
		//   DETAIL: Key (p)=(1) is still referenced from table "child".
		fk := referenced.Table.InboundForeignKey(c.FKOrdinal)
		fmt.Fprintf(&msg, "%s on table ", c.OpName)
		lex.EncodeEscapedSQLIdent(&msg, string(referenced.Alias.TableName))
		msg.WriteString(" violates foreign key constraint ")
		lex.EncodeEscapedSQLIdent(&msg, fk.Name())
		msg.WriteString(" on table ")
		lex.EncodeEscapedSQLIdent(&msg, string(origin.Alias.TableName))

		details.WriteString("Key (")
		for i := 0; i < fk.ColumnCount(); i++ {
			if i > 0 {
				details.WriteString(", ")
			}
			col := referenced.Table.Column(fk.ReferencedColumnOrdinal(referenced.Table, i))
			details.WriteString(string(col.ColName()))
		}
		details.WriteString(")=(")
		for i, d := range keyVals {
			if i > 0 {
				details.WriteString(", ")
			}
			details.WriteString(d.String())
		}
		details.WriteString(") is still referenced from table ")
		lex.EncodeEscapedSQLIdent(&details, string(origin.Alias.TableName))
		details.WriteByte('.')
	}

	return errors.WithDetail(
		pgerror.New(pgcode.ForeignKeyViolation, msg.String()),
		details.String(),
	)
}

// canAutoCommit determines if it is safe to auto commit the mutation contained
// in the expression.
//
// Mutations can commit the transaction as part of the same KV request,
// potentially taking advantage of the 1PC optimization. This is not ok to do in
// general; a sufficient set of conditions is:
//   1. There is a single mutation in the query.
//   2. The mutation is the root operator, or it is directly under a Project
//      with no side-effecting expressions. An example of why we can't allow
//      side-effecting expressions: if the projection encounters a
//      division-by-zero error, the mutation shouldn't have been committed.
//
// An extra condition relates to how the FK checks are run. If they run before
// the mutation (via the insert fast path), auto commit is possible. If they run
// after the mutation (the general path), auto commit is not possible. It is up
// to the builder logic for each mutation to handle this.
//
// Note that there are other necessary conditions related to execution
// (specifically, that the transaction is implicit); it is up to the exec
// factory to take that into account as well.
func (b *Builder) canAutoCommit(rel memo.RelExpr) bool {
	if !rel.Relational().CanMutate {
		// No mutations in the expression.
		return false
	}

	switch rel.Op() {
	case opt.InsertOp, opt.UpsertOp, opt.UpdateOp, opt.DeleteOp:
		// Check that there aren't any more mutations in the input.
		// TODO(radu): this can go away when all mutations are under top-level
		// With ops.
		return !rel.Child(0).(memo.RelExpr).Relational().CanMutate

	case opt.ProjectOp:
		// Allow Project on top, as long as the expressions are not side-effecting.
		//
		// TODO(radu): for now, we only allow passthrough projections because not all
		// builtins that can error out are marked as side-effecting.
		proj := rel.(*memo.ProjectExpr)
		if len(proj.Projections) != 0 {
			return false
		}
		return b.canAutoCommit(proj.Input)

	default:
		return false
	}
}
