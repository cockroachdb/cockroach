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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	disableExecFKs := len(ins.Checks) > 0
	node, err := b.factory.ConstructInsert(
		input.root,
		tab,
		insertOrds,
		returnOrds,
		checkOrds,
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

func (b *Builder) buildUpdate(upd *memo.UpdateExpr) (execPlan, error) {
	// Currently, the execution engine requires one input column for each fetch
	// and update expression, so use ensureColumns to map and reorder colums so
	// that they correspond to target table columns. For example:
	//
	//   UPDATE xyz SET x=1, y=1
	//
	// Here, the input has just one column (because the constant is shared), and
	// so must be mapped to two separate update columns.
	//
	// TODO(andyk): Using ensureColumns here can result in an extra Render.
	// Upgrade execution engine to not require this.
	colList := make(opt.ColList, 0, len(upd.FetchCols)+len(upd.UpdateCols)+len(upd.CheckCols))
	colList = appendColsWhenPresent(colList, upd.FetchCols)
	colList = appendColsWhenPresent(colList, upd.UpdateCols)
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
	node, err := b.factory.ConstructUpdate(
		input.root,
		tab,
		fetchColOrds,
		updateColOrds,
		returnColOrds,
		checkOrds,
	)
	if err != nil {
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
	node, err := b.factory.ConstructUpsert(
		input.root,
		tab,
		canaryCol,
		insertColOrds,
		fetchColOrds,
		updateColOrds,
		returnColOrds,
		checkOrds,
	)
	if err != nil {
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
	disableExecFKs := len(del.Checks) > 0
	node, err := b.factory.ConstructDelete(
		input.root,
		tab,
		fetchColOrds,
		returnColOrds,
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

	root, err := b.factory.ConstructDeleteRange(tab, needed, scan.Constraint)
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
			origin := md.TableMeta(c.OriginTable)
			referenced := md.TableMeta(c.ReferencedTable)
			var fk cat.ForeignKeyConstraint
			if c.FKOutbound {
				fk = origin.Table.OutboundForeignKey(c.FKOrdinal)
			} else {
				fk = referenced.Table.InboundForeignKey(c.FKOrdinal)
			}

			var msg, details bytes.Buffer
			if c.FKOutbound {
				// Generate an error of the form:
				//   ERROR:  insert on table "child" violates foreign key constraint "foo"
				//   DETAIL: Key (child_p)=(2) is not present in table "parent".
				msg.WriteString("insert on table ")
				lex.EncodeEscapedSQLIdent(&msg, string(origin.Alias.TableName))
				msg.WriteString(" violates foreign key constraint ")
				lex.EncodeEscapedSQLIdent(&msg, fk.Name())

				details.WriteString("Key (")
				for i := 0; i < fk.ColumnCount(); i++ {
					if i > 0 {
						details.WriteString(" ,")
					}
					col := origin.Table.Column(fk.OriginColumnOrdinal(origin.Table, i))
					details.WriteString(string(col.ColName()))
				}
				details.WriteString(")=(")
				for i, col := range c.KeyCols {
					if i > 0 {
						details.WriteString(", ")
					}
					details.WriteString(row[query.getColumnOrdinal(col)].String())
				}
				details.WriteString(") is not present in table ")
				lex.EncodeEscapedSQLIdent(&details, string(referenced.Alias.TableName))
				details.WriteByte('.')
			} else {
				// Generate an error of the form:
				//   ERROR:  delete on table "parent" violates foreign key constraint "child_child_p_fkey" on table "child"
				//   DETAIL: Key (p)=(1) is still referenced from table "child".
				msg.WriteString("delete on table ")
				lex.EncodeEscapedSQLIdent(&msg, string(referenced.Alias.TableName))
				msg.WriteString(" violates foreign key constraint")
				// TODO(justin): get the name of the FK constraint (it's not populated
				// on this descriptor.
				msg.WriteString(" on table ")
				lex.EncodeEscapedSQLIdent(&msg, string(origin.Alias.TableName))
				// TODO(justin): get the details, the columns are not populated on this
				// descriptor.
			}

			return errors.WithDetail(
				pgerror.New(pgcode.ForeignKeyViolation, msg.String()),
				details.String(),
			)
		}
		node, err := b.factory.ConstructErrorIfRows(query.root, mkErr)
		if err != nil {
			return err
		}
		b.postqueries = append(b.postqueries, node)
	}
	return nil
}
