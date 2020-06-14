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
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
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
	mutExpr, inputExpr memo.RelExpr, colList opt.ColList, p *memo.MutationPrivate,
) (execPlan, error) {
	if b.shouldApplyImplicitLockingToMutationInput(mutExpr) {
		// Re-entrance is not possible because mutations are never nested.
		b.forceForUpdateLocking = true
		defer func() { b.forceForUpdateLocking = false }()
	}

	input, err := b.buildRelational(inputExpr)
	if err != nil {
		return execPlan{}, err
	}

	if p.WithID != 0 {
		// The input might have extra columns that are used only by FK checks; make
		// sure we don't project them away.
		cols := inputExpr.Relational().OutputCols.Copy()
		for _, c := range colList {
			cols.Remove(c)
		}
		for c, ok := cols.Next(0); ok; c, ok = cols.Next(c + 1) {
			colList = append(colList, c)
		}
	}

	input, err = b.ensureColumns(input, colList, nil, inputExpr.ProvidedPhysical().Ordering)
	if err != nil {
		return execPlan{}, err
	}

	if p.WithID != 0 {
		label := fmt.Sprintf("buffer %d", p.WithID)
		bufferNode, err := b.factory.ConstructBuffer(input.root, label)
		if err != nil {
			return execPlan{}, err
		}

		b.addBuiltWithExpr(p.WithID, input.outputCols, bufferNode)
		input.root = bufferNode
	}
	return input, nil
}

func (b *Builder) buildInsert(ins *memo.InsertExpr) (execPlan, error) {
	if ep, ok, err := b.tryBuildFastPathInsert(ins); err != nil || ok {
		return ep, err
	}
	// Construct list of columns that only contains columns that need to be
	// inserted (e.g. delete-only mutation columns don't need to be inserted).
	colList := make(opt.ColList, 0, len(ins.InsertCols)+len(ins.CheckCols)+len(ins.IndexPredicateCols))
	colList = appendColsWhenPresent(colList, ins.InsertCols)
	colList = appendColsWhenPresent(colList, ins.CheckCols)
	colList = appendColsWhenPresent(colList, ins.IndexPredicateCols)
	input, err := b.buildMutationInput(ins, ins.Input, colList, &ins.MutationPrivate)
	if err != nil {
		return execPlan{}, err
	}

	// Construct the Insert node.
	tab := b.mem.Metadata().Table(ins.Table)
	insertOrds := ordinalSetFromColList(ins.InsertCols)
	checkOrds := ordinalSetFromColList(ins.CheckCols)
	returnOrds := ordinalSetFromColList(ins.ReturnCols)
	node, err := b.factory.ConstructInsert(
		input.root,
		tab,
		insertOrds,
		returnOrds,
		checkOrds,
		b.allowAutoCommit && len(ins.Checks) == 0 && len(ins.FKCascades) == 0,
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
	if !b.allowInsertFastPath {
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
		out.InsertCols = make([]exec.TableColumnOrdinal, len(lookupJoin.KeyCols))
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
			out.InsertCols[i] = exec.TableColumnOrdinal(findCol(ins.InsertCols, inputCol))
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

	colList := make(opt.ColList, 0, len(ins.InsertCols)+len(ins.CheckCols)+len(ins.IndexPredicateCols))
	colList = appendColsWhenPresent(colList, ins.InsertCols)
	colList = appendColsWhenPresent(colList, ins.CheckCols)
	colList = appendColsWhenPresent(colList, ins.IndexPredicateCols)
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

	input, err := b.buildMutationInput(upd, upd.Input, colList, &upd.MutationPrivate)
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

	node, err := b.factory.ConstructUpdate(
		input.root,
		tab,
		fetchColOrds,
		updateColOrds,
		returnColOrds,
		checkOrds,
		passthroughCols,
		b.allowAutoCommit && len(upd.Checks) == 0 && len(upd.FKCascades) == 0,
	)
	if err != nil {
		return execPlan{}, err
	}

	if err := b.buildFKChecks(upd.Checks); err != nil {
		return execPlan{}, err
	}

	if err := b.buildFKCascades(upd.WithID, upd.FKCascades); err != nil {
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

	input, err := b.buildMutationInput(ups, ups.Input, colList, &ups.MutationPrivate)
	if err != nil {
		return execPlan{}, err
	}

	// Construct the Upsert node.
	md := b.mem.Metadata()
	tab := md.Table(ups.Table)
	canaryCol := exec.NodeColumnOrdinal(-1)
	if ups.CanaryCol != 0 {
		canaryCol = input.getNodeColumnOrdinal(ups.CanaryCol)
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
		b.allowAutoCommit && len(ups.Checks) == 0 && len(ups.FKCascades) == 0,
	)
	if err != nil {
		return execPlan{}, err
	}

	if err := b.buildFKChecks(ups.Checks); err != nil {
		return execPlan{}, err
	}

	if err := b.buildFKCascades(ups.WithID, ups.FKCascades); err != nil {
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
	if ep, ok, err := b.tryBuildDeleteRange(del); err != nil || ok {
		return ep, err
	}

	// Ensure that order of input columns matches order of target table columns.
	//
	// TODO(andyk): Using ensureColumns here can result in an extra Render.
	// Upgrade execution engine to not require this.
	colList := make(opt.ColList, 0, len(del.FetchCols))
	colList = appendColsWhenPresent(colList, del.FetchCols)

	input, err := b.buildMutationInput(del, del.Input, colList, &del.MutationPrivate)
	if err != nil {
		return execPlan{}, err
	}

	// Construct the Delete node.
	md := b.mem.Metadata()
	tab := md.Table(del.Table)
	fetchColOrds := ordinalSetFromColList(del.FetchCols)
	returnColOrds := ordinalSetFromColList(del.ReturnCols)
	node, err := b.factory.ConstructDelete(
		input.root,
		tab,
		fetchColOrds,
		returnColOrds,
		b.allowAutoCommit && len(del.Checks) == 0 && len(del.FKCascades) == 0,
	)
	if err != nil {
		return execPlan{}, err
	}

	if err := b.buildFKChecks(del.Checks); err != nil {
		return execPlan{}, err
	}

	if err := b.buildFKCascades(del.WithID, del.FKCascades); err != nil {
		return execPlan{}, err
	}

	// Construct the output column map.
	ep := execPlan{root: node}
	if del.NeedResults() {
		ep.outputCols = mutationOutputColMap(del)
	}

	return ep, nil
}

// tryBuildDeleteRange attempts to construct a fast DeleteRange execution for a
// logical Delete operator, checking all required conditions. See
// exec.Factory.ConstructDeleteRange.
func (b *Builder) tryBuildDeleteRange(del *memo.DeleteExpr) (_ execPlan, ok bool, _ error) {
	// If rows need to be returned from the Delete operator (i.e. RETURNING
	// clause), no fast path is possible, because row values must be fetched.
	if del.NeedResults() {
		return execPlan{}, false, nil
	}

	// Check for simple Scan input operator without a limit; anything else is not
	// supported by a range delete.
	if scan, ok := del.Input.(*memo.ScanExpr); !ok || scan.HardLimit != 0 {
		return execPlan{}, false, nil
	}

	tab := b.mem.Metadata().Table(del.Table)
	if tab.DeletableIndexCount() > 1 {
		// Any secondary index prevents fast path, because separate delete batches
		// must be formulated to delete rows from them.
		return execPlan{}, false, nil
	}

	primaryIdx := tab.Index(cat.PrimaryIndex)

	// If the table is interleaved in another table, we cannot use the fast path.
	if primaryIdx.InterleaveAncestorCount() > 0 {
		return execPlan{}, false, nil
	}

	if primaryIdx.InterleavedByCount() > 0 {
		return b.tryBuildDeleteRangeOnInterleaving(del, tab)
	}

	// No other tables interleaved inside this table. We can use the fast path
	// if this table is not referenced by any foreign keys (because the
	// integrity of those references must be checked).
	if tab.InboundForeignKeyCount() > 0 {
		return execPlan{}, false, nil
	}

	ep, err := b.buildDeleteRange(del, nil /* interleavedTables */)
	if err != nil {
		return execPlan{}, false, err
	}
	return ep, true, nil
}

// tryBuildDeleteRangeOnInterleaving attempts to construct a fast DeleteRange
// execution for a logical Delete operator when the table is at the root of an
// interleaving hierarchy.
//
// We can use DeleteRange only when foreign keys are set up such that a deletion
// of a row cascades into deleting all interleaved rows with the same prefix.
// More specifically, the following conditions must apply:
//  - none of the tables in the hierarchy have secondary indexes;
//  - none of the tables in the hierarchy are referenced by any tables outside
//    the hierarchy;
//  - all foreign key references between tables in the hierarchy have columns
//    that match the interleaving;
//  - all tables in the interleaving hierarchy have at least an ON DELETE
//    CASCADE foreign key reference to an ancestor.
//
func (b *Builder) tryBuildDeleteRangeOnInterleaving(
	del *memo.DeleteExpr, root cat.Table,
) (_ execPlan, ok bool, _ error) {
	// To check the conditions above, we explore the entire hierarchy using
	// breadth-first search.
	queue := make([]cat.Table, 0, root.Index(cat.PrimaryIndex).InterleavedByCount())
	tables := make(map[cat.StableID]cat.Table)
	tables[root.ID()] = root
	queue = append(queue, root)
	for queuePos := 0; queuePos < len(queue); queuePos++ {
		currTab := queue[queuePos]

		if currTab.DeletableIndexCount() > 1 {
			return execPlan{}, false, nil
		}

		currIdx := currTab.Index(cat.PrimaryIndex)
		for i, n := 0, currIdx.InterleavedByCount(); i < n; i++ {
			// We don't care about the index ID because we bail if any of the tables
			// have any secondary indexes anyway.
			tableID, _ := currIdx.InterleavedBy(i)
			if tab, ok := tables[tableID]; ok {
				err := errors.AssertionFailedf("multiple interleave paths to table %s", tab.Name())
				return execPlan{}, false, err
			}
			ds, _, err := b.catalog.ResolveDataSourceByID(context.TODO(), cat.Flags{}, tableID)
			if err != nil {
				return execPlan{}, false, err
			}
			child := ds.(cat.Table)
			tables[tableID] = child
			queue = append(queue, child)
		}
	}

	// Verify that there are no "inbound" foreign key references from outside the
	// hierarchy and that all foreign key references between tables in the hierarchy
	// match the interleaving (i.e. a prefix of the PK of the child references the
	// PK of the ancestor).
	for _, parent := range queue {
		for i, n := 0, parent.InboundForeignKeyCount(); i < n; i++ {
			fk := parent.InboundForeignKey(i)
			child, ok := tables[fk.OriginTableID()]
			if !ok {
				// Foreign key from a table outside of the hierarchy.
				return execPlan{}, false, nil
			}
			childIdx := child.Index(cat.PrimaryIndex)
			parentIdx := parent.Index(cat.PrimaryIndex)
			numCols := fk.ColumnCount()
			if parentIdx.KeyColumnCount() != numCols || childIdx.KeyColumnCount() < numCols {
				return execPlan{}, false, nil
			}
			for i := 0; i < numCols; i++ {
				if fk.OriginColumnOrdinal(child, i) != childIdx.Column(i).Ordinal {
					return execPlan{}, false, nil
				}
				if fk.ReferencedColumnOrdinal(parent, i) != parentIdx.Column(i).Ordinal {
					return execPlan{}, false, nil
				}
			}
		}
	}

	// Finally, verify that each table (except for the root) has an ON DELETE
	// CASCADE foreign key reference to another table in the hierarchy.
	for _, tab := range queue[1:] {
		found := false
		for i, n := 0, tab.OutboundForeignKeyCount(); i < n; i++ {
			fk := tab.OutboundForeignKey(i)
			if fk.DeleteReferenceAction() == tree.Cascade && tables[fk.ReferencedTableID()] != nil {
				// Note that we must have already checked above that this foreign key matches
				// the interleaving.
				found = true
				break
			}
		}
		if !found {
			return execPlan{}, false, nil
		}
	}

	ep, err := b.buildDeleteRange(del, queue[1:])
	if err != nil {
		return execPlan{}, false, err
	}
	return ep, true, nil
}

// buildDeleteRange constructs a DeleteRange operator that deletes contiguous
// rows in the primary index; the caller must have already checked the
// conditions which allow use of DeleteRange.
func (b *Builder) buildDeleteRange(
	del *memo.DeleteExpr, interleavedTables []cat.Table,
) (execPlan, error) {
	// tryBuildDeleteRange has already validated that input is a Scan operator.
	scan := del.Input.(*memo.ScanExpr)
	tab := b.mem.Metadata().Table(scan.Table)
	needed, _ := b.getColumns(scan.Cols, scan.Table)
	maxKeys := 0
	if len(interleavedTables) == 0 {
		// Calculate the maximum number of keys that the scan could return by
		// multiplying the number of possible result rows by the number of column
		// families of the table. The factory uses this information to determine
		// whether to allow autocommit.
		// We don't do this if there are interleaved children, as we don't know how
		// many children rows may be in range.
		maxKeys = int(b.indexConstraintMaxResults(scan)) * tab.FamilyCount()
	}
	// Other mutations only allow auto-commit if there are no FK checks or
	// cascades. In this case, we won't actually execute anything for the checks
	// or cascades - if we got this far, we determined that the FKs match the
	// interleaving hierarchy and a delete range is sufficient.
	root, err := b.factory.ConstructDeleteRange(
		tab,
		needed,
		scan.Constraint,
		interleavedTables,
		maxKeys,
		b.allowAutoCommit,
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
func ordinalSetFromColList(colList opt.ColList) util.FastIntSet {
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
				keyVals[i] = row[query.getNodeColumnOrdinal(col)]
			}
			return mkFKCheckErr(md, c, keyVals)
		}
		node, err := b.factory.ConstructErrorIfRows(query.root, mkErr)
		if err != nil {
			return err
		}
		b.checks = append(b.checks, node)
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
		lex.EncodeEscapedSQLIdent(&msg, string(origin.Alias.ObjectName))
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
			lex.EncodeEscapedSQLIdent(&details, string(referenced.Alias.ObjectName))
			details.WriteByte('.')
		}
	} else {
		// Generate an error of the form:
		//   ERROR:  delete on table "parent" violates foreign key constraint
		//           "child_child_p_fkey" on table "child"
		//   DETAIL: Key (p)=(1) is still referenced from table "child".
		fk := referenced.Table.InboundForeignKey(c.FKOrdinal)
		fmt.Fprintf(&msg, "%s on table ", c.OpName)
		lex.EncodeEscapedSQLIdent(&msg, string(referenced.Alias.ObjectName))
		msg.WriteString(" violates foreign key constraint ")
		lex.EncodeEscapedSQLIdent(&msg, fk.Name())
		msg.WriteString(" on table ")
		lex.EncodeEscapedSQLIdent(&msg, string(origin.Alias.ObjectName))

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
		lex.EncodeEscapedSQLIdent(&details, string(origin.Alias.ObjectName))
		details.WriteByte('.')
	}

	return errors.WithDetail(
		pgerror.Newf(pgcode.ForeignKeyViolation, "%s", msg.String()),
		details.String(),
	)
}

func (b *Builder) buildFKCascades(withID opt.WithID, cascades memo.FKCascades) error {
	if len(cascades) == 0 {
		return nil
	}
	cb, err := makeCascadeBuilder(b, withID)
	if err != nil {
		return err
	}
	for i := range cascades {
		b.cascades = append(b.cascades, cb.setupCascade(&cascades[i]))
	}
	return nil
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

// forUpdateLocking is the row-level locking mode used by mutations during their
// initial row scan, when such locking is deemed desirable. The locking mode is
// equivalent that used by a SELECT ... FOR UPDATE statement.
var forUpdateLocking = &tree.LockingItem{Strength: tree.ForUpdate}

// shouldApplyImplicitLockingToMutationInput determines whether or not the
// builder should apply a FOR UPDATE row-level locking mode to the initial row
// scan of a mutation expression.
func (b *Builder) shouldApplyImplicitLockingToMutationInput(mutExpr memo.RelExpr) bool {
	switch t := mutExpr.(type) {
	case *memo.InsertExpr:
		// Unlike with the other three mutation expressions, it never makes
		// sense to apply implicit row-level locking to the input of an INSERT
		// expression because any contention results in unique constraint
		// violations.
		return false

	case *memo.UpdateExpr:
		return b.shouldApplyImplicitLockingToUpdateInput(t)

	case *memo.UpsertExpr:
		return b.shouldApplyImplicitLockingToUpsertInput(t)

	case *memo.DeleteExpr:
		return b.shouldApplyImplicitLockingToDeleteInput(t)

	default:
		panic(errors.AssertionFailedf("unexpected mutation expression %T", t))
	}
}

// shouldApplyImplicitLockingToUpdateInput determines whether or not the builder
// should apply a FOR UPDATE row-level locking mode to the initial row scan of
// an UPDATE statement.
//
// Conceptually, if we picture an UPDATE statement as the composition of a
// SELECT statement and an INSERT statement (with loosened semantics around
// existing rows) then this method determines whether the builder should perform
// the following transformation:
//
//   UPDATE t = SELECT FROM t + INSERT INTO t
//   =>
//   UPDATE t = SELECT FROM t FOR UPDATE + INSERT INTO t
//
// The transformation is conditional on the UPDATE expression tree matching a
// pattern. Specifically, the FOR UPDATE locking mode is only used during the
// initial row scan when all row filters have been pushed into the ScanExpr. If
// the statement includes any filters that cannot be pushed into the scan then
// no row-level locking mode is applied. The rationale here is that FOR UPDATE
// locking is not necessary for correctness due to serializable isolation, so it
// is strictly a performance optimization for contended writes. Therefore, it is
// not worth risking the transformation being a pessimization, so it is only
// applied when doing so does not risk creating artificial contention.
func (b *Builder) shouldApplyImplicitLockingToUpdateInput(upd *memo.UpdateExpr) bool {
	if !b.evalCtx.SessionData.ImplicitSelectForUpdate {
		return false
	}

	// Try to match the Update's input expression against the pattern:
	//
	//   [Project] [IndexJoin] Scan
	//
	input := upd.Input
	if proj, ok := input.(*memo.ProjectExpr); ok {
		input = proj.Input
	}
	if idxJoin, ok := input.(*memo.IndexJoinExpr); ok {
		input = idxJoin.Input
	}
	_, ok := input.(*memo.ScanExpr)
	return ok
}

// tryApplyImplicitLockingToUpsertInput determines whether or not the builder
// should apply a FOR UPDATE row-level locking mode to the initial row scan of
// an UPSERT statement.
//
// TODO(nvanbenschoten): implement this method to match on appropriate Upsert
// expression trees and apply a row-level locking mode.
func (b *Builder) shouldApplyImplicitLockingToUpsertInput(ups *memo.UpsertExpr) bool {
	return false
}

// tryApplyImplicitLockingToDeleteInput determines whether or not the builder
// should apply a FOR UPDATE row-level locking mode to the initial row scan of
// an DELETE statement.
//
// TODO(nvanbenschoten): implement this method to match on appropriate Delete
// expression trees and apply a row-level locking mode.
func (b *Builder) shouldApplyImplicitLockingToDeleteInput(del *memo.DeleteExpr) bool {
	return false
}
