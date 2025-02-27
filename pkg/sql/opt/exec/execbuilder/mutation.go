// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execbuilder

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

func (b *Builder) buildMutationInput(
	mutExpr, inputExpr memo.RelExpr, colList opt.ColList, p *memo.MutationPrivate,
) (_ execPlan, err error) {
	toLock, err := b.shouldApplyImplicitLockingToMutationInput(mutExpr)
	if err != nil {
		return execPlan{}, err
	}
	if toLock != 0 {
		if b.forceForUpdateLocking != 0 {
			if b.evalCtx.SessionData().BufferedWritesEnabled || buildutil.CrdbTestBuild {
				// We currently don't expect this to happen, so we return an
				// assertion failure in test builds. Additionally, we will rely
				// on forceForUpdateLocking set properly when buffered writes
				// are enabled for correctness.
				return execPlan{}, errors.AssertionFailedf(
					"unexpectedly already locked %d, also want to lock %d", b.forceForUpdateLocking, toLock,
				)
			}
		}
		b.forceForUpdateLocking = toLock
		defer func() {
			b.forceForUpdateLocking = 0
		}()
	}

	input, inputCols, err := b.buildRelational(inputExpr)
	if err != nil {
		return execPlan{}, err
	}

	// TODO(mgartner/radu): This can incorrectly append columns in a FK cascade
	// update that are never used during execution. See issue #57097.
	if p.WithID != 0 {
		// The input might have extra columns that are used only by FK or unique
		// checks; make sure we don't project them away.
		cols := inputExpr.Relational().OutputCols.Copy()
		for _, c := range colList {
			cols.Remove(c)
		}
		for c, ok := cols.Next(0); ok; c, ok = cols.Next(c + 1) {
			colList = append(colList, c)
		}
	}

	// Currently, the execution engine requires one input column for each fetch,
	// insert, update, and delete expression, so use ensureColumns to map and
	// reorder columns so that they correspond to target table columns.
	// For example:
	//
	//   UPDATE xyz SET x=1, y=1
	//
	// Here, the input has just one column (because the constant is shared), and
	// so must be mapped to two separate update columns.
	//
	// TODO(andyk): Using ensureColumns here can result in an extra Render.
	// Upgrade execution engine to not require this.
	input, inputCols, err = b.ensureColumns(
		input, inputCols, inputExpr, colList,
		inputExpr.ProvidedPhysical().Ordering, true, /* reuseInputCols */
	)
	if err != nil {
		return execPlan{}, err
	}

	if p.WithID != 0 {
		label := fmt.Sprintf("buffer %d", p.WithID)
		bufferNode, err := b.factory.ConstructBuffer(input.root, label)
		if err != nil {
			return execPlan{}, err
		}

		b.addBuiltWithExpr(p.WithID, inputCols, bufferNode)
		input.root = bufferNode
	} else {
		b.colOrdsAlloc.Free(inputCols)
	}
	return input, nil
}

func (b *Builder) buildInsert(ins *memo.InsertExpr) (_ execPlan, outputCols colOrdMap, err error) {
	if ep, cols, ok, err := b.tryBuildFastPathInsert(ins); err != nil || ok {
		return ep, cols, err
	}
	// Construct list of columns that only contains columns that need to be
	// inserted (e.g. delete-only mutation columns don't need to be inserted).
	colList := appendColsWhenPresent(
		ins.InsertCols, ins.CheckCols, ins.PartialIndexPutCols,
		ins.VectorIndexPutPartitionCols, ins.VectorIndexPutQuantizedVecCols,
	)
	input, err := b.buildMutationInput(ins, ins.Input, colList, &ins.MutationPrivate)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// Construct the Insert node.
	tab := b.mem.Metadata().Table(ins.Table)
	insertOrds := ordinalSetFromColList(ins.InsertCols)
	checkOrds := ordinalSetFromColList(ins.CheckCols)
	returnOrds := ordinalSetFromColList(ins.ReturnCols)
	node, err := b.factory.ConstructInsert(
		input.root,
		tab,
		ins.ArbiterIndexes,
		ins.ArbiterConstraints,
		insertOrds,
		returnOrds,
		checkOrds,
		ins.UniqueWithTombstoneIndexes,
		b.allowAutoCommit && len(ins.UniqueChecks) == 0 &&
			len(ins.FKChecks) == 0 && len(ins.FKCascades) == 0 && ins.AfterTriggers == nil,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	// Construct the output column map.
	ep := execPlan{root: node}
	if ins.NeedResults() {
		outputCols = b.mutationOutputColMap(ins)
	}

	if err := b.buildUniqueChecks(ins.UniqueChecks); err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	if err := b.buildFKChecks(ins.FKChecks); err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	if err := b.buildAfterTriggers(ins.WithID, ins.AfterTriggers); err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	return ep, outputCols, nil
}

// tryBuildFastPathInsert attempts to construct an insert using the fast path,
// checking all required conditions. See exec.Factory.ConstructInsertFastPath.
func (b *Builder) tryBuildFastPathInsert(
	ins *memo.InsertExpr,
) (_ execPlan, outputCols colOrdMap, ok bool, _ error) {
	// Conditions from ConstructFastPathInsert:
	//
	//  - there are no other mutations in the statement, and the output of the
	//    insert is not processed through side-effecting expressions (i.e. we can
	//    auto-commit);
	//
	// This condition was taken into account in build().
	if !b.allowInsertFastPath {
		return execPlan{}, colOrdMap{}, false, nil
	}
	// If there are unique checks required, there must be the same number of fast
	// path unique checks.
	if len(ins.UniqueChecks) != len(ins.FastPathUniqueChecks) {
		return execPlan{}, colOrdMap{}, false, nil
	}
	// Do not attempt the fast path if there are any triggers.
	if ins.AfterTriggers != nil {
		return execPlan{}, colOrdMap{}, false, nil
	}

	insInput := ins.Input
	values, ok := insInput.(*memo.ValuesExpr)
	// Values expressions containing subqueries or UDFs, or having a size larger
	// than the max mutation batch size are disallowed.
	if !ok || !memo.ValuesLegalForInsertFastPath(values) {
		return execPlan{}, colOrdMap{}, false, nil
	}

	md := b.mem.Metadata()
	tab := md.Table(ins.Table)

	uniqChecks := make([]exec.InsertFastPathCheck, len(ins.UniqueChecks))
	for i := range ins.FastPathUniqueChecks {
		c := &ins.FastPathUniqueChecks[i]
		if len(c.DatumsFromConstraint) == 0 {
			// We need at least one DatumsFromConstraint in order to perform
			// uniqueness checks during fast-path insert. Even if DatumsFromConstraint
			// contains no Datums, that case indicates that all values to check come
			// from the input row.
			return execPlan{}, colOrdMap{}, false, nil
		}
		execFastPathCheck := &uniqChecks[i]
		// Set up the execbuilder structure from the elements built during
		// exploration.
		execFastPathCheck.ReferencedTable = md.Table(c.ReferencedTableID)
		execFastPathCheck.ReferencedIndex = execFastPathCheck.ReferencedTable.Index(c.ReferencedIndexOrdinal)
		execFastPathCheck.CheckOrdinal = c.CheckOrdinal

		// If there is a unique index with implicit partitioning columns, the fast
		// path can write tombstones to lock the row in all partitions.
		locking, err := b.buildLocking(ins.Table, c.Locking)
		if err != nil {
			return execPlan{}, colOrdMap{}, false, err
		}
		execFastPathCheck.Locking = locking
		execFastPathCheck.InsertCols = make([]exec.TableColumnOrdinal, len(c.InsertCols))
		for j, insertCol := range c.InsertCols {
			execFastPathCheck.InsertCols[j] = exec.TableColumnOrdinal(md.ColumnMeta(insertCol).Table.ColumnOrdinal(insertCol))
		}
		datumsFromConstraintSpec := c.DatumsFromConstraint
		execFastPathCheck.DatumsFromConstraint = make([]tree.Datums, len(datumsFromConstraintSpec))
		for j, row := range datumsFromConstraintSpec {
			execFastPathCheck.DatumsFromConstraint[j] = make(tree.Datums, tab.ColumnCount())
			tuple := row.(*memo.TupleExpr)
			if len(c.InsertCols) != len(tuple.Elems) {
				panic(errors.AssertionFailedf("expected %d tuple elements in insert fast path uniqueness check, found %d", len(c.InsertCols), len(tuple.Elems)))
			}
			for k := 0; k < len(tuple.Elems); k++ {
				var constDatum tree.Datum
				switch e := tuple.Elems[k].(type) {
				case *memo.ConstExpr:
					constDatum = e.Value
				case *memo.TrueExpr:
					constDatum = tree.DBoolTrue
				case *memo.FalseExpr:
					constDatum = tree.DBoolFalse
				default:
					return execPlan{}, colOrdMap{}, false, nil
				}
				execFastPathCheck.DatumsFromConstraint[j][execFastPathCheck.InsertCols[k]] = constDatum
			}
		}
		uniqCheck := &ins.UniqueChecks[i]
		// TODO(mgartner): We shouldn't keep references to md, uniqCheck, and
		// execFastPathCheck. Ideally, the query plan for the constraint would
		// produce the constraint column names as output columns. Then, the
		// error message could be constructed without needing to access the
		// catalog or metadata.
		execFastPathCheck.MkErr = func(values tree.Datums) error {
			return mkFastPathUniqueCheckErr(md, uniqCheck, values, execFastPathCheck.ReferencedIndex)
		}
	}

	//  - there are no self-referencing foreign keys;
	//  - all FK checks can be performed using direct lookups into unique indexes.
	fkChecks := make([]exec.InsertFastPathCheck, len(ins.FKChecks))
	for i := range ins.FKChecks {
		c := &ins.FKChecks[i]
		if md.Table(c.ReferencedTable).ID() == md.Table(ins.Table).ID() {
			// Self-referencing FK.
			return execPlan{}, colOrdMap{}, false, nil
		}
		fk := tab.OutboundForeignKey(c.FKOrdinal)
		lookupJoin, isLookupJoin := c.Check.(*memo.LookupJoinExpr)
		if !isLookupJoin || lookupJoin.JoinType != opt.AntiJoinOp {
			// Not a lookup anti-join.
			return execPlan{}, colOrdMap{}, false, nil
		}
		// TODO(rytaft): see if we can remove the requirement that LookupExpr is
		// empty.
		if len(lookupJoin.On) > 0 || len(lookupJoin.LookupExpr) > 0 ||
			len(lookupJoin.KeyCols) != fk.ColumnCount() {
			return execPlan{}, colOrdMap{}, false, nil
		}
		inputExpr := lookupJoin.Input
		// Ignore any select (used to deal with NULLs).
		if sel, isSelect := inputExpr.(*memo.SelectExpr); isSelect {
			inputExpr = sel.Input
		}
		withScan, isWithScan := inputExpr.(*memo.WithScanExpr)
		if !isWithScan {
			return execPlan{}, colOrdMap{}, false, nil
		}
		if withScan.With != ins.WithID {
			return execPlan{}, colOrdMap{}, false, nil
		}

		locking, err := b.buildLocking(lookupJoin.Table, lookupJoin.Locking)
		if err != nil {
			return execPlan{}, colOrdMap{}, false, err
		}

		out := &fkChecks[i]
		out.InsertCols = make([]exec.TableColumnOrdinal, len(lookupJoin.KeyCols))
		for j, keyCol := range lookupJoin.KeyCols {
			// The keyCol comes from the WithScan operator. We must find the matching
			// column in the mutation input.
			var withColOrd, inputColOrd int
			withColOrd, ok = withScan.OutCols.Find(keyCol)
			if !ok {
				return execPlan{}, colOrdMap{}, false, errors.AssertionFailedf("cannot find column %d", keyCol)
			}
			inputCol := withScan.InCols[withColOrd]
			inputColOrd, ok = ins.InsertCols.Find(inputCol)
			if !ok {
				return execPlan{}, colOrdMap{}, false, errors.AssertionFailedf("cannot find column %d", inputCol)
			}
			out.InsertCols[j] = exec.TableColumnOrdinal(inputColOrd)
		}

		out.ReferencedTable = md.Table(lookupJoin.Table)
		out.ReferencedIndex = out.ReferencedTable.Index(lookupJoin.Index)
		out.MatchMethod = fk.MatchMethod()
		out.Locking = locking
		out.MkErr = func(values tree.Datums) error {
			if len(values) != len(out.InsertCols) {
				return errors.AssertionFailedf("invalid FK violation values")
			}
			// This is a little tricky. The column ordering might not match between
			// the FK reference and the index we're looking up. We have to reshuffle
			// the values to fix that.
			fkVals := make(tree.Datums, len(values))
			for i := range fkVals {
				parentOrd := fk.ReferencedColumnOrdinal(out.ReferencedTable, i)
				for j := 0; j < out.ReferencedIndex.KeyColumnCount(); j++ {
					if out.ReferencedIndex.Column(j).Ordinal() == parentOrd {
						fkVals[i] = values[j]
						break
					}
				}
				if fkVals[i] == nil {
					return errors.AssertionFailedf("invalid column mapping")
				}
			}
			return mkFKCheckErr(md, c, fkVals)
		}
	}

	colList := appendColsWhenPresent(
		ins.InsertCols, ins.CheckCols, ins.PartialIndexPutCols,
		ins.VectorIndexDelPartitionCols, ins.VectorIndexPutQuantizedVecCols,
	)
	rows, err := b.buildValuesRows(values)
	if err != nil {
		return execPlan{}, colOrdMap{}, false, err
	}
	// We may need to rearrange the columns.
	rows, err = rearrangeColumns(values.Cols, rows, colList)
	if err != nil {
		return execPlan{}, colOrdMap{}, false, err
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
		uniqChecks,
		ins.UniqueWithTombstoneIndexes,
		b.allowAutoCommit,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, false, err
	}
	// Construct the output column map.
	ep := execPlan{root: node}
	if ins.NeedResults() {
		outputCols = b.mutationOutputColMap(ins)
	}
	return ep, outputCols, true, nil
}

// rearrangeColumns rearranges the columns in a matrix of TypedExpr values.
//
// Each column in inRows corresponds to a column in inCols. The values in the
// columns are rearranged so that they correspond to wantedCols. Note that
// wantedCols can contain the same column multiple times, in which case the
// values will be duplicated.
//
// Returns an error if wantedCols contains a column that isn't part of inCols.
func rearrangeColumns(
	inCols opt.ColList, inRows [][]tree.TypedExpr, wantedCols opt.ColList,
) (outRows [][]tree.TypedExpr, _ error) {
	if inCols.Equals(wantedCols) {
		// Nothing to do.
		return inRows, nil
	}

	outRows = makeTypedExprMatrix(len(inRows), len(wantedCols))
	for i, wanted := range wantedCols {
		j, ok := inCols.Find(wanted)
		if !ok {
			return nil, errors.AssertionFailedf("no column %d in input", wanted)
		}
		for rowIdx := range inRows {
			outRows[rowIdx][i] = inRows[rowIdx][j]
		}
	}

	return outRows, nil
}

func (b *Builder) buildUpdate(upd *memo.UpdateExpr) (_ execPlan, outputCols colOrdMap, err error) {
	var neededPassThroughCols opt.OptionalColList
	if upd.NeedResults() {
		// The RETURNING clause of the Update can refer to the columns
		// in any of the FROM tables. As a result, the Update may need
		// to passthrough those columns so the projection above can use
		// them.
		neededPassThroughCols = opt.OptionalColList(upd.PassthroughCols)
	}
	colList := appendColsWhenPresent(
		upd.FetchCols, upd.UpdateCols, neededPassThroughCols, upd.CheckCols,
		upd.PartialIndexPutCols, upd.PartialIndexDelCols,
		upd.VectorIndexPutPartitionCols, upd.VectorIndexPutQuantizedVecCols,
		upd.VectorIndexDelPartitionCols,
	)

	input, err := b.buildMutationInput(upd, upd.Input, colList, &upd.MutationPrivate)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// Construct the Update node.
	md := b.mem.Metadata()
	tab := md.Table(upd.Table)
	fetchColOrds := ordinalSetFromColList(upd.FetchCols)
	updateColOrds := ordinalSetFromColList(upd.UpdateCols)
	returnColOrds := ordinalSetFromColList(upd.ReturnCols)
	checkOrds := ordinalSetFromColList(upd.CheckCols)

	// Construct the result columns for the passthrough set.
	var passthroughCols colinfo.ResultColumns
	if upd.NeedResults() {
		for _, passthroughCol := range upd.PassthroughCols {
			colMeta := b.mem.Metadata().ColumnMeta(passthroughCol)
			passthroughCols = append(passthroughCols, colinfo.ResultColumn{Name: colMeta.Alias, Typ: colMeta.Type})
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
		upd.UniqueWithTombstoneIndexes,
		b.allowAutoCommit && len(upd.UniqueChecks) == 0 &&
			len(upd.FKChecks) == 0 && len(upd.FKCascades) == 0 && upd.AfterTriggers == nil,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	if err := b.buildUniqueChecks(upd.UniqueChecks); err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	if err := b.buildFKChecks(upd.FKChecks); err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	if err := b.buildFKCascades(upd.WithID, upd.FKCascades); err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	if err := b.buildAfterTriggers(upd.WithID, upd.AfterTriggers); err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// Construct the output column map.
	ep := execPlan{root: node}
	if upd.NeedResults() {
		outputCols = b.mutationOutputColMap(upd)
	}
	return ep, outputCols, nil
}

func (b *Builder) buildUpsert(ups *memo.UpsertExpr) (_ execPlan, outputCols colOrdMap, err error) {
	// If CanaryCol = 0, then this is the "blind upsert" case, which uses a KV
	// "Put" to insert new rows or blindly overwrite existing rows. Existing rows
	// do not need to be fetched or separately updated (i.e. ups.FetchCols and
	// ups.UpdateCols are both empty).
	colList := appendColsWhenPresent(
		ups.InsertCols, ups.FetchCols, ups.UpdateCols, opt.OptionalColList{ups.CanaryCol},
		ups.CheckCols, ups.PartialIndexPutCols, ups.PartialIndexDelCols,
		ups.VectorIndexPutPartitionCols, ups.VectorIndexPutQuantizedVecCols,
		ups.VectorIndexDelPartitionCols,
	)

	input, err := b.buildMutationInput(ups, ups.Input, colList, &ups.MutationPrivate)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// Construct the Upsert node.
	md := b.mem.Metadata()
	tab := md.Table(ups.Table)
	canaryCol := exec.NodeColumnOrdinal(-1)
	if ups.CanaryCol != 0 {
		// The canary column comes after the insert, fetch, and update columns.
		canaryCol = exec.NodeColumnOrdinal(
			ups.InsertCols.Len() + ups.FetchCols.Len() + ups.UpdateCols.Len(),
		)
		if colList[canaryCol] != ups.CanaryCol {
			return execPlan{}, colOrdMap{},
				errors.AssertionFailedf("canary column not found")
		}
	}
	insertColOrds := ordinalSetFromColList(ups.InsertCols)
	fetchColOrds := ordinalSetFromColList(ups.FetchCols)
	updateColOrds := ordinalSetFromColList(ups.UpdateCols)
	returnColOrds := ordinalSetFromColList(ups.ReturnCols)
	checkOrds := ordinalSetFromColList(ups.CheckCols)
	node, err := b.factory.ConstructUpsert(
		input.root,
		tab,
		ups.ArbiterIndexes,
		ups.ArbiterConstraints,
		canaryCol,
		insertColOrds,
		fetchColOrds,
		updateColOrds,
		returnColOrds,
		checkOrds,
		ups.UniqueWithTombstoneIndexes,
		b.allowAutoCommit && len(ups.UniqueChecks) == 0 &&
			len(ups.FKChecks) == 0 && len(ups.FKCascades) == 0 && ups.AfterTriggers == nil,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	if err := b.buildUniqueChecks(ups.UniqueChecks); err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	if err := b.buildFKChecks(ups.FKChecks); err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	if err := b.buildFKCascades(ups.WithID, ups.FKCascades); err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	if err := b.buildAfterTriggers(ups.WithID, ups.AfterTriggers); err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// If UPSERT returns rows, they contain all non-mutation columns from the
	// table, in the same order they're defined in the table. Each output column
	// value is taken from an insert, fetch, or update column, depending on the
	// result of the UPSERT operation for that row.
	ep := execPlan{root: node}
	if ups.NeedResults() {
		outputCols = b.mutationOutputColMap(ups)
	}
	return ep, outputCols, nil
}

func (b *Builder) buildDelete(del *memo.DeleteExpr) (_ execPlan, outputCols colOrdMap, err error) {
	// Check for the fast-path delete case that can use a range delete.
	if ep, ok, err := b.tryBuildDeleteRange(del); err != nil || ok {
		return ep, colOrdMap{}, err
	}
	var neededPassThroughCols opt.OptionalColList
	if del.NeedResults() {
		// The RETURNING clause of the Delete can refer to the columns
		// in any of the FROM tables. As a result, the Delete may need
		// to passthrough those columns so the projection above can use
		// them.
		neededPassThroughCols = opt.OptionalColList(del.PassthroughCols)
	}
	colList := appendColsWhenPresent(
		del.FetchCols, neededPassThroughCols, del.PartialIndexDelCols, del.VectorIndexDelPartitionCols,
	)

	input, err := b.buildMutationInput(del, del.Input, colList, &del.MutationPrivate)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	// Construct the Delete node.
	md := b.mem.Metadata()
	tab := md.Table(del.Table)
	fetchColOrds := ordinalSetFromColList(del.FetchCols)
	returnColOrds := ordinalSetFromColList(del.ReturnCols)

	// Construct the result columns for the passthrough set.
	var passthroughCols colinfo.ResultColumns
	if del.NeedResults() {
		passthroughCols = make(colinfo.ResultColumns, 0, len(del.PassthroughCols))
		for _, passthroughCol := range del.PassthroughCols {
			colMeta := b.mem.Metadata().ColumnMeta(passthroughCol)
			passthroughCols = append(passthroughCols, colinfo.ResultColumn{Name: colMeta.Alias, Typ: colMeta.Type})
		}
	}

	node, err := b.factory.ConstructDelete(
		input.root,
		tab,
		fetchColOrds,
		returnColOrds,
		passthroughCols,
		b.allowAutoCommit && len(del.FKChecks) == 0 &&
			len(del.FKCascades) == 0 && del.AfterTriggers == nil,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	if err := b.buildFKChecks(del.FKChecks); err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	if err := b.buildFKCascades(del.WithID, del.FKCascades); err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	if err := b.buildAfterTriggers(del.WithID, del.AfterTriggers); err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// Construct the output column map.
	ep := execPlan{root: node}
	if del.NeedResults() {
		outputCols = b.mutationOutputColMap(del)
	}

	return ep, outputCols, nil
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

	// We can use the fast path if we don't need to buffer the input to the
	// delete operator (for foreign key checks/cascades).
	if del.WithID != 0 {
		return execPlan{}, false, nil
	}

	ep, err := b.buildDeleteRange(del)
	if err != nil {
		return execPlan{}, false, err
	}
	if err := b.buildFKChecks(del.FKChecks); err != nil {
		return execPlan{}, false, err
	}
	if err := b.buildFKCascades(del.WithID, del.FKCascades); err != nil {
		return execPlan{}, false, err
	}
	if err := b.buildAfterTriggers(del.WithID, del.AfterTriggers); err != nil {
		return execPlan{}, false, err
	}
	return ep, true, nil
}

// buildDeleteRange constructs a DeleteRange operator that deletes contiguous
// rows in the primary index; the caller must have already checked the
// conditions which allow use of DeleteRange.
func (b *Builder) buildDeleteRange(del *memo.DeleteExpr) (execPlan, error) {
	// tryBuildDeleteRange has already validated that input is a Scan operator.
	scan := del.Input.(*memo.ScanExpr)
	tab := b.mem.Metadata().Table(scan.Table)
	needed, _ := b.getColumns(scan.Cols, scan.Table)

	autoCommit := false
	if b.allowAutoCommit {
		// Permitting autocommit in DeleteRange is very important, because DeleteRange
		// is used for simple deletes from primary indexes like
		// DELETE FROM t WHERE key = 1000
		// When possible, we need to make this a 1pc transaction for performance
		// reasons. At the same time, we have to be careful, because DeleteRange
		// returns all of the keys that it deleted - so we have to set a limit on the
		// DeleteRange request. But, trying to set autocommit and a limit on the
		// request doesn't work properly if the limit is hit. So, we permit autocommit
		// here if we can guarantee that the number of returned keys is finite and
		// relatively small.
		//
		// Mutations only allow auto-commit if there are no FK checks or cascades.

		if maxRows, ok := b.indexConstraintMaxResults(&scan.ScanPrivate, scan.Relational()); ok {
			if maxKeys := maxRows * uint64(tab.FamilyCount()); maxKeys <= row.TableTruncateChunkSize {
				autoCommit = true
			}
		}
		if len(del.FKChecks) > 0 || len(del.FKCascades) > 0 || del.AfterTriggers != nil {
			autoCommit = false
		}
	}

	root, err := b.factory.ConstructDeleteRange(
		tab,
		needed,
		scan.Constraint,
		autoCommit,
	)
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{root: root}, nil
}

// appendColsWhenPresent combines all non-zero column IDs from the given lists
// into a single column list, and returns the combined list.
func appendColsWhenPresent(lists ...opt.OptionalColList) opt.ColList {
	var cnt int
	for _, list := range lists {
		for _, id := range list {
			if id != 0 {
				cnt++
			}
		}
	}
	combined := make(opt.ColList, 0, cnt)
	for _, list := range lists {
		for _, col := range list {
			if col != 0 {
				combined = append(combined, col)
			}
		}
	}
	return combined
}

// ordinalSetFromColList returns the set of ordinal positions of each non-zero
// column ID in the given list. This is used with mutation operators, which
// maintain lists that correspond to the target table, with zero column IDs
// indicating columns that are not involved in the mutation.
func ordinalSetFromColList(colList opt.OptionalColList) intsets.Fast {
	var res intsets.Fast
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
func (b *Builder) mutationOutputColMap(mutation memo.RelExpr) colOrdMap {
	private := mutation.Private().(*memo.MutationPrivate)
	tab := b.mem.Metadata().Table(private.Table)
	outCols := mutation.Relational().OutputCols

	colMap := b.colOrdsAlloc.Alloc()
	ord := 0
	for i, n := 0, tab.ColumnCount(); i < n; i++ {
		colID := private.Table.ColumnID(i)
		// System columns should not be included in mutations.
		if outCols.Contains(colID) && tab.Column(i).Kind() != cat.System {
			colMap.Set(colID, ord)
			ord++
		}
	}

	// The output columns of the mutation will also include all
	// columns it allowed to pass through.
	for _, colID := range private.PassthroughCols {
		if colID != 0 {
			colMap.Set(colID, ord)
			ord++
		}
	}

	return colMap
}

// checkContainsLocking sets PlanFlagCheckContainsLocking based on whether we
// found locking while building a check query plan.
func (b *Builder) checkContainsLocking(mainContainsLocking bool) {
	if b.flags.IsSet(exec.PlanFlagContainsLocking) {
		b.flags.Set(exec.PlanFlagCheckContainsLocking)
	}
	if mainContainsLocking {
		b.flags.Set(exec.PlanFlagContainsLocking)
	}
}

// buildUniqueChecks builds uniqueness check queries. These check queries are
// used to enforce UNIQUE WITHOUT INDEX constraints.
//
// The checks consist of queries that will only return rows if a constraint is
// violated. Those queries are each wrapped in an ErrorIfRows operator, which
// will throw an appropriate error in case the inner query returns any rows.
func (b *Builder) buildUniqueChecks(checks memo.UniqueChecksExpr) error {
	defer b.checkContainsLocking(b.flags.IsSet(exec.PlanFlagContainsLocking))
	b.flags.Unset(exec.PlanFlagContainsLocking)
	md := b.mem.Metadata()
	for i := range checks {
		c := &checks[i]
		// Construct the query that returns uniqueness violations.
		query, queryCols, err := b.buildRelational(c.Check)
		if err != nil {
			return err
		}
		// Wrap the query in an error node.
		mkErr := func(row tree.Datums) error {
			keyVals := make(tree.Datums, len(c.KeyCols))
			for i, col := range c.KeyCols {
				ord, err := getNodeColumnOrdinal(queryCols, col)
				if err != nil {
					return err
				}
				keyVals[i] = row[ord]
			}
			return mkUniqueCheckErr(md, c, keyVals)
		}
		node, err := b.factory.ConstructErrorIfRows(query.root, mkErr)
		if err != nil {
			return err
		}
		b.checks = append(b.checks, node)
	}
	return nil
}

func (b *Builder) buildFKChecks(checks memo.FKChecksExpr) error {
	defer b.checkContainsLocking(b.flags.IsSet(exec.PlanFlagContainsLocking))
	b.flags.Unset(exec.PlanFlagContainsLocking)
	md := b.mem.Metadata()
	for i := range checks {
		c := &checks[i]
		// Construct the query that returns FK violations.
		query, queryCols, err := b.buildRelational(c.Check)
		if err != nil {
			return err
		}
		// Wrap the query in an error node.
		mkErr := func(row tree.Datums) error {
			keyVals := make(tree.Datums, len(c.KeyCols))
			for i, col := range c.KeyCols {
				ord, err := getNodeColumnOrdinal(queryCols, col)
				if err != nil {
					return err
				}
				keyVals[i] = row[ord]
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

// mkUniqueCheckErr generates a user-friendly error describing a uniqueness
// violation. The keyVals are the values that correspond to the
// cat.UniqueConstraint columns.
func mkUniqueCheckErr(md *opt.Metadata, c *memo.UniqueChecksItem, keyVals tree.Datums) error {
	tabMeta := md.TableMeta(c.Table)
	uc := tabMeta.Table.Unique(c.CheckOrdinal)
	constraintName := uc.Name()
	var msg, details bytes.Buffer

	// Generate an error of the form:
	//   ERROR:  duplicate key value violates unique constraint "foo"
	//   DETAIL: Key (k)=(2) already exists.
	msg.WriteString("duplicate key value violates unique constraint ")
	lexbase.EncodeEscapedSQLIdent(&msg, constraintName)

	details.WriteString("Key (")
	for i := 0; i < uc.ColumnCount(); i++ {
		if i > 0 {
			details.WriteString(", ")
		}
		col := tabMeta.Table.Column(uc.ColumnOrdinal(tabMeta.Table, i))
		details.WriteString(string(col.ColName()))
	}
	details.WriteString(")=(")
	for i, d := range keyVals {
		if i > 0 {
			details.WriteString(", ")
		}
		details.WriteString(d.String())
	}

	details.WriteString(") already exists.")

	return errors.WithDetail(
		pgerror.WithConstraintName(
			pgerror.Newf(pgcode.UniqueViolation, "%s", msg.String()),
			constraintName,
		),
		details.String(),
	)
}

// mkUniqueCheckErrWithoutColNames is a simpler version of mkUniqueCheckErr that
// omits column names from the error details.
func mkUniqueCheckErrWithoutColNames(
	md *opt.Metadata, c *memo.UniqueChecksItem, keyVals tree.Datums,
) error {
	tabMeta := md.TableMeta(c.Table)
	uc := tabMeta.Table.Unique(c.CheckOrdinal)
	constraintName := uc.Name()
	var msg, details bytes.Buffer

	// Generate an error of the form:
	//   ERROR:  duplicate key value violates unique constraint "foo"
	//   DETAIL: Key (2) already exists.
	msg.WriteString("duplicate key value violates unique constraint ")
	lexbase.EncodeEscapedSQLIdent(&msg, constraintName)

	details.WriteString("Key (")
	for i, d := range keyVals {
		if i > 0 {
			details.WriteString(", ")
		}
		details.WriteString(d.String())
	}

	details.WriteString(") already exists.")

	return errors.WithDetail(
		pgerror.WithConstraintName(
			pgerror.Newf(pgcode.UniqueViolation, "%s", msg.String()),
			constraintName,
		),
		details.String(),
	)
}

// mkFastPathUniqueCheckErr is a wrapper for mkUniqueCheckErr in the insert fast
// path flow, which reorders the keyVals row according to the ordering of the
// key columns in index `idx`. This is needed because mkUniqueCheckErr assumes
// the ordering of columns in `keyVals` matches the ordering of columns in
// `cat.UniqueConstraint.ColumnOrdinal(tabMeta.Table, i)`.
func mkFastPathUniqueCheckErr(
	md *opt.Metadata, c *memo.UniqueChecksItem, keyVals tree.Datums, idx cat.Index,
) error {

	tabMeta := md.TableMeta(c.Table)
	uc := tabMeta.Table.Unique(c.CheckOrdinal)

	newKeyVals := make(tree.Datums, 0, uc.ColumnCount())

	for i := 0; i < uc.ColumnCount(); i++ {
		ord := uc.ColumnOrdinal(tabMeta.Table, i)
		found := false
		for j := 0; j < idx.KeyColumnCount() && j < len(keyVals); j++ {
			keyCol := idx.Column(j)
			keyColOrd := keyCol.Column.Ordinal()
			if ord == keyColOrd {
				newKeyVals = append(newKeyVals, keyVals[j])
				found = true
				break
			}
		}
		if !found {
			// The unique constraint columns could not be matched to the index
			// key columns. This can happen when the index columns are computed
			// columns that map to the unique constraint columns (see #126988).
			// When this happens, produce a simpler error message.
			return mkUniqueCheckErrWithoutColNames(md, c, keyVals)
		}
	}
	return mkUniqueCheckErr(md, c, newKeyVals)
}

// mkFKCheckErr generates a user-friendly error describing a foreign key
// violation. The keyVals are the values that correspond to the
// cat.ForeignKeyConstraint columns.
func mkFKCheckErr(md *opt.Metadata, c *memo.FKChecksItem, keyVals tree.Datums) error {
	origin := md.TableMeta(c.OriginTable)
	referenced := md.TableMeta(c.ReferencedTable)

	var msg, details bytes.Buffer
	var constraintName string
	if c.FKOutbound {
		// Generate an error of the form:
		//   ERROR:  insert on table "child" violates foreign key constraint "foo"
		//   DETAIL: Key (child_p)=(2) is not present in table "parent".
		fk := origin.Table.OutboundForeignKey(c.FKOrdinal)
		constraintName = fk.Name()
		fmt.Fprintf(&msg, "%s on table ", c.OpName)
		lexbase.EncodeEscapedSQLIdent(&msg, string(origin.Alias.ObjectName))
		msg.WriteString(" violates foreign key constraint ")
		lexbase.EncodeEscapedSQLIdent(&msg, fk.Name())

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
			lexbase.EncodeEscapedSQLIdent(&details, string(referenced.Alias.ObjectName))
			details.WriteByte('.')
		}
	} else {
		// Generate an error of the form:
		//   ERROR:  delete on table "parent" violates foreign key constraint
		//           "child_child_p_fkey" on table "child"
		//   DETAIL: Key (p)=(1) is still referenced from table "child".
		fk := referenced.Table.InboundForeignKey(c.FKOrdinal)
		constraintName = fk.Name()
		fmt.Fprintf(&msg, "%s on table ", c.OpName)
		lexbase.EncodeEscapedSQLIdent(&msg, string(referenced.Alias.ObjectName))
		msg.WriteString(" violates foreign key constraint ")
		lexbase.EncodeEscapedSQLIdent(&msg, fk.Name())
		msg.WriteString(" on table ")
		lexbase.EncodeEscapedSQLIdent(&msg, string(origin.Alias.ObjectName))

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
		lexbase.EncodeEscapedSQLIdent(&details, string(origin.Alias.ObjectName))
		details.WriteByte('.')
	}

	return errors.WithDetail(
		pgerror.WithConstraintName(
			pgerror.Newf(pgcode.ForeignKeyViolation, "%s", msg.String()),
			constraintName,
		),
		details.String(),
	)
}

func (b *Builder) buildFKCascades(withID opt.WithID, cascades memo.FKCascades) error {
	if len(cascades) == 0 {
		return nil
	}
	cb, err := makePostQueryBuilder(b, withID)
	if err != nil {
		return err
	}
	for i := range cascades {
		b.cascades = append(b.cascades, cb.setupCascade(&cascades[i]))
	}
	return nil
}

func (b *Builder) buildAfterTriggers(withID opt.WithID, triggers *memo.AfterTriggers) error {
	if triggers == nil {
		return nil
	}
	tb, err := makePostQueryBuilder(b, withID)
	if err != nil {
		return err
	}
	b.triggers = append(b.triggers, tb.setupTriggers(triggers))
	return nil
}

// forUpdateLocking is the row-level locking mode implicitly used by mutations
// during their initial row scan, when such locking is deemed desirable. The
// locking mode is equivalent to that used by a SELECT FOR UPDATE statement,
// except not durable.
var forUpdateLocking = opt.Locking{
	Strength:   tree.ForUpdate,
	WaitPolicy: tree.LockWaitBlock,
	Durability: tree.LockDurabilityBestEffort,
}

// shouldApplyImplicitLockingToMutationInput determines whether or not the
// builder should apply a FOR UPDATE row-level locking mode to the initial row
// scan of a mutation expression. If the builder should lock the initial row
// scan, it returns the TableID of the scan, otherwise it returns 0.
func (b *Builder) shouldApplyImplicitLockingToMutationInput(
	mutExpr memo.RelExpr,
) (opt.TableID, error) {
	if !b.evalCtx.SessionData().ImplicitSelectForUpdate {
		return 0, nil
	}

	switch t := mutExpr.(type) {
	case *memo.InsertExpr:
		// Unlike with the other three mutation expressions, it never makes
		// sense to apply implicit row-level locking to the input of an INSERT
		// expression because any contention results in unique constraint
		// violations.
		return 0, nil

	case *memo.UpdateExpr:
		md := b.mem.Metadata()
		return shouldApplyImplicitLockingToUpdateOrDeleteInput(md, t.Input, t.Table), nil

	case *memo.UpsertExpr:
		return shouldApplyImplicitLockingToUpsertInput(t), nil

	case *memo.DeleteExpr:
		md := b.mem.Metadata()
		return shouldApplyImplicitLockingToUpdateOrDeleteInput(md, t.Input, t.Table), nil

	default:
		return 0, errors.AssertionFailedf("unexpected mutation expression %T", t)
	}
}

// shouldApplyImplicitLockingToUpdateOrDeleteInput determines whether the
// builder should apply a FOR UPDATE row-level locking mode to the initial row
// scan of an UPDATE statement or a DELETE. If the builder should lock the
// initial row scan, it returns the TableID of the scan, otherwise it returns 0.
//
// Conceptually, if we picture an UPDATE statement as the composition of a
// SELECT statement and an INSERT statement (with loosened semantics around
// existing rows) then this method determines whether the builder should perform
// the following transformation:
//
//	UPDATE t = SELECT FROM t + INSERT INTO t
//	=>
//	UPDATE t = SELECT FROM t FOR UPDATE + INSERT INTO t
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
//
// UPDATEs and DELETEs happen to have exactly the same matching pattern, so we
// reuse this function for both.
func shouldApplyImplicitLockingToUpdateOrDeleteInput(
	md *opt.Metadata, input memo.RelExpr, tabID opt.TableID,
) opt.TableID {
	// Try to match the mutation's input expression against the pattern:
	//
	//   [Project]* [IndexJoin] (Scan | LookupJoin [LookupJoin] Values)
	//
	// The IndexJoin will only be present if the base expression is a Scan, but
	// making it an optional prefix to the LookupJoins makes the logic simpler.
	input = unwrapProjectExprs(input)
	if idxJoin, ok := input.(*memo.IndexJoinExpr); ok {
		input = idxJoin.Input
	}
	switch t := input.(type) {
	case *memo.ScanExpr:
		return t.Table
	case *memo.LookupJoinExpr:
		if innerJoin, ok := t.Input.(*memo.LookupJoinExpr); ok && innerJoin.Table == t.Table {
			t = innerJoin
		}
		mutStableID := md.Table(tabID).ID()
		lookupStableID := md.Table(t.Table).ID()
		// Only lock rows read in the lookup join if the lookup table is the
		// same as the table being updated. Also, don't lock rows if there is an
		// ON condition so that we don't lock rows that won't be updated.
		if mutStableID == lookupStableID && t.On.IsTrue() && t.Input.Op() == opt.ValuesOp {
			return t.Table
		}
	}
	return 0
}

// tryApplyImplicitLockingToUpsertInput determines whether or not the builder
// should apply a FOR UPDATE row-level locking mode to the initial row scan of
// an UPSERT statement. If the builder should lock the initial row scan, it
// returns the TableID of the scan, otherwise it returns 0.
func shouldApplyImplicitLockingToUpsertInput(ups *memo.UpsertExpr) opt.TableID {
	// Try to match the Upsert's input expression against the pattern:
	//
	//   [Project]* (LeftJoin Scan | LookupJoin [LookupJoin]) [Project]* Values
	//
	input := ups.Input
	input = unwrapProjectExprs(input)
	var toLock opt.TableID
	switch join := input.(type) {
	case *memo.LeftJoinExpr:
		scan, ok := join.Right.(*memo.ScanExpr)
		if !ok {
			return 0
		}
		input = join.Left
		toLock = scan.Table

	case *memo.LookupJoinExpr:
		input = join.Input
		if inner, ok := input.(*memo.LookupJoinExpr); ok && inner.Table == join.Table {
			// When a generic query plan reads from a secondary index first,
			// then performs a lookup into the primary index, the plan has a
			// double lookup join pattern. We add implicit locks in this case
			// where both lookup joins have the same table.
			input = inner.Input
		}
		toLock = join.Table

	default:
		return 0
	}
	input = unwrapProjectExprs(input)
	if _, ok := input.(*memo.ValuesExpr); ok {
		return toLock
	}
	return 0
}

// unwrapProjectExprs unwraps zero or more nested ProjectExprs. It returns the
// first non-ProjectExpr in the chain, or the input if it is not a ProjectExpr.
func unwrapProjectExprs(input memo.RelExpr) memo.RelExpr {
	if proj, ok := input.(*memo.ProjectExpr); ok {
		return unwrapProjectExprs(proj.Input)
	}
	return input
}

func (b *Builder) buildLock(lock *memo.LockExpr) (_ execPlan, outputCols colOrdMap, err error) {
	// Don't bother creating the lookup join if we don't need it.
	locking, err := b.buildLocking(lock.Table, lock.Locking)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	if locking.IsNoOp() {
		return b.buildRelational(lock.Input)
	}

	// In some simple cases we can push the locking down into the input operation
	// instead of adding what would be a redundant lookup join. We only apply
	// these optimizations for single-column-family tables.
	//
	// TODO(michae2): To optimize more complex cases, such as a Project on top of
	// a Scan, or multiple Lock ops, etc, we need to do something similar to
	// ordering. That is, make locking a physical property required by Lock, and
	// make various operators provide the physical property, with a
	// locking-semi-LookupJoin as the enforcer of last resort.
	tab := b.mem.Metadata().Table(lock.Table)
	if tab.FamilyCount() == 1 {
		switch input := lock.Input.(type) {
		case *memo.ScanExpr:
			if input.Table == lock.KeySource && input.Index == cat.PrimaryIndex {
				// Make a shallow copy of the scan to avoid mutating the original.
				scan := *input
				scan.Locking = scan.Locking.Max(locking)
				return b.buildRelational(&scan)
			}
		case *memo.IndexJoinExpr:
			if input.Table == lock.KeySource {
				// Make a shallow copy of the join to avoid mutating the original.
				join := *input
				join.Locking = join.Locking.Max(locking)
				return b.buildRelational(&join)
			}
		case *memo.LookupJoinExpr:
			if input.Table == lock.KeySource && input.Index == cat.PrimaryIndex &&
				(input.JoinType == opt.InnerJoinOp || input.JoinType == opt.SemiJoinOp) &&
				!input.IsFirstJoinInPairedJoiner && !input.IsSecondJoinInPairedJoiner &&
				// We con't push the locking down if the lookup join has additional on
				// predicates that will filter out rows after the join.
				len(input.On) == 0 {
				// Make a shallow copy of the join to avoid mutating the original.
				join := *input
				join.Locking = join.Locking.Max(locking)
				return b.buildRelational(&join)
			}
		}
	}

	// Perform the locking using a semi-lookup-join to the primary index.
	join := &memo.LookupJoinExpr{
		Input: lock.Input,
		LookupJoinPrivate: memo.LookupJoinPrivate{
			JoinType:              opt.SemiJoinOp,
			Table:                 lock.Table,
			Index:                 cat.PrimaryIndex,
			KeyCols:               lock.KeyCols,
			Cols:                  lock.LockCols.Union(lock.Cols),
			LookupColsAreTableKey: true,
			Locking:               locking,
		},
	}
	join.CopyGroup(lock)
	return b.buildLookupJoin(join)
}

func (b *Builder) setMutationFlags(e memo.RelExpr) {
	b.flags.Set(exec.PlanFlagContainsMutation)
	switch e.Op() {
	case opt.DeleteOp:
		b.flags.Set(exec.PlanFlagContainsDelete)
	case opt.InsertOp:
		b.flags.Set(exec.PlanFlagContainsInsert)
	case opt.UpdateOp:
		b.flags.Set(exec.PlanFlagContainsUpdate)
	case opt.UpsertOp:
		b.flags.Set(exec.PlanFlagContainsUpsert)
	}
}
