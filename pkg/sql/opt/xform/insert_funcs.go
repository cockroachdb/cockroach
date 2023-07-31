// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package xform contains logic for transforming SQL queries.
package xform

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// CanUseUniqueChecksForInsertFastPath analyzes the `uniqueChecks` in an Insert
// of values and determines if they allow the insert to be executed in fast path
// mode, which performs the uniqueness checks in a batched KV operation. If
// insert fast path is legal, `InsertFastPathFKUniqCheck` in the individual
// UniqueChecksItems' private sections of `newUniqueChecks` is generated with
// information needed to build fast path insert in the execbuilder, and passed to
// the caller for constructing a new InsertExpr. The input to the Insert may be
// rewritten to merge a projection with the values expression, to enable fast
// path in more cases, and returned to the caller through `insInput`.
// Note that the execbuilder may still choose to not pick insert fast path if
// the statement no longer qualifies, for example if autocommit can't be done
// or if the session explicitly disables insert fast path.
func (c *CustomFuncs) CanUseUniqueChecksForInsertFastPath(
	ins *memo.InsertExpr, uniqueChecks memo.UniqueChecksExpr,
) (newUniqueChecks memo.UniqueChecksExpr, insInput memo.RelExpr, ok bool) {

	if len(uniqueChecks) == 0 {
		return memo.UniqueChecksExpr{}, nil, false
	}
	if uniqueChecks[0].FastPathCheck != nil {
		// Fast path checks have already been built.
		return memo.UniqueChecksExpr{}, nil, false
	}
	if c.e.evalCtx == nil {
		return memo.UniqueChecksExpr{}, nil, false
	}
	planner := c.e.evalCtx.Planner
	if planner == nil {
		// The planner can actually be nil in some tests. Avoid errors trying to
		// access a nil planner in different functions called below.
		return memo.UniqueChecksExpr{}, nil, false
	}
	canAutoCommit := c.canAutoCommit(ins)
	if !canAutoCommit {
		// Don't bother building fast path structures for a statement which won't
		// qualify for fast path in the execbuilder.
		return memo.UniqueChecksExpr{}, nil, false
	}

	sd := c.e.evalCtx.SessionData()
	if !sd.InsertFastPath {
		// Insert fast path is explicitly disabled. Skip the work of building
		// structures for it.
		return memo.UniqueChecksExpr{}, nil, false
	}

	// See comments in execbuilder.New() for why autocommit might be prohibited.
	// Insert fast path relies on using autocommit.
	prohibitAutoCommit := sd.TxnRowsReadErr != 0 && !sd.Internal
	if prohibitAutoCommit {
		// Don't bother building fast path structures for a statement which won't
		// qualify for fast path in the execbuilder.
		return memo.UniqueChecksExpr{}, nil, false
	}

	insInput = ins.Input
	var projectExpr *memo.ProjectExpr
	// Allow fast-path to merge the projection with the Values expression if all
	// input columns are passed through and no projections reference columns.
	// Likely expressions are constants or gen_random_uuid function calls.
	if projectExpr, ok = insInput.(*memo.ProjectExpr); ok {
		insInput = projectExpr.Input
		if _, ok = insInput.(*memo.ValuesExpr); !ok {
			return memo.UniqueChecksExpr{}, nil, false
		}
		if !projectExpr.Passthrough.Equals(insInput.Relational().OutputCols) {
			return memo.UniqueChecksExpr{}, nil, false
		}
		for i := 0; i < len(projectExpr.Projections); i++ {
			var sharedProps props.Shared
			memo.BuildSharedProps(projectExpr.Projections[i].Element, &sharedProps, c.e.evalCtx)
			if !sharedProps.OuterCols.Empty() {
				return memo.UniqueChecksExpr{}, nil, false
			}
		}
	}
	values, ok := insInput.(*memo.ValuesExpr)
	// Values expressions containing subqueries or UDFs, or having a size larger
	// than the max mutation batch size are disallowed.
	if !ok || !memo.ValuesLegalForInsertFastPath(values) {
		return memo.UniqueChecksExpr{}, nil, false
	}

	md := c.e.f.Memo().Metadata()
	tab := md.Table(ins.Table)

	uniqChecks := make([]opt.InsertFastPathFKUniqCheck, len(ins.UniqueChecks))
	for i := range ins.UniqueChecks {
		uniqueChecksItem := &ins.UniqueChecks[i]
		check := uniqueChecksItem.Check

		out := &uniqChecks[i]
		out.MatchMethod = tree.MatchFull

		// appendNewUniqueChecksItem handles building of the FastPathCheck into a
		// new UniqueChecksItem for the new InsertExpr being constructed.
		appendNewUniqueChecksItem := func() {
			newUniqueChecksItem := c.e.f.ConstructUniqueChecksItem(
				uniqueChecksItem.Check,
				&memo.UniqueChecksItemPrivate{
					Table:         uniqueChecksItem.Table,
					CheckOrdinal:  uniqueChecksItem.CheckOrdinal,
					KeyCols:       uniqueChecksItem.KeyCols,
					OpName:        uniqueChecksItem.OpName,
					FastPathCheck: out,
				},
			)
			newUniqueChecks = append(newUniqueChecks, newUniqueChecksItem)
		}

		// Skip over distribute and projection operations.
		if distributeExpr, isDistribute := check.(*memo.DistributeExpr); isDistribute {
			check = distributeExpr.Input
		}
		// We don't need the result of the check expression, so skip to the input of
		// any projections so we can inspect the scan or lookup join expressions
		// underneath.
		var skipProjectExpr *memo.ProjectExpr
		for skipProjectExpr, ok = check.(*memo.ProjectExpr); ok; skipProjectExpr, ok = check.(*memo.ProjectExpr) {
			check = skipProjectExpr.Input
		}
		if c.handleSingleRowInsert(ins, check, out, uniqueChecksItem) {
			appendNewUniqueChecksItem()
			continue
		}
		// The key values are found by analyzing a lookup join expression, so see
		// if we can find one.
		check = check.FirstExpr()
		var expr memo.RelExpr
		for check != nil {
			expr = check
			// We don't need the result of the check expression, so skip to the input
			// of any projections so we can inspect the lookup join expression
			// underneath.
			for skipProjectExpr, ok = expr.(*memo.ProjectExpr); ok; skipProjectExpr, ok = expr.(*memo.ProjectExpr) {
				expr = skipProjectExpr.Input
			}
			if expr.Op() == opt.LookupJoinOp {
				break
			}
			check = check.NextExpr()
		}
		if check == nil || expr == nil {
			return memo.UniqueChecksExpr{}, nil, false
		}

		var lookupJoin *memo.LookupJoinExpr
		var withScan *memo.WithScanExpr
		for ; expr != nil && expr.Op() == opt.LookupJoinOp; expr = expr.NextExpr() {
			lookupJoin, _ = expr.(*memo.LookupJoinExpr)
			out.ReferencedTable = md.Table(lookupJoin.Table)
			out.ReferencedIndex = out.ReferencedTable.Index(lookupJoin.Index)

			inputExpr := lookupJoin.Input
			// Allow a select if it has ignorable filters (checking done after withScan
			// is resolved).
			var selectCondition opt.ScalarExpr
			if sel, isSelect := inputExpr.(*memo.SelectExpr); isSelect {
				inputExpr = sel.Input
				if len(sel.Filters) > 1 {
					return memo.UniqueChecksExpr{}, nil, false
				} else if len(sel.Filters) == 1 {
					selectCondition = sel.Filters[0].Condition
				}
			}
			// A WithScan typically wraps multiple rows from a VALUES clause being
			// inserted, so peek into it.
			withScan, ok = inputExpr.(*memo.WithScanExpr)
			// Verify this is the WithScan corresponding to this insert.
			if !ok || withScan.With != ins.WithID {
				continue
			}
			// Ignore a select with an ignorable condition.
			if selectCondition != nil && !c.ignorableNECheck(selectCondition, withScan, lookupJoin.Table) {
				continue
			}
			// We found a valid lookup join.
			break
		}
		if lookupJoin == nil {
			return memo.UniqueChecksExpr{}, nil, false
		}

		// A uniqueness check may be handled via a lookup join into the index.
		// We could have a lookup expression or lookup KeyCols.
		if len(lookupJoin.LookupExpr) == 0 {
			out.InsertCols = make([]opt.TableColumnOrdinal, len(lookupJoin.KeyCols))
			// Make a single DatumsFromConstraint row as there will be one uniqueness
			// KV lookup per input row.
			out.DatumsFromConstraint = make([]tree.Datums, 1)
			out.DatumsFromConstraint[0] = make(tree.Datums, tab.ColumnCount())
			// For each index key column...
			for j, keyCol := range lookupJoin.KeyCols {
				// The keyCol comes from the WithScan operator. We must find the
				// matching column in the mutation input.
				var withColOrd int
				withColOrd, ok = withScan.OutCols.Find(keyCol)
				if !ok {
					return memo.UniqueChecksExpr{}, nil, false
				}
				// Get the column ID of the matching input column.
				inputCol := withScan.InCols[withColOrd]
				var inputColOrd int
				// Find the position of that column in the input row.
				inputColOrd, ok = ins.InsertCols.Find(inputCol)
				if !ok {
					return memo.UniqueChecksExpr{}, nil, false
				}
				out.InsertCols[j] = opt.TableColumnOrdinal(inputColOrd)
			}
			out.MkErr = func(values tree.Datums) error {
				return mkFastPathUniqueCheckErr(md, uniqueChecksItem, values, out.ReferencedIndex)
			}
		} else {
			if len(lookupJoin.KeyCols) > 0 {
				return memo.UniqueChecksExpr{}, nil, false
			}
			InExprSeen := false
			// There is one insert column associated with each lookup expression.
			out.InsertCols = make([]opt.TableColumnOrdinal, len(lookupJoin.LookupExpr))
			for _, joinFilter := range lookupJoin.LookupExpr {
				var eqExpr *memo.EqExpr
				var leftVariableExpr *memo.VariableExpr
				var tupleExpr *memo.TupleExpr
				var InExpr *memo.InExpr
				var keyCol opt.ColumnID
				// Populate InsertCols and DatumsFromConstraint using the join filter
				// condition.
				if InExpr, ok = joinFilter.Condition.(*memo.InExpr); ok {
					if InExprSeen {
						return memo.UniqueChecksExpr{}, nil, false
					}
					InExprSeen = true
					if leftVariableExpr, ok = InExpr.Left.(*memo.VariableExpr); !ok {
						return memo.UniqueChecksExpr{}, nil, false
					}
					keyCol = leftVariableExpr.Col
					if tupleExpr, ok = InExpr.Right.(*memo.TupleExpr); !ok {
						return memo.UniqueChecksExpr{}, nil, false
					}
					inputColOrd, indexColOrd, foundKeyColOrd := c.findKeyColOrd(keyCol, lookupJoin.Table, lookupJoin.Index)
					if !foundKeyColOrd {
						return memo.UniqueChecksExpr{}, nil, false
					}
					// Verify the lookup expressions are on a prefix of the index key.
					if indexColOrd >= len(lookupJoin.LookupExpr) {
						return memo.UniqueChecksExpr{}, nil, false
					}
					out.InsertCols[indexColOrd] = opt.TableColumnOrdinal(inputColOrd)
					// The datums are fixed for all columns in the IN expressions, so
					// add them to DatumsFromConstraint.
					out.DatumsFromConstraint = make([]tree.Datums, len(tupleExpr.Elems))
					for k, tuple := range tupleExpr.Elems {
						var constExpr *memo.ConstExpr
						if constExpr, ok = tuple.(*memo.ConstExpr); !ok {
							return memo.UniqueChecksExpr{}, nil, false
						}
						out.DatumsFromConstraint[k] = make(tree.Datums, tab.ColumnCount())
						out.DatumsFromConstraint[k][inputColOrd] = constExpr.Value
					}
				} else if eqExpr, ok = joinFilter.Condition.(*memo.EqExpr); ok {
					// For equality, the key column is on the right, as opposed to on the
					// left for IN expressions, as we are equating a left table column and
					// right table column.
					if rightVariableExpr, rightIsVariable := eqExpr.Right.(*memo.VariableExpr); rightIsVariable {
						keyCol = rightVariableExpr.Col
					}
					if _, leftIsVariable := eqExpr.Left.(*memo.VariableExpr); !leftIsVariable {
						return memo.UniqueChecksExpr{}, nil, false
					}
					inputColOrd, indexColOrd, foundKeyColOrd := c.findKeyColOrd(keyCol, lookupJoin.Table, lookupJoin.Index)
					if !foundKeyColOrd {
						return memo.UniqueChecksExpr{}, nil, false
					}
					// Verify the lookup expressions are on a prefix of the index key.
					if indexColOrd >= len(lookupJoin.LookupExpr) {
						return memo.UniqueChecksExpr{}, nil, false
					}
					out.InsertCols[indexColOrd] = opt.TableColumnOrdinal(inputColOrd)
				} else {
					return memo.UniqueChecksExpr{}, nil, false
				}
			}
			out.MkErr = func(values tree.Datums) error {
				return mkFastPathUniqueCheckErr(md, uniqueChecksItem, values, out.ReferencedIndex)
			}
		}
		if len(lookupJoin.On) > 1 {
			return memo.UniqueChecksExpr{}, nil, false
		} else if len(lookupJoin.On) == 1 {
			if !c.ignorableNECheck(lookupJoin.On[0].Condition, withScan, lookupJoin.Table) {
				return memo.UniqueChecksExpr{}, nil, false
			}
		}

		if len(out.DatumsFromConstraint) == 0 {
			// We need at least one DatumsFromConstraint in order to perform
			// uniqueness checks during fast-path insert. Even if DatumsFromConstraint
			// contains no Datums, that case indicates that all values to check come
			// from the input row.
			return memo.UniqueChecksExpr{}, nil, false
		}
		appendNewUniqueChecksItem()
	}

	insInput = ins.Input
	if projectExpr != nil {
		// Merge the projection with the Values expression.
		insInput = c.MergeProjectWithValues(projectExpr.Projections, projectExpr.Passthrough, values)
		if values, ok = insInput.(*memo.ValuesExpr); !ok {
			return memo.UniqueChecksExpr{}, nil, false
		}
	}
	return newUniqueChecks, insInput, true
}

// handleSingleRowInsert examines a duplicate check involving cross join of the
// insert row with a scan or select from the base table in fulfillment of the
// unique constraint built for the `uniqueChecksItem`, and builds the
// corresponding `out` parameter of type `InsertFastPathFKUniqCheck` which is
// used to drive insert fast path unique constraint checks in the execution
// engine. `check` is the tree of operations from `uniqueChecksItem` with
// distribute and projection operations skipped over.
func (c *CustomFuncs) handleSingleRowInsert(
	ins *memo.InsertExpr,
	check memo.RelExpr,
	out *opt.InsertFastPathFKUniqCheck,
	uniqueChecksItem *memo.UniqueChecksItem,
) (ok bool) {
	md := c.e.f.Memo().Metadata()
	tab := md.Table(ins.Table)

	uniqueCheckKeyCols := uniqueChecksItem.UniqueChecksItemPrivate.KeyCols.ToSet()
	insertValues, insertInputIsValues := ins.Input.(*memo.ValuesExpr)
	if !insertInputIsValues {
		return false
	}
	if len(insertValues.Rows) != 1 {
		return false
	}
	insertTuple, valueRowIsTuple := insertValues.Rows[0].(*memo.TupleExpr)
	if !valueRowIsTuple {
		return false
	}

	var inputExpr, rightExpr memo.RelExpr
	var onClause memo.FiltersExpr
	var scanExpr *memo.ScanExpr
	for check = check.FirstExpr(); check != nil; check = check.NextExpr() {
		expr := check
		// We don't need the result of the check expression, so skip to the input of
		// any projections so we can inspect the scan expression underneath.
		var skipProjectExpr *memo.ProjectExpr
		for skipProjectExpr, ok = expr.(*memo.ProjectExpr); ok; skipProjectExpr, ok = expr.(*memo.ProjectExpr) {
			expr = skipProjectExpr.Input
		}
		innerJoin, isInnerJoin := expr.(*memo.InnerJoinExpr)
		if isInnerJoin {
			inputExpr = innerJoin.Left
			rightExpr = innerJoin.Right
			onClause = innerJoin.On
		} else {
			// Not an inner join.
			semiJoin, isSemiJoin := expr.(*memo.SemiJoinExpr)
			if !isSemiJoin {
				continue
			}
			inputExpr = semiJoin.Left
			rightExpr = semiJoin.Right
			onClause = semiJoin.On
		}

		// We're expecting the join filters built in mutation_builder on the unique
		// key columns:
		//    Build the join filters:
		//      (new_a = existing_a) AND (new_b = existing_b) AND ...
		//
		//    Set the capacity to h.uniqueOrdinals.Len()+1 since we'll have an equality
		//    condition for each column in the unique constraint, plus one additional
		//    condition to prevent rows from matching themselves (see below).
		// Verify that is the case.
		joinFilterIsOK := func(filtersExpr memo.FiltersExpr, scanExpr *memo.ScanExpr, withScan *memo.WithScanExpr) bool {
			if len(filtersExpr) != uniqueCheckKeyCols.Len()+1 {
				return false
			}
			for i := range filtersExpr {
				if eqExpr, isEqExpr := filtersExpr[i].Condition.(*memo.EqExpr); isEqExpr {
					leftVariable, leftIsVariable := eqExpr.Left.(*memo.VariableExpr)
					_, rightIsVariable := eqExpr.Right.(*memo.VariableExpr)
					if !leftIsVariable || !rightIsVariable {
						return false
					}
					if !uniqueCheckKeyCols.Contains(leftVariable.Col) {
						return false
					}
				} else if !c.ignorableNECheck(filtersExpr[i].Condition, withScan, scanExpr.Table) {
					return false
				}
			}
			return true
		}

		// Skip over the select (check if it's ignorable below).
		var sel *memo.SelectExpr
		var isSelect bool

		// A WithScan typically wraps multiple rows from a VALUES clause being
		// inserted, so peek into it.
		withScan, isWithScan := inputExpr.(*memo.WithScanExpr)
		// Verify this is the WithScan corresponding to this insert.
		if isWithScan {
			if withScan.With != ins.WithID {
				continue
			}
			rightTableScan, rightIsTableScan := rightExpr.(*memo.ScanExpr)
			if !rightIsTableScan {
				continue
			}
			if !joinFilterIsOK(onClause, rightTableScan, withScan) {
				return false
			}
		}

		selectFilterIsOK := func(selectExpr *memo.SelectExpr, scanExpr *memo.ScanExpr) bool {
			if len(selectExpr.Filters) != 1 {
				return false
			}
			// Allow a select if it has an ignorable filter condition.
			if !c.ignorablePKNEConstCheck(selectExpr.Filters[0].Condition, insertTuple, scanExpr.Table) {
				return false
			}
			return true
		}

		_, isValues := inputExpr.(*memo.ValuesExpr)
		if !isWithScan && !isValues {
			continue
		}

		if isSelect {
			if scan, isScan := inputExpr.(*memo.ScanExpr); isScan {
				if !selectFilterIsOK(sel, scan) {
					continue
				}
			} else {
				continue
			}
		}

		if indexJoinExpr, isIndexJoin := rightExpr.(*memo.IndexJoinExpr); isIndexJoin {
			rightExpr = indexJoinExpr.Input
		}
		// Step into any LIMIT expression. to find the scan.
		if limitExpr, isLimit := rightExpr.(*memo.LimitExpr); isLimit {
			rightExpr = limitExpr.Input
		}
		var selectExpr *memo.SelectExpr
		// Save the select expression for later analysis.
		if selectExpr, ok = rightExpr.(*memo.SelectExpr); ok {
			rightExpr = selectExpr.Input
			possibleScan := selectExpr.FirstExpr()
			for ; possibleScan != nil; possibleScan = possibleScan.NextExpr() {
				expr = possibleScan
				if indexJoinExpr, isIndexJoin := expr.(*memo.IndexJoinExpr); isIndexJoin {
					expr = indexJoinExpr.Input
				}
				if scan, isScan := expr.(*memo.ScanExpr); isScan {
					// We need a scan with a constraint for analysis.
					if scan.Constraint != nil {
						selectExpr = nil
						rightExpr = scan
						break
					}
				}
				// Or, we need a Select from a Scan with a constraint for analysis.
				if selectExpr, isSelect = expr.(*memo.SelectExpr); isSelect {
					if scan, isScan := selectExpr.Input.(*memo.ScanExpr); isScan {
						if scan.Constraint != nil {
							if !selectFilterIsOK(selectExpr, scan) {
								continue
							}
							rightExpr = scan
							break
						}
					}
				}
			}
		}

		// Verify we have a Scan in hand.
		if scanExpr, ok = rightExpr.(*memo.ScanExpr); !ok {
			continue
		}
		// A constraint is needed to grab Datums.
		if scanExpr.Constraint == nil {
			continue
		}
		if selectExpr != nil {
			// Allow a select if it has an ignorable filter condition.
			if !selectFilterIsOK(selectExpr, scanExpr) {
				continue
			}
		}
		// We made it to the end of our qualification checks and have
		// a useful scanExpr in hand.
		break
	}
	if check == nil {
		return false
	}
	out.ReferencedTable = md.Table(scanExpr.Table)
	out.ReferencedIndex = out.ReferencedTable.Index(scanExpr.Index)

	// Set up InsertCols with the index key columns.
	numKeyCols := scanExpr.Constraint.Spans.Get(0).StartKey().Length()
	out.InsertCols = make([]opt.TableColumnOrdinal, numKeyCols)
	for j := 0; j < numKeyCols; j++ {
		ord := out.ReferencedIndex.Column(j).Ordinal()
		out.InsertCols[j] = opt.TableColumnOrdinal(ord)
	}
	// The number of KV requests will match the number of spans.
	out.DatumsFromConstraint = make([]tree.Datums, scanExpr.Constraint.Spans.Count())
	for j := 0; j < scanExpr.Constraint.Spans.Count(); j++ {
		// DatumsFromConstraint is indexed by table column ordinal, so build
		// a slice which is large enough for any column.
		out.DatumsFromConstraint[j] = make(tree.Datums, tab.ColumnCount())
		span := scanExpr.Constraint.Spans.Get(j)
		// Verify there is a single key...
		if span.Prefix(scanExpr.Memo().EvalContext()) != span.StartKey().Length() {
			return false
		}
		// ... and that the span has the same number of columns as the index key.
		if span.StartKey().Length() != numKeyCols {
			return false
		}
		for k := 0; k < span.StartKey().Length(); k++ {
			// Get the key column's table column ordinal.
			ord := out.InsertCols[k]
			// Populate DatumsFromConstraint with that key column value.
			out.DatumsFromConstraint[j][ord] = span.StartKey().Value(k)
		}
	}
	out.MkErr = func(values tree.Datums) error {
		return mkFastPathUniqueCheckErr(md, uniqueChecksItem, values, out.ReferencedIndex)
	}
	// We must build at least one DatumsFromConstraint entry to claim success.
	return len(out.DatumsFromConstraint) != 0
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
		for j := 0; j < idx.ColumnCount(); j++ {
			keyCol := idx.Column(j)
			keyColOrd := keyCol.Column.Ordinal()
			if ord == keyColOrd {
				newKeyVals = append(newKeyVals, keyVals[j])
				found = true
				break
			}
		}
		if !found {
			panic(errors.AssertionFailedf(
				"insert fast path failed uniqueness check, but could not find unique columns for row, %v", keyVals))
		}
	}
	return memo.MkUniqueCheckErr(md, c, newKeyVals)
}

// findKeyColOrd examines a lookupJoin.Table for the given `keyCol` and returns
// the ordinal position of that key column from the table metadata and the
// ordinal position of that key column in the key definition from the index
// metadata.
func (c *CustomFuncs) findKeyColOrd(
	keyCol opt.ColumnID, tableID opt.TableID, indexOrd cat.IndexOrdinal,
) (tableColOrd int, indexColOrd int, ok bool) {
	md := c.e.f.Memo().Metadata()
	table := md.Table(tableID)
	tableMeta := md.TableMeta(tableID)
	idx := table.Index(indexOrd)
	for indexColOrd = 0; indexColOrd < idx.KeyColumnCount(); indexColOrd++ {
		col := idx.Column(indexColOrd)
		ord := col.Ordinal()
		colID := tableMeta.MetaID.ColumnID(ord)
		if colID == keyCol {
			return ord, indexColOrd, true
		}
	}
	return 0, 0, false
}

// ignorableNECheck checks that any predicate expressions are only of the
// form: withScan_col1 <> lookup_table_index_col1 OR withScan_col2 <>
// lookup_table_index_col2 OR ... This condition is generated in
// `buildInsertionCheck` to make sure in the non-fast path flow, that insert
// rows don't match themselves in the join. This is because the uniqueness
// check happens after the insertion in the non-fast path flow. In the fast
// path flow the uniqueness check happens before the insert, so this
// identical row check is only there to prevent a false positive duplicate
// unique value errors and doesn't need to be examined for fast path
// processing.
func (c *CustomFuncs) ignorableNECheck(
	expr opt.ScalarExpr, withScan *memo.WithScanExpr, tableID opt.TableID,
) (ok bool) {
	switch t := expr.(type) {
	case *memo.OrExpr:
		return c.ignorableNECheck(t.Left, withScan, tableID) && c.ignorableNECheck(t.Right, withScan, tableID)
	case *memo.NeExpr:
		if t.Left.Op() != opt.VariableOp || t.Right.Op() != opt.VariableOp {
			return false
		}
		// The WithScan column is built on the left.
		leftVariable := t.Left.(*memo.VariableExpr)
		_, ok = withScan.OutCols.Find(leftVariable.Col)
		if !ok {
			return false
		}
		// Verify the right column is a primary key column (index ordinal 0).
		rightVariable := t.Right.(*memo.VariableExpr)
		_, _, ok = c.findKeyColOrd(rightVariable.Col, tableID, 0 /* primary index */)
		if !ok {
			return false
		}
		return true
	default:
		return false
	}
}

// ignorablePKNEConstCheck checks that any predicate expressions are only of the
// form: PK_col1 <> const1 OR PK_col2 <> const2 OR ... This condition is
// generated for single-row inserts to make sure in the non-fast path flow, that
// insert rows don't match themselves in the join. This is because the
// uniqueness check happens after the insertion in the non-fast path flow. In
// the fast path flow the uniqueness check happens before the insert, so this
// identical row check is only there to prevent a false positive duplicate
// unique value errors and doesn't need to be examined for fast path processing.
// The constants in the predicates are compared with the matching constant
// in the insert tuple for raw equality to verify that the constant was derived
// from the insert row.
func (c *CustomFuncs) ignorablePKNEConstCheck(
	expr opt.ScalarExpr, insertTuple *memo.TupleExpr, tableID opt.TableID,
) (ok bool) {
	switch t := expr.(type) {
	case *memo.OrExpr:
		return c.ignorablePKNEConstCheck(t.Left, insertTuple, tableID) && c.ignorablePKNEConstCheck(t.Right, insertTuple, tableID)
	case *memo.NeExpr:
		if t.Left.Op() != opt.VariableOp || t.Right.Op() != opt.ConstOp {
			return false
		}
		constFromPredicate := t.Right.(*memo.ConstExpr)

		// Verify the left column is a primary key column (index ordinal 0).
		leftVariable := t.Left.(*memo.VariableExpr)
		ord, _, ok := c.findKeyColOrd(leftVariable.Col, tableID, 0 /* primary index */)
		if !ok {
			return false
		}
		if ord >= len(insertTuple.Elems) {
			return false
		}
		constExprFromInsertRow, ok := insertTuple.Elems[ord].(*memo.ConstExpr)
		if !ok {
			return false
		}
		if constExprFromInsertRow != constFromPredicate {
			return false
		}
		return true
	default:
		return false
	}
}

// canAutoCommit determines if it is safe to auto commit the mutation contained
// in the expression.
//
// Mutations can commit the transaction as part of the same KV request,
// potentially taking advantage of the 1PC optimization. This is not ok to do in
// general; a sufficient set of conditions is:
//  1. There is a single mutation in the query.
//  2. The mutation is the root operator, or it is directly under a Project
//     with no side-effecting expressions. An example of why we can't allow
//     side-effecting expressions: if the projection encounters a
//     division-by-zero error, the mutation shouldn't have been committed.
//
// An extra condition relates to how the FK checks are run. If they run before
// the mutation (via the insert fast path), auto commit is possible. If they run
// after the mutation (the general path), auto commit is not possible. It is up
// to the builder logic for each mutation to handle this.
//
// Note that there are other necessary conditions related to execution
// (specifically, that the transaction is implicit); it is up to the exec
// factory to take that into account as well.
func (c *CustomFuncs) canAutoCommit(rel memo.RelExpr) bool {
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
		proj := rel.(*memo.ProjectExpr)
		for i := 0; i < len(proj.Projections); i++ {
			if !proj.Projections[i].ScalarProps().VolatilitySet.IsLeakproof() {
				return false
			}
		}
		return c.canAutoCommit(proj.Input)

	case opt.DistributeOp:
		// Distribute is currently a no-op, so check whether the input can
		// auto-commit.
		return c.canAutoCommit(rel.(*memo.DistributeExpr).Input)

	default:
		return false
	}
}
