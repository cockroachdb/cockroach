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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// CanUseUniqueChecksForInsertFastPath analyzes the `uniqueChecks` in an Insert
// of values and determines if they allow the insert to be executed in fast path
// mode, which performs the uniqueness checks in a batched KV operation. If
// insert fast path is legal, `InsertFastPathFKUniqCheck` in the private section
// of each `FastPathUniqueChecksItem` in `FastPathUniqueChecks`  is generated
// with information needed to build fast path insert in the execbuilder, and
// returned to the caller via `newFastPathUniqueChecks` for constructing a new
// InsertExpr. Note that the execbuilder may still choose to not pick insert
// fast path if the statement no longer qualifies, for example if autocommit
// can't be done or if the session explicitly disables insert fast path.
func (c *CustomFuncs) CanUseUniqueChecksForInsertFastPath(
	ins *memo.InsertExpr,
) (newFastPathUniqueChecks memo.FastPathUniqueChecksExpr, ok bool) {

	fastPathUniqueChecks := ins.FastPathUniqueChecks
	if len(fastPathUniqueChecks) == 0 {
		return memo.FastPathUniqueChecksExpr{}, false
	}
	if fastPathUniqueChecks[0].FastPathCheck != nil {
		// Fast path checks have already been built.
		return memo.FastPathUniqueChecksExpr{}, false
	}
	if c.e.evalCtx == nil {
		return memo.FastPathUniqueChecksExpr{}, false
	}
	planner := c.e.evalCtx.Planner
	if planner == nil {
		// The planner can actually be nil in some tests. Avoid errors trying to
		// access a nil planner in different functions called below.
		return memo.FastPathUniqueChecksExpr{}, false
	}
	canAutoCommit := memo.CanAutoCommit(ins)
	if !canAutoCommit {
		// Don't bother building fast path structures for a statement which won't
		// qualify for fast path in the execbuilder.
		return memo.FastPathUniqueChecksExpr{}, false
	}

	sd := c.e.evalCtx.SessionData()
	if !sd.InsertFastPath {
		// Insert fast path is explicitly disabled. Skip the work of building
		// structures for it.
		return memo.FastPathUniqueChecksExpr{}, false
	}

	// See comments in execbuilder.New() for why autocommit might be prohibited.
	// Insert fast path relies on using autocommit.
	prohibitAutoCommit := sd.TxnRowsReadErr != 0 && !sd.Internal
	if prohibitAutoCommit {
		// Don't bother building fast path structures for a statement which won't
		// qualify for fast path in the execbuilder.
		return memo.FastPathUniqueChecksExpr{}, false
	}

	insInput := ins.Input
	values, ok := insInput.(*memo.ValuesExpr)
	// Values expressions containing subqueries or UDFs, or having a size larger
	// than the max mutation batch size are disallowed.
	if !ok || !memo.ValuesLegalForInsertFastPath(values) {
		return memo.FastPathUniqueChecksExpr{}, false
	}
	uniqChecks := make([]opt.InsertFastPathFKUniqCheck, len(ins.FastPathUniqueChecks))
	for i := range ins.FastPathUniqueChecks {
		fastPathUniqueChecksItem := &ins.FastPathUniqueChecks[i]
		check := fastPathUniqueChecksItem.Check
		out := &uniqChecks[i]

		// appendNewUniqueChecksItem handles building of the FastPathCheck into a
		// new UniqueChecksItem for the new InsertExpr being constructed.
		appendNewUniqueChecksItem := func() {
			newFastPathUniqueChecksItem := c.e.f.ConstructFastPathUniqueChecksItem(
				fastPathUniqueChecksItem.Check,
				&memo.FastPathUniqueChecksItemPrivate{
					FastPathCheck: out,
				},
			)
			newFastPathUniqueChecks = append(newFastPathUniqueChecks, newFastPathUniqueChecksItem)
		}

		// Skip over distribute and projection operations.
		if distributeExpr, isDistribute := check.(*memo.DistributeExpr); isDistribute {
			check = distributeExpr.Input
		}
		if c.handleSingleRowInsert(ins, check, out) {
			appendNewUniqueChecksItem()
			continue
		}
		return memo.FastPathUniqueChecksExpr{}, false
	}
	return newFastPathUniqueChecks, true
}

// handleSingleRowInsert examines an insert fast path index `check` relation
// which was specially constructed to find a constrained index scan which fully
// consumes all filters needed to perform the uniqueness check in
// `uniqueChecksItem`, and builds the corresponding `out` parameter of type
// `InsertFastPathFKUniqCheck`. This struct is used to drive insert fast path
// unique constraint checks in the execution engine.
func (c *CustomFuncs) handleSingleRowInsert(
	ins *memo.InsertExpr, check memo.RelExpr, out *opt.InsertFastPathFKUniqCheck,
) (ok bool) {
	md := c.e.f.Memo().Metadata()
	tab := md.Table(ins.Table)

	var scanExpr *memo.ScanExpr
	for check = check.FirstExpr(); check != nil; check = check.NextExpr() {
		expr := check

		if indexJoinExpr, isIndexJoin := expr.(*memo.IndexJoinExpr); isIndexJoin {
			expr = indexJoinExpr.Input
		}
		if scan, isScan := expr.(*memo.ScanExpr); isScan {
			// We need a scan with a constraint for analysis.
			if scan.Constraint != nil {
				scanExpr = scan
				break
			}
		}
	}
	if check == nil {
		return false
	}
	referencedTable := md.Table(scanExpr.Table)
	referencedIndex := referencedTable.Index(scanExpr.Index)
	out.ReferencedTableID = scanExpr.Table
	out.ReferencedIndexOrdinal = scanExpr.Index

	// Set up InsertCols with the index key columns.
	numKeyCols := scanExpr.Constraint.Spans.Get(0).StartKey().Length()
	out.InsertCols = make(opt.ColList, numKeyCols)
	columnOrdinals := make([]int, numKeyCols)
	for j := 0; j < numKeyCols; j++ {
		ord := referencedIndex.Column(j).Ordinal()
		columnOrdinals[j] = ord
		out.InsertCols[j] = out.ReferencedTableID.ColumnID(ord)
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
			ord := columnOrdinals[k]
			// Populate DatumsFromConstraint with that key column value.
			out.DatumsFromConstraint[j][ord] = span.StartKey().Value(k)
		}
	}
	// We must build at least one DatumsFromConstraint entry to claim success.
	return len(out.DatumsFromConstraint) != 0
}
