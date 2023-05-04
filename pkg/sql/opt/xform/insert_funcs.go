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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// CanUseUniqueChecksForInsertFastPath analyzes the `uniqueChecks` in an Insert
// of values and determines if they allow the insert to be executed in fast path
// mode, which performs the uniqueness checks in a batched KV operation. If
// insert fast path is legal, `InsertFastPathFKUniqCheck` in the individual
// UniqueChecksItems' private sections of `newUniqueChecks` is generated with
// information needed to build fast path insert in the execbuilder, and passed
// to the caller for constructing a new InsertExpr. The input to the Insert may
// be rewritten to merge a projection with the values expression, to enable fast
// path in more cases, and returned to the caller through `insInput`. Note that
// the execbuilder may still choose to not pick insert fast path if the
// statement no longer qualifies, for example if autocommit can't be done or if
// the session explicitly disables insert fast path.
func (c *CustomFuncs) CanUseUniqueChecksForInsertFastPath(
	ins *memo.InsertExpr,
) (newFastPathUniqueChecks memo.FastPathUniqueChecksExpr, insInput memo.RelExpr, ok bool) {

	fastPathUniqueChecks := ins.FastPathUniqueChecks
	if len(fastPathUniqueChecks) == 0 {
		return memo.FastPathUniqueChecksExpr{}, nil, false
	}
	if fastPathUniqueChecks[0].FastPathCheck != nil {
		// Fast path checks have already been built.
		return memo.FastPathUniqueChecksExpr{}, nil, false
	}
	if c.e.evalCtx == nil {
		return memo.FastPathUniqueChecksExpr{}, nil, false
	}
	planner := c.e.evalCtx.Planner
	if planner == nil {
		// The planner can actually be nil in some tests. Avoid errors trying to
		// access a nil planner in different functions called below.
		return memo.FastPathUniqueChecksExpr{}, nil, false
	}
	canAutoCommit := c.canAutoCommit(ins)
	if !canAutoCommit {
		// Don't bother building fast path structures for a statement which won't
		// qualify for fast path in the execbuilder.
		return memo.FastPathUniqueChecksExpr{}, nil, false
	}

	sd := c.e.evalCtx.SessionData()
	if !sd.InsertFastPath {
		// Insert fast path is explicitly disabled. Skip the work of building
		// structures for it.
		return memo.FastPathUniqueChecksExpr{}, nil, false
	}

	// See comments in execbuilder.New() for why autocommit might be prohibited.
	// Insert fast path relies on using autocommit.
	prohibitAutoCommit := sd.TxnRowsReadErr != 0 && !sd.Internal
	if prohibitAutoCommit {
		// Don't bother building fast path structures for a statement which won't
		// qualify for fast path in the execbuilder.
		return memo.FastPathUniqueChecksExpr{}, nil, false
	}

	insInput = ins.Input
	var projectExpr *memo.ProjectExpr
	// Allow fast-path to merge the projection with the Values expression if all
	// input columns are passed through and no projections reference columns.
	// Likely expressions are constants or gen_random_uuid function calls.
	if projectExpr, ok = insInput.(*memo.ProjectExpr); ok {
		insInput = projectExpr.Input
		if _, ok = insInput.(*memo.ValuesExpr); !ok {
			return memo.FastPathUniqueChecksExpr{}, nil, false
		}
		if !projectExpr.Passthrough.Equals(insInput.Relational().OutputCols) {
			return memo.FastPathUniqueChecksExpr{}, nil, false
		}
		for i := 0; i < len(projectExpr.Projections); i++ {
			var sharedProps props.Shared
			memo.BuildSharedProps(projectExpr.Projections[i].Element, &sharedProps, c.e.evalCtx)
			if !sharedProps.OuterCols.Empty() {
				return memo.FastPathUniqueChecksExpr{}, nil, false
			}
		}
	}
	values, ok := insInput.(*memo.ValuesExpr)
	// Values expressions containing subqueries or UDFs, or having a size larger
	// than the max mutation batch size are disallowed.
	if !ok || !memo.ValuesLegalForInsertFastPath(values) {
		return memo.FastPathUniqueChecksExpr{}, nil, false
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
		return memo.FastPathUniqueChecksExpr{}, nil, false
	}

	insInput = ins.Input
	if projectExpr != nil {
		// Merge the projection with the Values expression.
		insInput = c.MergeProjectWithValues(projectExpr.Projections, projectExpr.Passthrough, values)
		if _, ok = insInput.(*memo.ValuesExpr); !ok {
			return memo.FastPathUniqueChecksExpr{}, nil, false
		}
	}
	return newFastPathUniqueChecks, insInput, true
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
