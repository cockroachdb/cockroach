// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package xform contains logic for transforming SQL queries.
package xform

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// CanUseUniqueChecksForInsertFastPath analyzes the `uniqueChecks` in an Insert
// of values and determines if they allow the insert to be executed in fast path
// mode, which performs the uniqueness checks in a batched KV operation. If
// insert fast path is legal, `DatumsFromConstraint` and other items in the
// private section of each `FastPathUniqueChecksItem` in `FastPathUniqueChecks`
// is constructed with information needed to build fast path insert in the
// execbuilder, and returned to the caller via `newFastPathUniqueChecks` for
// constructing a new InsertExpr. Note that the execbuilder may still choose not
// to pick insert fast path if the statement no longer qualifies, for example if
// autocommit can't be done or if the session explicitly disables insert fast
// path.
func (c *CustomFuncs) CanUseUniqueChecksForInsertFastPath(
	ins *memo.InsertExpr,
) (newFastPathUniqueChecks memo.FastPathUniqueChecksExpr, ok bool) {

	fastPathUniqueChecks := ins.FastPathUniqueChecks
	if len(fastPathUniqueChecks) == 0 {
		return memo.FastPathUniqueChecksExpr{}, false
	}
	if len(fastPathUniqueChecks[0].DatumsFromConstraint) != 0 {
		// Fast path checks have already been built.
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
	for i := range ins.FastPathUniqueChecks {
		fastPathUniqueChecksItem := &ins.FastPathUniqueChecks[i]
		check := fastPathUniqueChecksItem.Check
		checkOrdinal := fastPathUniqueChecksItem.CheckOrdinal

		// Skip over distribute and projection operations.
		if distributeExpr, isDistribute := check.(*memo.DistributeExpr); isDistribute {
			check = distributeExpr.Input
		}
		if private, ok := c.handleSingleRowInsert(check, checkOrdinal); ok {
			// Build the qualified FastPathCheck into a new UniqueChecksItem for the
			// new InsertExpr being constructed.
			newFastPathUniqueChecksItem := c.e.f.ConstructFastPathUniqueChecksItem(
				fastPathUniqueChecksItem.Check,
				private,
			)
			newFastPathUniqueChecks = append(newFastPathUniqueChecks, newFastPathUniqueChecksItem)
			continue
		}
		return memo.FastPathUniqueChecksExpr{}, false
	}
	return newFastPathUniqueChecks, true
}

// handleSingleRowInsert examines an insert fast path index `check` relation
// which was specially constructed to find a constrained index scan which fully
// consumes all filters needed to perform the uniqueness check in
// `uniqueChecksItem`, and builds the corresponding `private` return value of
// type `FastPathUniqueChecksItemPrivate`. This struct is used to drive insert
// fast path unique constraint checks in the execution engine.
func (c *CustomFuncs) handleSingleRowInsert(
	check memo.RelExpr, checkOrdinal int,
) (private *memo.FastPathUniqueChecksItemPrivate, ok bool) {
	md := c.e.f.Memo().Metadata()

	var scanExpr *memo.ScanExpr
	for check = check.FirstExpr(); check != nil; check = check.NextExpr() {
		expr := check

		if indexJoinExpr, isIndexJoin := expr.(*memo.IndexJoinExpr); isIndexJoin {
			expr = indexJoinExpr.Input
		}
		if scan, isScan := expr.(*memo.ScanExpr); isScan {
			// We need a scan with a constraint for analysis, and it should not be
			// a contradiction (having no spans).
			if scan.Constraint != nil && scan.Constraint.Spans.Count() != 0 {
				scanExpr = scan
				break
			}
		}
	}
	if check == nil {
		return nil, false
	}
	referencedTable := md.Table(scanExpr.Table)
	referencedIndex := referencedTable.Index(scanExpr.Index)
	private = &memo.FastPathUniqueChecksItemPrivate{}
	private.ReferencedTableID = scanExpr.Table
	private.ReferencedIndexOrdinal = scanExpr.Index
	private.CheckOrdinal = checkOrdinal
	private.Locking = scanExpr.Locking

	// Set up InsertCols with the index key columns.
	numKeyCols := scanExpr.Constraint.Spans.Get(0).StartKey().Length()
	private.InsertCols = make(opt.ColList, numKeyCols)
	columnOrdinals := make([]int, numKeyCols)
	for j := 0; j < numKeyCols; j++ {
		ord := referencedIndex.Column(j).Ordinal()
		if scanExpr.Constraint.Columns.Get(j).ID() != scanExpr.Table.ColumnID(ord) {
			// Check that the constraint column ids match those of the chosen index.
			// Maybe this must always be true, but better to check and not allow any
			// incorrect decisions.
			return nil, false
		}
		columnOrdinals[j] = ord
		private.InsertCols[j] = private.ReferencedTableID.ColumnID(ord)
	}
	// The number of KV requests will match the number of spans.
	private.DatumsFromConstraint = make(memo.ScalarListExpr, scanExpr.Constraint.Spans.Count())
	for j := 0; j < scanExpr.Constraint.Spans.Count(); j++ {
		span := scanExpr.Constraint.Spans.Get(j)
		// Verify that the span has the same number of columns as the index key...
		if span.StartKey().Length() != numKeyCols {
			return nil, false
		}
		// ... and that the span has a single key.
		if !span.HasSingleKey(c.e.ctx, c.e.evalCtx) {
			return nil, false
		}
		keySpecification := make(memo.ScalarListExpr, span.StartKey().Length())
		elemTypes := make([]*types.T, span.StartKey().Length())
		for k := 0; k < span.StartKey().Length(); k++ {
			// Populate DatumsFromConstraint with that key column value.
			elemTypes[k] = span.StartKey().Value(k).ResolvedType()
			keySpecification[k] = c.e.f.ConstructConstVal(span.StartKey().Value(k), elemTypes[k])
		}
		private.DatumsFromConstraint[j] = c.e.f.ConstructTuple(keySpecification, types.MakeTuple(elemTypes))
	}
	// We must build at least one DatumsFromConstraint entry to claim success.
	return private, len(private.DatumsFromConstraint) != 0
}
