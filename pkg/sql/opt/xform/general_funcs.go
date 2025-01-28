// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package xform contains logic for transforming SQL queries.
package xform

import (
	"container/list"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/idxconstraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/lookupjoin"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/ordering"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/partialidx"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// CustomFuncs contains all the custom match and replace functions used by the
// exploration rules. The unnamed norm.CustomFuncs allows CustomFuncs to provide
// a clean interface for calling functions from both the xform and norm packages
// using the same struct.
type CustomFuncs struct {
	norm.CustomFuncs
	e  *explorer
	im partialidx.Implicator
	cb lookupjoin.ConstraintBuilder
}

// Init initializes a new CustomFuncs with the given explorer.
func (c *CustomFuncs) Init(e *explorer) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*c = CustomFuncs{
		e: e,
	}
	c.CustomFuncs.Init(e.f)
	c.im.Init(e.ctx, e.f, e.mem.Metadata(), e.evalCtx)
}

// IsCanonicalScan returns true if the given ScanPrivate is an original
// unaltered primary index Scan operator (i.e. unconstrained and not limited).
func (c *CustomFuncs) IsCanonicalScan(scan *memo.ScanPrivate) bool {
	return scan.IsCanonical()
}

// HasInvertedIndexes returns true if at least one inverted index is defined on
// the Scan operator's table.
func (c *CustomFuncs) HasInvertedIndexes(scanPrivate *memo.ScanPrivate) bool {
	md := c.e.mem.Metadata()
	tab := md.Table(scanPrivate.Table)

	// Skip the primary index because it cannot be inverted.
	for i := 1; i < tab.IndexCount(); i++ {
		if tab.Index(i).Type() == idxtype.INVERTED {
			return true
		}
	}
	return false
}

// RemapJoinColsInFilter returns a new FiltersExpr where columns in leftSrc's
// table are replaced with columns of the same ordinal in leftDst's table and
// rightSrc's table are replaced with columns of the same ordinal in rightDst's
// table. leftSrc and leftDst must scan the same base table. rightSrc and
// rightDst must scan the same base table.
func (c *CustomFuncs) remapJoinColsInFilter(
	filters memo.FiltersExpr, leftSrc, leftDst, rightSrc, rightDst *memo.ScanPrivate,
) memo.FiltersExpr {
	newFilters := c.remapJoinColsInScalarExpr(&filters, leftSrc, leftDst, rightSrc, rightDst).(*memo.FiltersExpr)
	return *newFilters
}

// remapJoinColsInScalarExpr remaps ColumnIDs in a scalar expression involving
// columns from a join of two base table scans.
func (c *CustomFuncs) remapJoinColsInScalarExpr(
	scalar opt.ScalarExpr, leftSrc, leftDst, rightSrc, rightDst *memo.ScanPrivate,
) opt.ScalarExpr {
	md := c.e.mem.Metadata()
	if md.Table(leftSrc.Table).ID() != md.Table(leftDst.Table).ID() {
		panic(errors.AssertionFailedf("left scans must have the same base table"))
	}
	if md.Table(rightSrc.Table).ID() != md.Table(rightDst.Table).ID() {
		panic(errors.AssertionFailedf("right scans must have the same base table"))
	}
	if leftSrc.Cols.Len() != leftDst.Cols.Len() {
		panic(errors.AssertionFailedf("left scans must have the same number of columns"))
	}
	if rightSrc.Cols.Len() != rightDst.Cols.Len() {
		panic(errors.AssertionFailedf("rightscans must have the same number of columns"))
	}
	// Remap each column in leftSrc to a column in leftDst.
	var colMap opt.ColMap
	for srcCol, ok := leftSrc.Cols.Next(0); ok; srcCol, ok = leftSrc.Cols.Next(srcCol + 1) {
		ord := leftSrc.Table.ColumnOrdinal(srcCol)
		dstCol := leftDst.Table.ColumnID(ord)
		colMap.Set(int(srcCol), int(dstCol))
	}
	for srcCol, ok := rightSrc.Cols.Next(0); ok; srcCol, ok = rightSrc.Cols.Next(srcCol + 1) {
		ord := rightSrc.Table.ColumnOrdinal(srcCol)
		dstCol := rightDst.Table.ColumnID(ord)
		colMap.Set(int(srcCol), int(dstCol))
	}
	return c.e.f.RemapCols(scalar, colMap)
}

// checkConstraintFilters generates all filters that we can derive from the
// check constraints. These are constraints that have been validated and are
// non-nullable. We only use non-nullable check constraints because they
// behave differently from filters on NULL. Check constraints are satisfied
// when their expression evaluates to NULL, while filters are not.
//
// For example, the check constraint a > 1 is satisfied if a is NULL but the
// equivalent filter a > 1 is not.
//
// These filters do not really filter any rows, they are rather facts or
// guarantees about the data but treating them as filters may allow some
// indexes to be constrained and used. Consider the following example:
//
// CREATE TABLE abc (
//
//	a INT PRIMARY KEY,
//	b INT NOT NULL,
//	c STRING NOT NULL,
//	CHECK (a < 10 AND a > 1),
//	CHECK (b < 10 AND b > 1),
//	CHECK (c in ('first', 'second')),
//	INDEX secondary (b, a),
//	INDEX tertiary (c, b, a))
//
// Now consider the query: SELECT a, b WHERE a > 5
//
// Notice that the filter provided previously wouldn't let the optimizer use
// the secondary or tertiary indexes. However, given that we can use the
// constraints on a, b and c, we can actually use the secondary and tertiary
// indexes. In fact, for the above query we can do the following:
//
// select
//
//	├── columns: a:1(int!null) b:2(int!null)
//	├── scan abc@tertiary
//	│		├── columns: a:1(int!null) b:2(int!null)
//	│		└── constraint: /3/2/1: [/'first'/2/6 - /'first'/9/9] [/'second'/2/6 - /'second'/9/9]
//	└── filters
//	      └── gt [type=bool]
//	          ├── variable: a [type=int]
//	          └── const: 5 [type=int]
//
// Similarly, the secondary index could also be used. All such index scans
// will be added to the memo group.
func (c *CustomFuncs) checkConstraintFilters(tabID opt.TableID) memo.FiltersExpr {
	md := c.e.mem.Metadata()
	tabMeta := md.TableMeta(tabID)
	if tabMeta.Constraints == nil {
		return memo.FiltersExpr{}
	}
	filters := *tabMeta.Constraints.(*memo.FiltersExpr)
	// Limit slice capacity to allow the caller to append if necessary.
	return filters[:len(filters):len(filters)]
}

func (c *CustomFuncs) initIdxConstraintForIndex(
	requiredFilters, optionalFilters memo.FiltersExpr, tabID opt.TableID, indexOrd int,
) (ic *idxconstraint.Instance) {
	ic = &idxconstraint.Instance{}

	// Fill out data structures needed to initialize the idxconstraint library.
	// Use LaxKeyColumnCount, since all columns <= LaxKeyColumnCount are
	// guaranteed to be part of each row's key (i.e. not stored in row's value,
	// which does not take part in an index scan). Note that the OrderingColumn
	// slice cannot be reused, as Instance.Init can use it in the constraint.
	md := c.e.mem.Metadata()
	tabMeta := md.TableMeta(tabID)
	index := tabMeta.Table.Index(indexOrd)
	ps := tabMeta.IndexPartitionLocality(index.Ordinal())
	columns := make([]opt.OrderingColumn, index.LaxKeyColumnCount())
	var notNullCols opt.ColSet
	for i := range columns {
		col := index.Column(i)
		ordinal := col.Ordinal()
		nullable := col.IsNullable()
		colID := tabID.ColumnID(ordinal)
		columns[i] = opt.MakeOrderingColumn(colID, col.Descending)
		if !nullable {
			notNullCols.Add(colID)
		}
	}

	// Generate index constraints.
	ic.Init(
		c.e.ctx, requiredFilters, optionalFilters,
		columns, notNullCols, tabMeta.ComputedCols,
		tabMeta.ColsInComputedColsExpressions,
		true /* consolidate */, c.e.evalCtx, c.e.f, ps,
		c.checkCancellation,
	)
	return ic
}

// splitScanIntoUnionScans tries to find a UnionAll of Scan operators (with an
// optional hard limit) that each scan over a single key from the original
// Scan's constraints. The UnionAll is returned if the scans can provide the
// given ordering, and if the statistics suggest that splitting the scan could
// be beneficial. If no such UnionAll of Scans can be found, ok=false is
// returned. This is beneficial in cases where an ordering is required on a
// suffix of the index columns, and constraining the first column(s) allows the
// scan to provide that ordering.
func (c *CustomFuncs) splitScanIntoUnionScans(
	ordering props.OrderingChoice,
	scan memo.RelExpr,
	sp *memo.ScanPrivate,
	cons *constraint.Constraint,
	limit int,
	keyPrefixLength int,
) (_ memo.RelExpr, ok bool) {
	return c.splitScanIntoUnionScansOrSelects(ordering, scan, sp, cons, limit, keyPrefixLength, nil /* filters */)
}

// splitScanIntoUnionScansOrSelects tries to find a UnionAll of Scan operators,
// (with an optional hard limit), or UnionAll of Selects from Scans that each
// scan over a single key from the original Scan's constraints. The UnionAll is
// returned if the scans/selects can provide the given ordering, and if the
// statistics suggest that splitting them could be beneficial. If no such
// UnionAll of Scans/Selects can be found, ok=false is returned. This is
// beneficial in cases where an ordering is required on a suffix of the index
// columns, and constraining the first column(s) allows the scan to provide that
// ordering.
// TODO(drewk): handle inverted scans.
func (c *CustomFuncs) splitScanIntoUnionScansOrSelects(
	ordering props.OrderingChoice,
	scan memo.RelExpr,
	sp *memo.ScanPrivate,
	cons *constraint.Constraint,
	limit int,
	keyPrefixLength int,
	filters memo.FiltersExpr,
) (_ memo.RelExpr, ok bool) {
	// Hash index bucket count may be higher than 16, especially in large table
	// cases. Pick a default maxScanCount sufficiently high to avoid disabling
	// this optimization for such indexes with high bucket count, but not so high
	// that we might plan thousands of Scans. If IN list check constraints on the
	// first index column exist, use that to find the number for maxScanCount.
	// TODO(msirek): Is there a more efficient way to represent this plan when #
	//   of scans is high, for example, introduce new operation types like
	//   "SkipScanExpr", "SkipSelectExpr", "LooseScanExpr", "LooseSelectExpr" (see
	//   https://wiki.postgresql.org/wiki/Loose_indexscan and
	//   https://github.com/cockroachdb/cockroach/pull/38216), or maybe just set
	//   new read modes in the pre-existing ScanExpr and SelectExpr?
	//   This would allow us to represent order-preserving skip scans without the
	//   need for large UNION ALL expressions.
	//
	// TODO(msirek): We may want to lift this max entirely at some point.
	// maxScanCount is the maximum number of scans to produce if there is no
	// non-null check constraint defined on the first index column.
	maxScanCount := 256
	// hardMaxScanCount is the upper limit on what we'll attempt to increase
	// maxScanCount to using numAllowedValues() based on check constraint values.
	// This is configurable using a session setting opt_split_scan_limit.
	// If OptSplitScanLimit is below maxScanCount, we will decrease maxScanCount
	// to that value because a hard limit should never be lower than a soft limit.
	hardMaxScanCount := int(c.e.evalCtx.SessionData().OptSplitScanLimit)
	if hardMaxScanCount < maxScanCount {
		maxScanCount = hardMaxScanCount
	}
	if cons.Columns.Count() == 0 {
		return
	}
	firstColID := cons.Columns.Get(0).ID()
	// The number of allowed values from the check constraints may surpass
	// maxScanCount. Find this number so we don't inadvertently disable this
	// optimization.
	if checkConstraintValues, ok := c.numAllowedValues(firstColID, sp.Table); ok {
		if maxScanCount < checkConstraintValues {
			if checkConstraintValues > hardMaxScanCount {
				maxScanCount = hardMaxScanCount
			} else {
				maxScanCount = checkConstraintValues
			}
		}
	}
	keyCtx := constraint.MakeKeyContext(c.e.ctx, &cons.Columns, c.e.evalCtx)
	spans := cons.Spans

	// Get the total number of keys that can be extracted from the spans. Also
	// keep track of the first span which would exceed the Scan budget if it was
	// used to construct new Scan operators.
	var keyCount int
	budgetExceededIndex := spans.Count()
	additionalScanBudget := maxScanCount
	for i := 0; i < spans.Count(); i++ {
		if cnt, ok := spans.Get(i).KeyCount(&keyCtx, keyPrefixLength); ok {
			keyCount += int(cnt)
			additionalScanBudget -= int(cnt)
			if additionalScanBudget < 0 {
				// Splitting any spans from this span on would lead to exceeding the max
				// Scan count. Keep track of the index of this span.
				budgetExceededIndex = i
				break
			}
		}
	}
	if keyCount <= 0 || (keyCount == 1 && spans.Count() == 1) || budgetExceededIndex == 0 {
		// Ensure that at least one new Scan will be constructed.
		return nil, false
	}

	scanCount := keyCount
	if scanCount > maxScanCount {
		// We will construct at most maxScanCount new Scans.
		scanCount = maxScanCount
	}
	rowCount := scan.Relational().Statistics().RowCount
	if limit > 0 {
		nLogN := rowCount * math.Log2(rowCount)
		if scan.Relational().Statistics().Available &&
			float64(scanCount*randIOCostFactor+limit*seqIOCostFactor) >= nLogN {
			// Splitting the Scan may not be worth the overhead. Creating a sequence of
			// Scans and Unions is expensive, so we only want to create the plan if it
			// is likely to be used. This rewrite is competing against a plan with a
			// sort, which is approximately O(N log N), so use that as a rough costing
			// threshold.
			return nil, false
		}
	}

	// The index ordering must have a prefix of columns of length keyLength
	// followed by the ordering columns either in order or in reverse order.
	hasOrderingSeq, reverse := indexHasOrderingSequence(
		c.e.mem.Metadata(), scan, sp, ordering, keyPrefixLength)
	if !hasOrderingSeq {
		return nil, false
	}
	newHardLimit := memo.MakeScanLimit(int64(limit), reverse)
	// With a filter we might have to scan all rows before reaching the LIMIT.
	if !filters.IsTrue() {
		newHardLimit = 0
	}

	// makeNewUnion builds a UnionAll tree node with 'left' and 'right' child
	// nodes, which may be scans, selects or previously-built UnionAllExprs. The
	// ColumnIDs of the original Scan are used by the resulting expression, but it
	// is up to the caller to set this via outCols.
	makeNewUnion := func(left, right memo.RelExpr, outCols opt.ColList) memo.RelExpr {
		return c.e.f.ConstructUnionAll(left, right, &memo.SetPrivate{
			LeftCols:  left.Relational().OutputCols.ToList(),
			RightCols: right.Relational().OutputCols.ToList(),
			OutCols:   outCols,
		})
	}

	// Attempt to extract single-key spans and use them to construct limited
	// Scans. Add these Scans to a UnionAll tree. Any remaining spans will be used
	// to construct a single unlimited Scan, which will also be added to the
	// UnionAll tree.
	var noLimitSpans constraint.Spans
	var last memo.RelExpr
	spColList := sp.Cols.ToList()
	queue := list.New()
	for i, n := 0, spans.Count(); i < n; i++ {
		if i >= budgetExceededIndex {
			// The Scan budget has been reached; no additional Scans can be created.
			noLimitSpans.Append(spans.Get(i))
			continue
		}
		singleKeySpans, ok := spans.Get(i).Split(&keyCtx, keyPrefixLength)
		if !ok {
			// Single key spans could not be extracted from this span, so add it to
			// the set of spans that will be used to construct an unlimited Scan.
			noLimitSpans.Append(spans.Get(i))
			continue
		}
		for j, m := 0, singleKeySpans.Count(); j < m; j++ {
			// Construct a new Scan for each span.
			// Note: newHardLimit will be 0 (i.e., no limit) if there are
			// filters to be applied in a Select.
			newScanPrivate := c.makeNewScanPrivate(
				sp,
				cons.Columns,
				newHardLimit,
				singleKeySpans.Get(j),
			)
			newScanOrLimitedSelect := c.e.f.ConstructScan(newScanPrivate)
			if !filters.IsTrue() {
				// If there are filters, apply them and a limit. The limit is
				// not needed if there are no filters because the scan's hard
				// limit will be set (see newHardLimit).
				//
				// TODO(mgartner/msirek): Converting ColSets to ColLists here is
				// only safe because column IDs are always allocated in a
				// consistent, ascending order for each duplicated table in the
				// metadata. If column ID allocation changes, this could break.
				newColList := newScanPrivate.Cols.ToList()
				newScanOrLimitedSelect = c.e.f.ConstructLimit(
					c.e.f.ConstructSelect(
						newScanOrLimitedSelect,
						c.RemapScanColsInFilter(filters, sp, newScanPrivate),
					),
					c.IntConst(tree.NewDInt(tree.DInt(limit))),
					ordering.RemapColumns(spColList, newColList),
				)
			}
			queue.PushBack(newScanOrLimitedSelect)
		}
	}

	// Return early if the queue is empty. This is possible if the first
	// splittable span splits into a number of keys greater than maxScanCount.
	if queue.Len() == 0 {
		return nil, false
	}

	var outCols opt.ColList
	oddNumScans := (queue.Len() % 2) != 0

	// Make the UNION ALLs as a balanced tree. This performs better for large
	// numbers of spans than a left-deep tree because neighboring branches can
	// be processed in parallel and the deeper the tree, the more comparisons the
	// rows from the deepest nodes in the tree need to undergo as rows are merged
	// for each UNION ALL operation.
	//
	// This balanced tree building  works by feeding RelExprs into a queue and
	// popping them from the front, combining each pair of nodes with a
	// UnionAllOp. Initially only Scan or Select operations are pushed to the
	// queue, so the first round of UnionAllOps, representing the deepest level of
	// the tree, contain only unions of scans. As UnionAllOps are built, they are
	// fed into the back of the queue, and when all scans have been popped from
	// the queue, the unions are processed. The unions themselves are then built
	// as children of new UnionAllOps. For every two UnionAllOps popped, we push
	// one new parent UnionAllOp onto the queue. Eventually we will end up with
	// only a single RelExpr in the queue, which is our root node.
	for queue.Len() > 1 {
		left := queue.Front()
		queue.Remove(left)
		right := queue.Front()
		queue.Remove(right)
		if oddNumScans &&
			(left.Value.(memo.RelExpr).Op() == opt.UnionAllOp ||
				right.Value.(memo.RelExpr).Op() == opt.UnionAllOp) {
			// Not necessary, but keep spans with lower values in the left subtree.
			left, right = right, left
		}
		// TODO(mgartner/msirek): Converting ColSets to ColLists here is only safe
		// because column IDs are always allocated in a consistent, ascending order
		// for each duplicated table in the metadata. If column ID allocation
		// changes, this could break.
		if noLimitSpans.Count() == 0 && queue.Len() == 0 {
			outCols = sp.Cols.ToList()
		} else {
			_, cols := c.DuplicateColumnIDs(sp.Table, sp.Cols)
			outCols = cols.ToList()
		}
		queue.PushBack(makeNewUnion(left.Value.(memo.RelExpr), right.Value.(memo.RelExpr), outCols))
	}
	lastElem := queue.Front()
	last = lastElem.Value.(memo.RelExpr)
	queue.Remove(lastElem)

	if noLimitSpans.Count() == spans.Count() {
		// Expect to generate at least one new limited single-key Scan. This could
		// happen if a valid key count could be obtained for at least span, but no
		// span could be split into single-key spans.
		return nil, false
	}
	if noLimitSpans.Count() == 0 {
		// All spans could be used to generate limited Scans.
		return last, true
	}

	// If any spans could not be used to generate limited Scans, use them to
	// construct an unlimited Scan and add it to the UnionAll tree.
	newScanPrivate := c.DuplicateScanPrivate(sp)
	// Map from cons Columns to new columns.
	newScanPrivate.SetConstraint(c.e.ctx, c.e.evalCtx, &constraint.Constraint{
		Columns: cons.Columns.RemapColumns(sp.Table, newScanPrivate.Table),
		Spans:   noLimitSpans,
	})
	// TODO(mgartner): We should be able to add a LIMIT above the Scan or Select
	// below, as long as we remap the original ordering columns. This could
	// allow a top-k to be planned instead of a sort.
	newScanOrSelect := c.e.f.ConstructScan(newScanPrivate)
	if !filters.IsTrue() {
		newScanOrSelect = c.e.f.ConstructSelect(
			newScanOrSelect,
			c.RemapScanColsInFilter(filters, sp, newScanPrivate),
		)
	}
	// TODO(mgartner/msirek): Converting ColSets to ColLists here is only safe
	// because column IDs are always allocated in a consistent, ascending order
	// for each duplicated table in the metadata. If column ID allocation
	// changes, this could break.
	return makeNewUnion(last, newScanOrSelect, spColList), true
}

// numAllowedValues returns the number of allowed values for a column with a
// given columnID by checking if there is a CHECK constraint with no nulls
// defined on it and returning the maximum number of results that can be read
// from Spans on that individual column. If not found, (0, false) is returned.
func (c *CustomFuncs) numAllowedValues(
	columnID opt.ColumnID, tabID opt.TableID,
) (numValues int, ok bool) {
	tabMeta := c.e.f.Metadata().TableMeta(tabID)
	constraints := tabMeta.Constraints
	if constraints == nil {
		return 0, false
	}
	if constraints.Op() != opt.FiltersOp {
		return 0, false
	}
	cols := opt.MakeColSet(columnID)
	filters := *constraints.(*memo.FiltersExpr)
	// For each ANDed check constraint...
	for i := 0; i < len(filters); i++ {
		filter := &filters[i]
		// This must be some type of comparison operation, or an OR or AND
		// expression. These operations have at least 2 children.
		if filter.Condition.ChildCount() < 2 {
			continue
		}
		if filter.ScalarProps().Constraints == nil {
			continue
		}
		for j := 0; j < filter.ScalarProps().Constraints.Length(); j++ {
			constraint := filter.ScalarProps().Constraints.Constraint(j)
			firstConstraintColID := constraint.Columns.Get(0).ID()
			if columnID != firstConstraintColID {
				continue
			}
			if constraint.IsContradiction() || constraint.IsUnconstrained() {
				continue
			}
			if distinctVals, ok := constraint.CalculateMaxResults(c.e.ctx, c.e.evalCtx, cols, cols); ok {
				if distinctVals > math.MaxInt32 {
					return math.MaxInt32, true
				}
				return int(distinctVals), true
			}
		}
	}
	return 0, false
}

// indexHasOrderingSequenceOnOne returns whether the Scan can provide an
// ordering on at least one of the columns in cols, in either the forward or
// reverse direction, under the assumption that we are scanning a single-key
// span with the given keyLength.
func indexHasOrderingSequenceOnOne(
	md *opt.Metadata, scan memo.RelExpr, sp *memo.ScanPrivate, cols opt.ColSet, keyLength int,
) (hasSequence bool) {
	for col, ok := cols.Next(0); ok; col, ok = cols.Next(col) {
		var ord props.OrderingChoice
		ord.AppendCol(col, false)
		if has, _ := indexHasOrderingSequence(md, scan, sp, ord, keyLength); has {
			return true
		}
		col++
	}
	return false
}

// indexHasOrderingSequence returns whether the Scan can provide a given
// ordering under the assumption that we are scanning a single-key span with the
// given keyLength (and if so, whether we need to scan it in reverse).
// For example:
//
// index: +1/-2/+3,
// limitOrdering: -2/+3,
// keyLength: 1,
// =>
// hasSequence: True, reverse: False
//
// index: +1/-2/+3,
// limitOrdering: +2/-3,
// keyLength: 1,
// =>
// hasSequence: True, reverse: True
//
// index: +1/-2/+3/+4,
// limitOrdering: +3/+4,
// keyLength: 1,
// =>
// hasSequence: False, reverse: False
func indexHasOrderingSequence(
	md *opt.Metadata,
	scan memo.RelExpr,
	sp *memo.ScanPrivate,
	limitOrdering props.OrderingChoice,
	keyLength int,
) (hasSequence, reverse bool) {
	tableMeta := md.TableMeta(sp.Table)
	index := tableMeta.Table.Index(sp.Index)

	if keyLength > index.ColumnCount() {
		// The key contains more columns than the index. The limit ordering sequence
		// cannot be part of the index ordering.
		return false, false
	}

	// Create a copy of the Scan's FuncDepSet, and add the first 'keyCount'
	// columns from the index as constant columns. The columns are constant
	// because the span contains only a single key on those columns.
	var fds props.FuncDepSet
	fds.CopyFrom(&scan.Relational().FuncDeps)
	prefixCols := opt.ColSet{}
	for i := 0; i < keyLength; i++ {
		col := sp.Table.IndexColumnID(index, i)
		prefixCols.Add(col)
	}
	fds.AddConstants(prefixCols)

	// Use fds to simplify a copy of the limit ordering; the prefix columns will
	// become part of the optional ColSet.
	requiredOrdering := limitOrdering.Copy()
	requiredOrdering.Simplify(&fds)

	// If the ScanPrivate can satisfy requiredOrdering, it must return columns
	// ordered by a prefix of length keyLength, followed by the columns of
	// limitOrdering.
	return ordering.ScanPrivateCanProvide(md, sp, &requiredOrdering)
}

// makeNewScanPrivate returns a new ScanPrivate with a new TableID and the given
// limit and span. All ColumnIDs and references to those ColumnIDs are replaced
// with new ones from the new TableID. All other fields are simply copied from
// the old ScanPrivate.
func (c *CustomFuncs) makeNewScanPrivate(
	sp *memo.ScanPrivate,
	columns constraint.Columns,
	newHardLimit memo.ScanLimit,
	span *constraint.Span,
) *memo.ScanPrivate {
	newScanPrivate := c.DuplicateScanPrivate(sp)

	// duplicateScanPrivate does not initialize the Constraint or HardLimit
	// fields, so we do that now.
	newScanPrivate.HardLimit = newHardLimit

	// Construct the new Constraint field with the given span and remapped
	// ordering columns.
	var newSpans constraint.Spans
	newSpans.InitSingleSpan(span)
	newConstraint := &constraint.Constraint{
		Columns: columns.RemapColumns(sp.Table, newScanPrivate.Table),
		Spans:   newSpans,
	}
	newScanPrivate.SetConstraint(c.e.ctx, c.e.evalCtx, newConstraint)

	return newScanPrivate
}

// getKnownScanConstraint returns a Constraint that is known to hold true for
// the output of the Scan operator with the given ScanPrivate. If the
// ScanPrivate has a Constraint, the scan Constraint is returned. Otherwise, an
// effort is made to retrieve a Constraint from the underlying table's check
// constraints. getKnownScanConstraint assumes that the scan is not inverted.
func (c *CustomFuncs) getKnownScanConstraint(
	sp *memo.ScanPrivate,
) (_ *constraint.Constraint, found bool) {
	if sp.Constraint != nil {
		// The ScanPrivate has a constraint, so return it.
		return sp.Constraint, !sp.Constraint.IsUnconstrained()
	}

	// Build a constraint set with the check constraints of the underlying
	// table.
	filters := c.checkConstraintFilters(sp.Table)
	instance := c.initIdxConstraintForIndex(
		nil, /* requiredFilters */
		filters,
		sp.Table,
		sp.Index,
	)
	var cons constraint.Constraint
	instance.Constraint(&cons)
	return &cons, !cons.IsUnconstrained()
}
