// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/ordering"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// LimitScanPrivate constructs a new ScanPrivate value that is based on the
// given ScanPrivate. The new private's HardLimit is set to the given limit,
// which must be a constant int datum value. The other fields are inherited from
// the existing private.
func (c *CustomFuncs) LimitScanPrivate(
	scanPrivate *memo.ScanPrivate, limit tree.Datum, required physical.OrderingChoice,
) *memo.ScanPrivate {
	// Determine the scan direction necessary to provide the required ordering.
	_, reverse := ordering.ScanPrivateCanProvide(c.e.mem.Metadata(), scanPrivate, &required)

	newScanPrivate := *scanPrivate
	newScanPrivate.HardLimit = memo.MakeScanLimit(int64(*limit.(*tree.DInt)), reverse)
	return &newScanPrivate
}

// CanLimitFilteredScan returns true if the given scan has not already been
// limited, and is constrained or scans a partial index. This is only possible
// when the required ordering of the rows to be limited can be satisfied by the
// Scan operator.
//
// NOTE: Limiting unconstrained, non-partial index scans is done by the
//       GenerateLimitedScans rule, since that can require IndexJoin operators
//       to be generated.
func (c *CustomFuncs) CanLimitFilteredScan(
	scanPrivate *memo.ScanPrivate, required physical.OrderingChoice,
) bool {
	if scanPrivate.HardLimit != 0 {
		// Don't push limit into scan if scan is already limited. This would
		// usually only happen when normalizations haven't run, as otherwise
		// redundant Limit operators would be discarded.
		return false
	}

	if scanPrivate.Constraint == nil && !scanPrivate.UsesPartialIndex(c.e.mem.Metadata()) {
		// This is not a constrained scan nor a partial index scan, so skip it.
		// The GenerateLimitedScans rule is responsible for limited
		// unconstrained scans on non-partial indexes.
		return false
	}

	ok, _ := ordering.ScanPrivateCanProvide(c.e.mem.Metadata(), scanPrivate, &required)
	return ok
}

// GenerateLimitedScans enumerates all non-inverted and non-partial secondary
// indexes on the Scan operator's table and tries to create new limited Scan
// operators from them. Since this only needs to be done once per table,
// GenerateLimitedScans should only be called on the original unaltered primary
// index Scan operator (i.e. not constrained or limited).
//
// For a secondary index that "covers" the columns needed by the scan, a single
// limited Scan operator is created. For a non-covering index, an IndexJoin is
// constructed to add missing columns to the limited Scan.
//
// Inverted index scans are not guaranteed to produce a specific number
// of result rows because they contain multiple entries for a single row
// indexed. Therefore, they cannot be considered for limited scans.
//
// Partial indexes do not index every row in the table and they can only be used
// in cases where a query filter implies the partial index predicate.
// GenerateLimitedScans deals with limits, but no filters, so it cannot generate
// limited partial index scans. Limiting partial indexes is done by the
// PushLimitIntoFilteredScans rule.
func (c *CustomFuncs) GenerateLimitedScans(
	grp memo.RelExpr,
	scanPrivate *memo.ScanPrivate,
	limit tree.Datum,
	required physical.OrderingChoice,
) {
	limitVal := int64(*limit.(*tree.DInt))

	var sb indexScanBuilder
	sb.init(c, scanPrivate.Table)

	// Iterate over all non-inverted, non-partial indexes, looking for those
	// that can be limited.
	var iter scanIndexIter
	iter.Init(c.e.mem, &c.im, scanPrivate, nil /* filters */, rejectInvertedIndexes|rejectPartialIndexes)
	iter.ForEach(func(index cat.Index, filters memo.FiltersExpr, indexCols opt.ColSet, isCovering bool) {
		newScanPrivate := *scanPrivate
		newScanPrivate.Index = index.Ordinal()

		// If the alternate index does not conform to the ordering, then skip it.
		// If reverse=true, then the scan needs to be in reverse order to match
		// the required ordering.
		ok, reverse := ordering.ScanPrivateCanProvide(
			c.e.mem.Metadata(), &newScanPrivate, &required,
		)
		if !ok {
			return
		}
		newScanPrivate.HardLimit = memo.MakeScanLimit(limitVal, reverse)

		// If the alternate index includes the set of needed columns, then construct
		// a new Scan operator using that index.
		if isCovering {
			sb.setScan(&newScanPrivate)
			sb.build(grp)
			return
		}

		// Otherwise, try to construct an IndexJoin operator that provides the
		// columns missing from the index.
		if scanPrivate.Flags.NoIndexJoin {
			return
		}

		// Scan whatever columns we need which are available from the index, plus
		// the PK columns.
		newScanPrivate.Cols = indexCols.Intersection(scanPrivate.Cols)
		newScanPrivate.Cols.UnionWith(sb.primaryKeyCols())
		sb.setScan(&newScanPrivate)

		// The Scan operator will go into its own group (because it projects a
		// different set of columns), and the IndexJoin operator will be added to
		// the same group as the original Limit operator.
		sb.addIndexJoin(scanPrivate.Cols)

		sb.build(grp)
	})
}

// ScanIsLimited returns true if the scan operator with the given ScanPrivate is
// limited.
func (c *CustomFuncs) ScanIsLimited(sp *memo.ScanPrivate) bool {
	return sp.HardLimit != 0
}

// ScanIsInverted returns true if the index of the given ScanPrivate is an
// inverted index.
func (c *CustomFuncs) ScanIsInverted(sp *memo.ScanPrivate) bool {
	md := c.e.mem.Metadata()
	idx := md.Table(sp.Table).Index(sp.Index)
	return idx.IsInverted()
}

// SplitScanIntoUnionScans returns a Union of Scan operators with hard limits
// that each scan over a single key from the original scan's constraints. This
// is beneficial in cases where the original scan had to scan over many rows but
// had relatively few keys to scan over.
// TODO(drewk): handle inverted scans.
func (c *CustomFuncs) SplitScanIntoUnionScans(
	limitOrdering physical.OrderingChoice, scan memo.RelExpr, sp *memo.ScanPrivate, limit tree.Datum,
) memo.RelExpr {
	const maxScanCount = 16
	const threshold = 4

	cons, ok := c.getKnownScanConstraint(sp)
	if !ok {
		// No valid constraint was found.
		return nil
	}

	keyCtx := constraint.MakeKeyContext(&cons.Columns, c.e.evalCtx)
	limitVal := int(*limit.(*tree.DInt))
	spans := cons.Spans

	// Retrieve the number of keys in the spans.
	keyCount, ok := spans.KeyCount(&keyCtx)
	if !ok {
		return nil
	}
	if keyCount <= 1 {
		// We need more than one key in order to split the existing Scan into
		// multiple Scans.
		return nil
	}
	if int(keyCount) > maxScanCount {
		// The number of new Scans created would exceed maxScanCount.
		return nil
	}

	// Check that the number of rows scanned by the new plan will be smaller than
	// the number scanned by the old plan by at least a factor of "threshold".
	if float64(int(keyCount)*limitVal*threshold) >= scan.Relational().Stats.RowCount {
		// Splitting the scan may not be worth the overhead; creating a sequence of
		// scans unioned together is expensive, so we don't want to create the plan
		// only for the optimizer to use something else. We only want to create the
		// plan if it is likely to be used.
		return nil
	}

	// Retrieve the length of the keys. All keys are required to be the same
	// length (this will be checked later) so we can simply use the length of the
	// first key.
	keyLength := spans.Get(0).StartKey().Length()

	// If the index ordering has a prefix of columns of length keyLength followed
	// by the limitOrdering columns, the scan can be split. Otherwise, return nil.
	hasLimitOrderingSeq, reverse := indexHasOrderingSequence(
		c.e.mem.Metadata(), scan, sp, limitOrdering, keyLength)
	if !hasLimitOrderingSeq {
		return nil
	}

	// Construct a hard limit for the new scans using the result of
	// hasLimitOrderingSeq.
	newHardLimit := memo.MakeScanLimit(int64(limitVal), reverse)

	// Construct a new Spans object containing a new Span for each key in the
	// original Scan's spans.
	newSpans, ok := spans.ExtractSingleKeySpans(&keyCtx, maxScanCount)
	if !ok {
		// Single key spans could not be created.
		return nil
	}

	// Construct a new ScanExpr for each span and union them all together. We
	// output the old ColumnIDs from each union.
	oldColList := opt.ColSetToList(scan.Relational().OutputCols)
	last := c.makeNewScan(sp, cons.Columns, newHardLimit, newSpans.Get(0))
	for i, cnt := 1, newSpans.Count(); i < cnt; i++ {
		newScan := c.makeNewScan(sp, cons.Columns, newHardLimit, newSpans.Get(i))
		last = c.e.f.ConstructUnion(last, newScan, &memo.SetPrivate{
			LeftCols:  opt.ColSetToList(last.Relational().OutputCols),
			RightCols: opt.ColSetToList(newScan.Relational().OutputCols),
			OutCols:   oldColList,
		})
	}
	return last
}

// indexHasOrderingSequence returns whether the scan can provide a given
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
//
func indexHasOrderingSequence(
	md *opt.Metadata,
	scan memo.RelExpr,
	sp *memo.ScanPrivate,
	limitOrdering physical.OrderingChoice,
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

// makeNewScan constructs a new Scan operator with a new TableID and the given
// limit and span. All ColumnIDs and references to those ColumnIDs are
// replaced with new ones from the new TableID. All other fields are simply
// copied from the old ScanPrivate.
func (c *CustomFuncs) makeNewScan(
	sp *memo.ScanPrivate,
	columns constraint.Columns,
	newHardLimit memo.ScanLimit,
	span *constraint.Span,
) memo.RelExpr {
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
	newScanPrivate.Constraint = newConstraint

	return c.e.f.ConstructScan(newScanPrivate)
}

// getKnownScanConstraint returns a Constraint that is known to hold true for
// the output of the Scan operator with the given ScanPrivate. If the
// ScanPrivate has a Constraint, the scan Constraint is returned. Otherwise, an
// effort is made to retrieve a Constraint from the underlying table's check
// constraints. getKnownScanConstraint assumes that the scan is not inverted.
func (c *CustomFuncs) getKnownScanConstraint(
	sp *memo.ScanPrivate,
) (cons *constraint.Constraint, found bool) {
	if sp.Constraint != nil {
		// The ScanPrivate has a constraint, so return it.
		cons = sp.Constraint
	} else {
		// Build a constraint set with the check constraints of the underlying
		// table.
		filters := c.checkConstraintFilters(sp.Table)
		instance := c.initIdxConstraintForIndex(
			nil, /* requiredFilters */
			filters,
			sp.Table,
			sp.Index,
			false, /* isInverted */
		)
		cons = instance.Constraint()
	}
	return cons, !cons.IsUnconstrained()
}
