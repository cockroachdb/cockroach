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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// LimitScanPrivate constructs a new ScanPrivate value that is based on the
// given ScanPrivate. The new private's HardLimit is set to the given limit,
// which must be a constant int datum value. The other fields are inherited from
// the existing private.
func (c *CustomFuncs) LimitScanPrivate(
	scanPrivate *memo.ScanPrivate, limit tree.Datum, required props.OrderingChoice,
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
	scanPrivate *memo.ScanPrivate, required props.OrderingChoice,
) bool {
	if scanPrivate.HardLimit != 0 {
		// Don't push limit into scan if scan is already limited. This would
		// usually only happen when normalizations haven't run, as otherwise
		// redundant Limit operators would be discarded.
		return false
	}

	md := c.e.mem.Metadata()
	if scanPrivate.Constraint == nil && scanPrivate.PartialIndexPredicate(md) == nil {
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
	grp memo.RelExpr, scanPrivate *memo.ScanPrivate, limit tree.Datum, required props.OrderingChoice,
) {
	limitVal := int64(*limit.(*tree.DInt))

	var pkCols opt.ColSet
	var sb indexScanBuilder
	sb.Init(c, scanPrivate.Table)

	// Iterate over all non-inverted, non-partial indexes, looking for those
	// that can be limited.
	var iter scanIndexIter
	iter.Init(c.e.evalCtx, c.e.f, c.e.mem, &c.im, scanPrivate, nil /* filters */, rejectInvertedIndexes|rejectPartialIndexes)
	iter.ForEach(func(index cat.Index, filters memo.FiltersExpr, indexCols opt.ColSet, isCovering bool, constProj memo.ProjectionsExpr) {
		// The iterator rejects partial indexes because there are no filters to
		// imply a partial index predicate. constProj is a projection of
		// constant values based on a partial index predicate. It should always
		// be empty because we iterate only on non-partial indexes. If it is
		// not, we panic to avoid performing a logically incorrect
		// transformation.
		if len(constProj) != 0 {
			panic(errors.AssertionFailedf("expected constProj to be empty"))
		}

		newScanPrivate := *scanPrivate
		newScanPrivate.Index = index.Ordinal()

		// If the alternate index does not conform to the ordering, then skip it.
		// If reverse=true, then the scan needs to be in reverse order to match
		// the required ordering.
		ok, reverse := ordering.ScanPrivateCanProvide(c.e.mem.Metadata(), &newScanPrivate, &required)
		if !ok {
			return
		}
		newScanPrivate.HardLimit = memo.MakeScanLimit(limitVal, reverse)

		// If the alternate index includes the set of needed columns, then construct
		// a new Scan operator using that index.
		if isCovering {
			sb.SetScan(&newScanPrivate)
			sb.Build(grp)
			return
		}

		// Otherwise, try to construct an IndexJoin operator that provides the
		// columns missing from the index.
		if scanPrivate.Flags.NoIndexJoin {
			return
		}

		// Calculate the PK columns once.
		if pkCols.Empty() {
			pkCols = c.PrimaryKeyCols(scanPrivate.Table)
		}

		// Scan whatever columns we need which are available from the index, plus
		// the PK columns.
		newScanPrivate.Cols = indexCols.Intersection(scanPrivate.Cols)
		newScanPrivate.Cols.UnionWith(pkCols)
		sb.SetScan(&newScanPrivate)

		// The Scan operator will go into its own group (because it projects a
		// different set of columns), and the IndexJoin operator will be added to
		// the same group as the original Limit operator.
		sb.AddIndexJoin(scanPrivate.Cols)

		sb.Build(grp)
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
// that each scan over a single key from the original Scan's constraints. This
// is beneficial in cases where the original Scan had to scan over many rows but
// had relatively few keys to scan over.
// TODO(drewk): handle inverted scans.
func (c *CustomFuncs) SplitScanIntoUnionScans(
	limitOrdering props.OrderingChoice, scan memo.RelExpr, sp *memo.ScanPrivate, limit tree.Datum,
) memo.RelExpr {
	const maxScanCount = 16
	const threshold = 4

	cons, ok := c.getKnownScanConstraint(sp)
	if !ok {
		// No valid constraint was found.
		return nil
	}

	// Find the length of the prefix of index columns preceding the first limit
	// ordering column. We will verify later that the entire ordering sequence is
	// represented in the index. Ex:
	//
	//     Index: +1/+2/-3, Limit Ordering: +3 => Prefix Length: 2
	//
	if len(limitOrdering.Columns) == 0 {
		// This case can be handled by GenerateLimitedScans.
		return nil
	}
	keyPrefixLength := cons.Columns.Count()
	for i := 0; i < cons.Columns.Count(); i++ {
		if limitOrdering.Columns[0].Group.Contains(cons.Columns.Get(i).ID()) {
			keyPrefixLength = i
			break
		}
	}
	if keyPrefixLength == 0 {
		// This case can be handled by GenerateLimitedScans.
		return nil
	}

	keyCtx := constraint.MakeKeyContext(&cons.Columns, c.e.evalCtx)
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
			}
		}
	}
	if keyCount <= 0 || (keyCount == 1 && spans.Count() == 1) || budgetExceededIndex == 0 {
		// Ensure that at least one new Scan will be constructed.
		return nil
	}

	scanCount := keyCount
	if scanCount > maxScanCount {
		// We will construct at most maxScanCount new Scans.
		scanCount = maxScanCount
	}

	limitVal := int(*limit.(*tree.DInt))

	if scan.Relational().Stats.Available &&
		float64(scanCount*limitVal*threshold) >= scan.Relational().Stats.RowCount {
		// Splitting the Scan may not be worth the overhead. Creating a sequence of
		// Scans and Unions is expensive, so we only want to create the plan if it
		// is likely to be used.
		return nil
	}

	// The index ordering must have a prefix of columns of length keyLength
	// followed by the limitOrdering columns either in order or in reverse order.
	hasLimitOrderingSeq, reverse := indexHasOrderingSequence(
		c.e.mem.Metadata(), scan, sp, limitOrdering, keyPrefixLength)
	if !hasLimitOrderingSeq {
		return nil
	}
	newHardLimit := memo.MakeScanLimit(int64(limitVal), reverse)

	// makeNewUnion extends the Union tree rooted at 'last' to include 'newScan'.
	// The ColumnIDs of the original Scan are used by the resulting expression.
	makeNewUnion := func(last, newScan memo.RelExpr, outCols opt.ColList) memo.RelExpr {
		return c.e.f.ConstructUnion(last, newScan, &memo.SetPrivate{
			LeftCols:  last.Relational().OutputCols.ToList(),
			RightCols: newScan.Relational().OutputCols.ToList(),
			OutCols:   outCols,
		})
	}

	// Attempt to extract single-key spans and use them to construct limited
	// Scans. Add these Scans to a Union tree. Any remaining spans will be used to
	// construct a single unlimited Scan, which will also be added to the Unions.
	var noLimitSpans constraint.Spans
	var last memo.RelExpr
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
			if last == nil {
				// This is the first limited Scan, so no Union necessary.
				last = c.makeNewScan(sp, cons.Columns, newHardLimit, singleKeySpans.Get(j))
				continue
			}
			// Construct a new Scan for each span.
			newScan := c.makeNewScan(sp, cons.Columns, newHardLimit, singleKeySpans.Get(j))

			// Add the scan to the union tree. If it is the final union in the
			// tree, use the original scan's columns as the union's out columns.
			// Otherwise, create new output column IDs for the union.
			var outCols opt.ColList
			finalUnion := i == n-1 && j == m-1 && noLimitSpans.Count() == 0
			if finalUnion {
				outCols = sp.Cols.ToList()
			} else {
				_, cols := c.DuplicateColumnIDs(sp.Table, sp.Cols)
				outCols = cols.ToList()
			}
			last = makeNewUnion(last, newScan, outCols)
		}
	}
	if noLimitSpans.Count() == spans.Count() {
		// Expect to generate at least one new limited single-key Scan. This could
		// happen if a valid key count could be obtained for at least span, but no
		// span could be split into single-key spans.
		return nil
	}
	if noLimitSpans.Count() == 0 {
		// All spans could be used to generate limited Scans.
		return last
	}

	// If any spans could not be used to generate limited Scans, use them to
	// construct an unlimited Scan and add it to the Union tree.
	newScanPrivate := c.DuplicateScanPrivate(sp)
	newScanPrivate.SetConstraint(c.e.evalCtx, &constraint.Constraint{
		Columns: sp.Constraint.Columns.RemapColumns(sp.Table, newScanPrivate.Table),
		Spans:   noLimitSpans,
	})
	newScan := c.e.f.ConstructScan(newScanPrivate)
	return makeNewUnion(last, newScan, sp.Cols.ToList())
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
//
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
	newScanPrivate.SetConstraint(c.e.evalCtx, newConstraint)

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
		)
		cons = instance.Constraint()
	}
	return cons, !cons.IsUnconstrained()
}
