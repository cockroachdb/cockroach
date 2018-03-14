// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"fmt"
	"sort"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const nonCoveringIndexPenalty = 10

// analyzeOrderingFn is the interface through which the index selection code
// discovers how useful is the ordering provided by a certain index. The higher
// layer (select) desires a certain ordering on a number of columns; it calls
// into the index selection code with an analyzeOrderingFn that computes how
// many columns of that desired ordering are satisfied by the index ordering.
// Both the number of matching columns and the total columns in the desired
// ordering are returned.
//
// For example, consider the table t {
//    a INT,
//    b INT,
//    c INT,
//    INDEX ab (a, b)
//    INDEX bac (b, a, c)
// }
//
// For `SELECT * FROM t ORDER BY a, c`, the desired ordering is (a, c);
// totalCols is 2. In this case:
//  - the primary index has no ordering on a, b, c; matchingCols is 0.
//  - the ab index matches the first column of the desired ordering;
//    matchingCols is 1.
//  - the bac index doesn't match the desired ordering at all; mathcingCols
//    is 0.
//
// For `SELECT * FROM t WHERE b=1 ORDER BY a, c`, the desired ordering is (a, c);
// totalCols is 2. In this case:
//  - the primary index has no ordering on a, b, c; matchingCols is 0.
//  - the ab index matches the first column of the desired ordering;
//    matchingCols is 1.
//  - the bac index, along with the fact that b is constrained to a single
//    value, matches the desired ordering; matchingCols is 2.
type analyzeOrderingFn func(indexProps physicalProps) (matchingCols, totalCols int)

// selectIndex analyzes the scanNode to determine if there is an index
// available that can fulfill the query with a more restrictive scan.
//
// Analysis currently consists of a simplification of the filter expression,
// replacing expressions which are not usable by indexes by "true". The
// simplified expression is then considered for each index and a set of range
// constraints is created for the index. The candidate indexes are ranked using
// these constraints and the best index is selected. The constraints are then
// transformed into a set of spans to scan within the index.
//
// The analyzeOrdering function is used to determine how useful the ordering of
// an index is. If no particular ordering is desired, it can be nil.
//
// If preferOrderMatching is true, we prefer an index that matches the desired
// ordering completely, even if it is not a covering index.
func (p *planner) selectIndex(
	ctx context.Context, s *scanNode, analyzeOrdering analyzeOrderingFn, preferOrderMatching bool,
) (planNode, error) {
	if s.desc.IsEmpty() {
		// No table.
		s.initOrdering(0 /* exactPrefix */, p.EvalContext())
		return s, nil
	}

	if s.filter == nil && analyzeOrdering == nil && s.specifiedIndex == nil {
		// No where-clause, no ordering, and no specified index.
		s.initOrdering(0 /* exactPrefix */, p.EvalContext())
		var err error
		s.spans, err = unconstrainedSpans(s.desc, s.index)
		if err != nil {
			return nil, errors.Wrapf(err, "table ID = %d, index ID = %d", s.desc.ID, s.index.ID)
		}
		return s, nil
	}

	candidates := make([]*indexInfo, 0, len(s.desc.Indexes)+1)
	if s.specifiedIndex != nil {
		// An explicit secondary index was requested. Only add it to the candidate
		// indexes list.
		candidates = append(candidates, &indexInfo{
			desc:  s.desc,
			index: s.specifiedIndex,
		})
	} else {
		candidates = append(candidates, &indexInfo{
			desc:  s.desc,
			index: &s.desc.PrimaryIndex,
		})
		for i := range s.desc.Indexes {
			candidates = append(candidates, &indexInfo{
				desc:  s.desc,
				index: &s.desc.Indexes[i],
			})
		}
	}

	for _, c := range candidates {
		c.init(s)
	}

	if s.filter != nil {
		filterExpr, err := opt.BuildScalarExpr(s.filter, p.EvalContext())
		if err != nil {
			return nil, err
		}
		for _, c := range candidates {
			if err := c.makeIndexConstraints(
				filterExpr, p.EvalContext(),
			); err != nil {
				return nil, err
			}
			if spans, ok := c.ic.Spans(); ok && len(spans) == 0 {
				// No spans (i.e. the filter is always false). Note that if a filter
				// results in no constraints, ok would be false.
				return newZeroNode(s.resultColumns), nil
			}
		}
	}

	if s.noIndexJoin {
		// Eliminate non-covering indexes. We do this after the check above for
		// constant false filter.
		for i := 0; i < len(candidates); {
			if !candidates[i].covering {
				candidates[i] = candidates[len(candidates)-1]
				candidates = candidates[:len(candidates)-1]
			} else {
				i++
			}
		}
		if len(candidates) == 0 {
			// The primary index is always covering. So the only way this can
			// happen is if we had a specified index.
			if s.specifiedIndex == nil {
				panic("no covering indexes")
			}
			return nil, fmt.Errorf("index \"%s\" is not covering and NO_INDEX_JOIN was specified",
				s.specifiedIndex.Name)
		}
	}

	for _, c := range candidates {
		if analyzeOrdering != nil {
			c.analyzeOrdering(ctx, s, analyzeOrdering, preferOrderMatching, p.EvalContext())
		}
	}

	indexInfoByCost(candidates).Sort()

	if log.V(2) {
		for i, c := range candidates {
			spans, ok := c.ic.Spans()
			spansStr := "<none>"
			if ok {
				spansStr = fmt.Sprintf("%v", spans)
			}
			log.Infof(ctx, "%d: selectIndex(%s): cost=%v logicalSpans=%s reverse=%t",
				i, c.index.Name, c.cost, spansStr, c.reverse)
		}
	}

	// After sorting, candidates[0] contains the best index. Copy its info into
	// the scanNode.
	c := candidates[0]
	s.index = c.index
	s.specifiedIndex = nil
	s.run.isSecondaryIndex = (c.index != &s.desc.PrimaryIndex)

	logicalSpans, ok := c.ic.Spans()
	var err error
	s.spans, err = spansFromLogicalSpans(s.desc, c.index, logicalSpans, ok)
	if err != nil {
		return nil, errors.Wrapf(
			err, "logicalSpans = %v, table ID = %d, index ID = %d",
			logicalSpans, s.desc.ID, s.index.ID,
		)
	}

	if len(s.spans) == 0 {
		// There are no spans to scan.
		return newZeroNode(s.resultColumns), nil
	}

	s.origFilter = s.filter
	if s.filter != nil {
		s.filter = c.ic.RemainingFilter(&s.filterVars)

		// Constraint propagation may have produced new constant sub-expressions.
		// Propagate them and check if s.filter can be applied prematurely.
		if s.filter != nil {
			var err error
			s.filter, err = p.extendedEvalCtx.NormalizeExpr(s.filter)
			if err != nil {
				return nil, err
			}
			switch s.filter {
			case tree.DBoolFalse, tree.DNull:
				return &zeroNode{}, nil
			case tree.DBoolTrue:
				s.filter = nil
			}
		}
	}
	s.filterVars.Rebind(s.filter, true, false)

	s.reverse = c.reverse

	var plan planNode
	if c.covering && c.index.Type != sqlbase.IndexDescriptor_INVERTED {
		s.initOrdering(c.exactPrefix, p.EvalContext())
		plan = s
	} else {
		// Note: makeIndexJoin destroys s and returns a new index scan
		// node. The filter in that node may be different from the
		// original table filter.
		plan, s, err = p.makeIndexJoin(s, c.exactPrefix)
		if err != nil {
			return nil, err
		}
	}

	if log.V(3) {
		log.Infof(ctx, "%s: filter=%v", c.index.Name, s.filter)
		for i, span := range s.spans {
			log.Infof(ctx, "%s/%d: %s", c.index.Name, i, sqlbase.PrettySpan(sqlbase.IndexKeyValDirs(c.index), span, 2))
		}
	}

	return plan, nil
}

type indexInfo struct {
	desc        *sqlbase.TableDescriptor
	index       *sqlbase.IndexDescriptor
	cost        float64
	covering    bool // Does the index cover the required IndexedVars?
	reverse     bool
	exactPrefix int

	ic opt.IndexConstraints
}

func (v *indexInfo) init(s *scanNode) {
	v.covering = v.isCoveringIndex(s)

	// The base cost is the number of keys per row.
	if v.index == &v.desc.PrimaryIndex {
		// The primary index contains 1 key per column plus the sentinel key per
		// row.
		v.cost = float64(1 + len(v.desc.Columns) - len(v.desc.PrimaryIndex.ColumnIDs))
	} else {
		v.cost = 1
		if !v.covering {
			v.cost += float64(1 + len(v.desc.Columns) - len(v.desc.PrimaryIndex.ColumnIDs))
			// Non-covering indexes are significantly more expensive than covering
			// indexes.
			v.cost *= nonCoveringIndexPenalty
		}
	}
}

// analyzeOrdering analyzes the ordering provided by the index and determines
// if it matches the ordering requested by the query. Non-matching orderings
// increase the cost of using the index.
//
// If preferOrderMatching is true, we prefer an index that matches the desired
// ordering completely, even if it is not a covering index.
func (v *indexInfo) analyzeOrdering(
	ctx context.Context,
	scan *scanNode,
	analyzeOrdering analyzeOrderingFn,
	preferOrderMatching bool,
	evalCtx *tree.EvalContext,
) {
	// Analyze the ordering provided by the index (either forward or reverse).
	fwdIndexProps := scan.computePhysicalProps(v.index, v.exactPrefix, false, evalCtx)
	revIndexProps := scan.computePhysicalProps(v.index, v.exactPrefix, true, evalCtx)
	fwdMatch, fwdOrderCols := analyzeOrdering(fwdIndexProps)
	revMatch, revOrderCols := analyzeOrdering(revIndexProps)

	if fwdOrderCols != revOrderCols {
		panic(fmt.Sprintf("fwdOrderCols(%d) != revOrderCols(%d)", fwdOrderCols, revOrderCols))
	}

	orderCols := fwdOrderCols

	// Weigh the cost by how much of the ordering matched.
	//
	// TODO(pmattis): Need to determine the relative weight for index selection
	// based on sorting vs index selection based on filtering. Sorting is
	// expensive due to the need to buffer up the rows and perform the sort, but
	// not filtering is also expensive due to the larger number of rows scanned.
	match := fwdMatch
	if match < revMatch {
		match = revMatch
		v.reverse = true
	}
	weight := float64(orderCols+1) / float64(match+1)
	v.cost *= weight

	if match == orderCols && preferOrderMatching {
		// Offset the non-covering index cost penalty.
		v.cost *= (1.0 / nonCoveringIndexPenalty)
	}

	if log.V(2) {
		log.Infof(ctx, "%s: analyzeOrdering: weight=%0.2f reverse=%v match=%d",
			v.index.Name, weight, v.reverse, match)
	}
}

// isCoveringIndex returns true if all of the columns needed from the scanNode are contained within
// the index. This allows a scan of only the index to be performed without requiring subsequent
// lookup of the full row.
func (v *indexInfo) isCoveringIndex(scan *scanNode) bool {
	if v.index == &v.desc.PrimaryIndex {
		// The primary key index always covers all of the columns.
		return true
	}

	for _, colIdx := range scan.valNeededForCol.Ordered() {
		// This is possible during a schema change when we have
		// additional mutation columns.
		if colIdx >= len(v.desc.Columns) && len(v.desc.Mutations) > 0 {
			return false
		}
		colID := v.desc.Columns[colIdx].ID
		if !v.index.ContainsColumnID(colID) {
			return false
		}
	}
	return true
}

type indexInfoByCost []*indexInfo

func (v indexInfoByCost) Len() int {
	return len(v)
}

func (v indexInfoByCost) Less(i, j int) bool {
	return v[i].cost < v[j].cost
}

func (v indexInfoByCost) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}

func (v indexInfoByCost) Sort() {
	sort.Sort(v)
}

// makeIndexConstraints uses the opt code to generate index
// constraints. Initializes v.ic, as well as v.exactPrefix and v.cost (with a
// baseline cost for the index).
func (v *indexInfo) makeIndexConstraints(filter *opt.Expr, evalCtx *tree.EvalContext) error {
	numIndexCols := len(v.index.ColumnIDs)

	numExtraCols := 0
	isInverted := (v.index.Type == sqlbase.IndexDescriptor_INVERTED)
	// TODO(radu): we currently don't support index constraints on PK
	// columns on an inverted index.
	if !isInverted && !v.index.Unique {
		// We have a non-unique index; the extra columns are added to the key and we
		// can use them for index constraints.
		numExtraCols = len(v.index.ExtraColumnIDs)
	}

	colIdxMap := make(map[sqlbase.ColumnID]int, len(v.desc.Columns))
	for i := range v.desc.Columns {
		colIdxMap[v.desc.Columns[i].ID] = i
	}

	// Set up the IndexColumnInfo structures.
	colInfos := make([]opt.IndexColumnInfo, 0, numIndexCols+numExtraCols)
	for i := 0; i < numIndexCols+numExtraCols; i++ {
		var colID sqlbase.ColumnID
		var dir encoding.Direction

		if i < numIndexCols {
			colID = v.index.ColumnIDs[i]
			var err error
			dir, err = v.index.ColumnDirections[i].ToEncodingDirection()
			if err != nil {
				return err
			}
		} else {
			colID = v.index.ExtraColumnIDs[i-numIndexCols]
			// Extra columns are always ascending.
			dir = encoding.Ascending
		}

		idx, ok := colIdxMap[colID]
		if !ok {
			// Inactive column.
			break
		}

		colDesc := &v.desc.Columns[idx]
		colInfos = append(colInfos, opt.IndexColumnInfo{
			VarIdx:    idx,
			Typ:       colDesc.Type.ToDatumType(),
			Direction: dir,
			Nullable:  colDesc.Nullable,
		})
	}
	var spans opt.LogicalSpans
	var ok bool
	if filter != nil {
		v.ic.Init(filter, colInfos, isInverted, evalCtx)
		spans, ok = v.ic.Spans()
	}
	if !ok {
		// The index isn't being restricted at all, bump the cost significantly to
		// make any index which does restrict the keys more desirable.
		v.cost *= 1000
	} else {
		v.exactPrefix = opt.ExactPrefix(spans, evalCtx)
		// Find the number of columns that are restricted in all spans.
		numCols := len(colInfos)
		for _, sp := range spans {
			// Take the max between the length of the start values and the end
			// values.
			n := len(sp.Start.Vals)
			if n < len(sp.End.Vals) {
				n = len(sp.End.Vals)
			}
			// Take the minimum n across all spans.
			if numCols > n {
				numCols = n
			}
		}
		// Boost the cost by what fraction of columns have constraints. The higher
		// the fraction, the smaller the cost.
		v.cost *= float64((numIndexCols + numExtraCols)) / float64(numCols)
	}
	return nil
}

func unconstrainedSpans(
	tableDesc *sqlbase.TableDescriptor, index *sqlbase.IndexDescriptor,
) (roachpb.Spans, error) {
	return spansFromLogicalSpans(
		tableDesc, index, nil /* logicalSpans */, false, /* logicalSpansOk */
	)
}

// spansFromLogicalSpans converts op.LogicalSpans to roachpb.Spans.  interstices
// are pieces of the key that need to be inserted after each column (for
// interleavings).
func spansFromLogicalSpans(
	tableDesc *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	logicalSpans opt.LogicalSpans,
	logicalSpansOk bool,
) (roachpb.Spans, error) {
	interstices := make([][]byte, len(index.ColumnDirections)+len(index.ExtraColumnIDs)+1)
	interstices[0] = sqlbase.MakeIndexKeyPrefix(tableDesc, index.ID)
	if len(index.Interleave.Ancestors) > 0 {
		// TODO(eisen): too much of this code is copied from EncodePartialIndexKey.
		sharedPrefixLen := 0
		for i, ancestor := range index.Interleave.Ancestors {
			// The first ancestor is already encoded in interstices[0].
			if i != 0 {
				interstices[sharedPrefixLen] =
					encoding.EncodeUvarintAscending(interstices[sharedPrefixLen], uint64(ancestor.TableID))
				interstices[sharedPrefixLen] =
					encoding.EncodeUvarintAscending(interstices[sharedPrefixLen], uint64(ancestor.IndexID))
			}
			sharedPrefixLen += int(ancestor.SharedPrefixLen)
			interstices[sharedPrefixLen] = encoding.EncodeInterleavedSentinel(interstices[sharedPrefixLen])
		}
		interstices[sharedPrefixLen] =
			encoding.EncodeUvarintAscending(interstices[sharedPrefixLen], uint64(tableDesc.ID))
		interstices[sharedPrefixLen] =
			encoding.EncodeUvarintAscending(interstices[sharedPrefixLen], uint64(index.ID))
	}

	if !logicalSpansOk {
		// Encode a full span.
		sp, err := spanFromLogicalSpan(tableDesc, index, opt.MakeFullSpan(), interstices)
		if err != nil {
			return nil, err
		}
		return roachpb.Spans{sp}, nil
	}

	spans := make(roachpb.Spans, len(logicalSpans))
	for i, ls := range logicalSpans {
		s, err := spanFromLogicalSpan(tableDesc, index, ls, interstices)
		if err != nil {
			return nil, err
		}
		spans[i] = s
	}
	return spans, nil
}

// encodeLogicalKey encodes each logical part of a key into a
// roachpb.Key; interstices[i] is inserted before the i-th value.
func encodeLogicalKey(
	index *sqlbase.IndexDescriptor, vals tree.Datums, interstices [][]byte,
) (roachpb.Key, error) {
	var key roachpb.Key
	for i, val := range vals {
		key = append(key, interstices[i]...)

		var err error
		// For extra columns (like implicit columns), the direction
		// is ascending.
		dir := encoding.Ascending
		if i < len(index.ColumnDirections) {
			dir, err = index.ColumnDirections[i].ToEncodingDirection()
			if err != nil {
				return nil, err
			}
		}

		if index.Type == sqlbase.IndexDescriptor_INVERTED {
			keys, err := sqlbase.EncodeInvertedIndexTableKeys(val, key)
			if err != nil {
				return nil, err
			}
			if len(keys) > 1 {
				err := pgerror.NewError(
					pgerror.CodeInternalError, "trying to use multiple keys in index lookup",
				)
				return nil, err
			}
			if len(keys) < 1 {
				err := pgerror.NewError(
					pgerror.CodeInternalError, "can't look up empty JSON",
				)
				return nil, err
			}
			key = keys[0]
		} else {
			key, err = sqlbase.EncodeTableKey(key, val, dir)
			if err != nil {
				return nil, err
			}
		}
	}
	return key, nil
}

// spanFromLogicalSpan converts an opt.LogicalSpan to a
// roachpb.Span.
func spanFromLogicalSpan(
	tableDesc *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	ls opt.LogicalSpan,
	interstices [][]byte,
) (roachpb.Span, error) {
	var s roachpb.Span
	var err error
	// Encode each logical part of the start key.
	s.Key, err = encodeLogicalKey(index, ls.Start.Vals, interstices)
	if err != nil {
		return roachpb.Span{}, err
	}
	if ls.Start.Inclusive {
		s.Key = append(s.Key, interstices[len(ls.Start.Vals)]...)
	} else {
		// We need to exclude the value this logical part refers to.
		s.Key = s.Key.PrefixEnd()
	}
	// Encode each logical part of the end key.
	s.EndKey, err = encodeLogicalKey(index, ls.End.Vals, interstices)
	if err != nil {
		return roachpb.Span{}, err
	}
	s.EndKey = append(s.EndKey, interstices[len(ls.End.Vals)]...)

	// We tighten the end key to prevent reading interleaved children after the
	// last parent key. If ls.End.Inclusive is true, we also advance the key as
	// necessary.
	s.EndKey, err = sqlbase.AdjustEndKeyForInterleave(tableDesc, index, s.EndKey, ls.End.Inclusive)
	if err != nil {
		return roachpb.Span{}, err
	}

	return s, nil
}
