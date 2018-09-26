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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/idxconstraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
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

	var optimizer xform.Optimizer

	if s.filter != nil {
		optimizer.Init(p.EvalContext())
		md := optimizer.Memo().Metadata()
		for i := range s.resultColumns {
			md.AddColumn(s.resultColumns[i].Name, s.resultColumns[i].Typ)
		}
		bld := optbuilder.NewScalar(ctx, &p.semaCtx, p.EvalContext(), optimizer.Factory())
		bld.AllowUnsupportedExpr = true
		err := bld.Build(s.filter)
		if err != nil {
			return nil, err
		}
		filterExpr := optimizer.Memo().Root()
		for _, c := range candidates {
			if err := c.makeIndexConstraints(
				&optimizer, filterExpr, p.EvalContext(),
			); err != nil {
				return nil, err
			}
			if c.ic.Constraint().IsContradiction() {
				// No spans (i.e. the filter is always false). Note that if a filter
				// results in no constraints, ok would be false.
				return newZeroNode(s.resultColumns), nil
			}
		}
	}

	// Remove any inverted indexes that don't generate any spans, a full-scan of
	// an inverted index is always invalid.
	for i := 0; i < len(candidates); {
		c := candidates[i].ic.Constraint()
		if candidates[i].index.Type == sqlbase.IndexDescriptor_INVERTED && (c == nil || c.IsUnconstrained()) {
			candidates[i] = candidates[len(candidates)-1]
			candidates = candidates[:len(candidates)-1]
		} else {
			i++
		}
	}

	if len(candidates) == 0 {
		// The primary index is never inverted. So the only way this can happen is
		// if we had a specified index.
		if s.specifiedIndex == nil {
			panic("no non-inverted indexes")
		}
		return nil, fmt.Errorf("index \"%s\" is inverted and cannot be used for this query",
			s.specifiedIndex.Name)
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
			log.Infof(
				ctx, "%d: selectIndex(%s): cost=%v constraint=%s reverse=%t",
				i, c.index.Name, c.cost, c.ic.Constraint(), c.reverse,
			)
		}
	}

	// After sorting, candidates[0] contains the best index. Copy its info into
	// the scanNode.
	c := candidates[0]
	s.index = c.index
	s.specifiedIndex = nil
	s.run.isSecondaryIndex = (c.index != &s.desc.PrimaryIndex)

	var err error
	s.spans, err = spansFromConstraint(s.desc, c.index, c.ic.Constraint(), s.valNeededForCol)
	if err != nil {
		return nil, errors.Wrapf(
			err, "constraint = %s, table ID = %d, index ID = %d",
			c.ic.Constraint(), s.desc.ID, s.index.ID,
		)
	}

	if len(s.spans) == 0 {
		// There are no spans to scan.
		return newZeroNode(s.resultColumns), nil
	}

	s.origFilter = s.filter
	if s.filter != nil {
		remGroup := c.ic.RemainingFilter()
		remEv := memo.MakeNormExprView(optimizer.Memo(), remGroup)
		if remEv.Operator() == opt.TrueOp {
			s.filter = nil
		} else {
			execBld := execbuilder.New(nil /* execFactory */, remEv, nil /* evalCtx */)
			s.filter, err = execBld.BuildScalar(&s.filterVars)
			if err != nil {
				return nil, err
			}
		}
	}
	s.filterVars.Rebind(s.filter, true, false)

	s.reverse = c.reverse

	var plan planNode
	if c.covering {
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

	ic idxconstraint.Instance
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
	if v.index.Type == sqlbase.IndexDescriptor_INVERTED {
		return false
	}
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
func (v *indexInfo) makeIndexConstraints(
	optimizer *xform.Optimizer, filter memo.ExprView, evalCtx *tree.EvalContext,
) error {
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

	columns := make([]opt.OrderingColumn, 0, numIndexCols+numExtraCols)
	var notNullCols opt.ColSet
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

		col := opt.MakeOrderingColumn(opt.ColumnID(idx+1), dir == encoding.Descending)
		columns = append(columns, col)
		if !v.desc.Columns[idx].Nullable {
			notNullCols.Add(idx + 1)
		}
	}
	v.ic.Init(filter, columns, notNullCols, isInverted, evalCtx, optimizer.Factory())
	idxConstraint := v.ic.Constraint()
	if idxConstraint.IsUnconstrained() {
		// The index isn't being restricted at all, bump the cost significantly to
		// make any index which does restrict the keys more desirable.
		v.cost *= 1000
	} else {
		v.exactPrefix = idxConstraint.ExactPrefix(evalCtx)
		// Find the number of columns that are restricted in all spans.
		numCols := len(columns)
		for i := 0; i < idxConstraint.Spans.Count(); i++ {
			sp := idxConstraint.Spans.Get(i)
			// Take the max between the length of the start values and the end
			// values.
			n := sp.StartKey().Length()
			if e := sp.EndKey().Length(); n < e {
				n = e
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
	return spansFromConstraint(tableDesc, index, nil, exec.ColumnOrdinalSet{})
}

// spansFromConstraint converts the spans in a Constraint to roachpb.Spans.
//
// interstices are pieces of the key that need to be inserted after each column
// (for interleavings).
func spansFromConstraint(
	tableDesc *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	c *constraint.Constraint,
	needed exec.ColumnOrdinalSet,
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

	if c == nil || c.IsUnconstrained() {
		// Encode a full span.
		sp, err := spansFromConstraintSpan(
			tableDesc, index, &constraint.UnconstrainedSpan, interstices, needed)
		if err != nil {
			return nil, err
		}
		return sp, nil
	}

	spans := make(roachpb.Spans, 0, c.Spans.Count())
	for i := 0; i < c.Spans.Count(); i++ {
		s, err := spansFromConstraintSpan(tableDesc, index, c.Spans.Get(i), interstices, needed)
		if err != nil {
			return nil, err
		}
		spans = append(spans, s...)
	}
	return spans, nil
}

// encodeConstraintKey encodes each logical part of a constraint.Key into a
// roachpb.Key; interstices[i] is inserted before the i-th value.
func encodeConstraintKey(
	index *sqlbase.IndexDescriptor, ck constraint.Key, interstices [][]byte,
) (roachpb.Key, error) {
	var key roachpb.Key
	for i := 0; i < ck.Length(); i++ {
		val := ck.Value(i)
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

// spansFromConstraintSpan converts a constraint.Span to one or more
// roachpb.Spans. It returns multiple spans in the case that multiple,
// non-adjacent column families should be scanned.
func spansFromConstraintSpan(
	tableDesc *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	cs *constraint.Span,
	interstices [][]byte,
	needed exec.ColumnOrdinalSet,
) (roachpb.Spans, error) {
	var s roachpb.Span
	var err error
	// Encode each logical part of the start key.
	s.Key, err = encodeConstraintKey(index, cs.StartKey(), interstices)
	if err != nil {
		return nil, err
	}
	if cs.StartBoundary() == constraint.IncludeBoundary {
		s.Key = append(s.Key, interstices[cs.StartKey().Length()]...)
	} else {
		// We need to exclude the value this logical part refers to.
		s.Key = s.Key.PrefixEnd()
	}
	// Encode each logical part of the end key.
	s.EndKey, err = encodeConstraintKey(index, cs.EndKey(), interstices)
	if err != nil {
		return nil, err
	}
	s.EndKey = append(s.EndKey, interstices[cs.EndKey().Length()]...)

	// Optimization: for single row lookups on a table with multiple column
	// families, only scan the relevant column families.
	if needed.Len() > 0 &&
		index.ID == tableDesc.PrimaryIndex.ID &&
		len(tableDesc.Families) > 1 &&
		cs.StartKey().Length() == len(tableDesc.PrimaryIndex.ColumnIDs) &&
		s.Key.Equal(s.EndKey) {
		neededFamilyIDs := neededColumnFamilyIDs(tableDesc, needed)
		if len(neededFamilyIDs) < len(tableDesc.Families) {
			spans := make(roachpb.Spans, 0, len(neededFamilyIDs))
			for i, familyID := range neededFamilyIDs {
				var span roachpb.Span
				span.Key = make(roachpb.Key, len(s.Key))
				copy(span.Key, s.Key)
				span.Key = keys.MakeFamilyKey(span.Key, uint32(familyID))
				span.EndKey = span.Key.PrefixEnd()
				if i > 0 && familyID == neededFamilyIDs[i-1]+1 {
					// This column family is adjacent to the previous one. We can merge
					// the two spans into one.
					spans[len(spans)-1].EndKey = span.EndKey
				} else {
					spans = append(spans, span)
				}
			}
			return spans, nil
		}
	}

	// We tighten the end key to prevent reading interleaved children after the
	// last parent key. If cs.End.Inclusive is true, we also advance the key as
	// necessary.
	endInclusive := cs.EndBoundary() == constraint.IncludeBoundary
	s.EndKey, err = sqlbase.AdjustEndKeyForInterleave(tableDesc, index, s.EndKey, endInclusive)
	if err != nil {
		return nil, err
	}
	return roachpb.Spans{s}, nil
}

func neededColumnFamilyIDs(
	tableDesc *sqlbase.TableDescriptor, neededCols exec.ColumnOrdinalSet,
) []sqlbase.FamilyID {
	colIdxMap := tableDesc.ColumnIdxMap()

	var needed []sqlbase.FamilyID
	for _, family := range tableDesc.Families {
		for _, columnID := range family.ColumnIDs {
			columnOrdinal := colIdxMap[columnID]
			if neededCols.Contains(columnOrdinal) {
				needed = append(needed, family.ID)
				break
			}
		}
	}

	// TODO(solon): There is a further optimization possible here: if there is at
	// least one non-nullable column in the needed column families, we can
	// potentially omit the primary family, since the primary keys are encoded
	// in all families.

	return needed
}
