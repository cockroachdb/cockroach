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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"bytes"
	"fmt"
	"sort"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
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
type analyzeOrderingFn func(indexOrdering orderingInfo) (matchingCols, totalCols int)

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
		s.initOrdering(0)
		return s, nil
	}

	if s.filter == nil && analyzeOrdering == nil && s.specifiedIndex == nil {
		// No where-clause, no ordering, and no specified index.
		s.initOrdering(0)
		var err error
		s.spans, err = makeSpans(nil, &s.desc, s.index)
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
			desc:  &s.desc,
			index: s.specifiedIndex,
		})
	} else {
		candidates = append(candidates, &indexInfo{
			desc:  &s.desc,
			index: &s.desc.PrimaryIndex,
		})
		for i := range s.desc.Indexes {
			candidates = append(candidates, &indexInfo{
				desc:  &s.desc,
				index: &s.desc.Indexes[i],
			})
		}
	}

	for _, c := range candidates {
		c.init(s)
	}

	if s.filter != nil {
		// Analyze the filter expression, simplifying it and splitting it up into
		// possibly overlapping ranges.
		exprs, equivalent := analyzeExpr(&p.evalCtx, s.filter)
		if log.V(2) {
			log.Infof(ctx, "analyzeExpr: %s -> %s [equivalent=%v]", s.filter, exprs, equivalent)
		}

		// Check to see if the filter simplified to a constant.
		if len(exprs) == 1 && len(exprs[0]) == 1 {
			if d, ok := exprs[0][0].(*parser.DBool); ok && bool(!*d) {
				// The expression simplified to false.
				return &emptyNode{}, nil
			}
		}

		// If the simplified expression is equivalent and there is a single
		// disjunction, use it for the filter instead of the original expression.
		if equivalent && len(exprs) == 1 {
			s.filter = joinAndExprs(exprs[0])
		}

		// TODO(pmattis): If "len(exprs) > 1" then we have multiple disjunctive
		// expressions. For example, "a <= 1 OR a >= 5" will get translated into
		// "[[a <= 1], [a >= 5]]".
		//
		// We currently map all disjunctions onto the same index; this works
		// well if we can derive constraints for a set of columns from all
		// disjunctions, e.g. `a < 5 OR a > 10`.
		//
		// However, we can't generate any constraints if the disjunctions refer
		// to different columns, e.g. `a > 1 OR b > 1`. We would need to perform
		// index selection independently for each of the disjunctive
		// expressions, and we would need infrastructure to do a
		// multi-index-join. There are complexities: if there are a large
		// number of disjunctive expressions we should limit how many indexes we
		// use.

		for _, c := range candidates {
			c.analyzeExprs(exprs)
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

	if analyzeOrdering != nil {
		for _, c := range candidates {
			c.analyzeOrdering(ctx, s, analyzeOrdering, preferOrderMatching)
		}
	}

	indexInfoByCost(candidates).Sort()

	if log.V(2) {
		for i, c := range candidates {
			log.Infof(ctx, "%d: selectIndex(%s): cost=%v constraints=%s reverse=%t",
				i, c.index.Name, c.cost, c.constraints, c.reverse)
		}
	}

	// After sorting, candidates[0] contains the best index. Copy its info into
	// the scanNode.
	c := candidates[0]
	s.index = c.index
	s.specifiedIndex = nil
	s.isSecondaryIndex = (c.index != &s.desc.PrimaryIndex)
	var err error
	s.spans, err = makeSpans(c.constraints, c.desc, c.index)
	if err != nil {
		return nil, errors.Wrapf(err, "constraints = %v, table ID = %d, index ID = %d",
			c.constraints, s.desc.ID, s.index.ID)
	}
	if len(s.spans) == 0 {
		// There are no spans to scan.
		return &emptyNode{}, nil
	}

	s.filter = applyIndexConstraints(&p.evalCtx, s.filter, c.constraints)
	if s.filter != nil {
		// Constraint propagation may have produced new constant sub-expressions.
		// Propagate them and check if s.filter can be applied prematurely.
		var err error
		s.filter, err = p.evalCtx.NormalizeExpr(s.filter)
		if err != nil {
			return nil, err
		}
		if s.filter == parser.DBoolFalse {
			return &emptyNode{}, nil
		}
		if s.filter == parser.DBoolTrue {
			s.filter = nil
		}
	}

	s.reverse = c.reverse

	var plan planNode
	if c.covering {
		s.initOrdering(c.exactPrefix)
		plan = s
	} else {
		// Note: makeIndexJoin destroys s and returns a new index scan
		// node. The filter in that node may be different from the
		// original table filter.
		plan, s = s.p.makeIndexJoin(s, c.exactPrefix)
	}

	if log.V(3) {
		log.Infof(ctx, "%s: filter=%v", c.index.Name, s.filter)
		for i, span := range s.spans {
			log.Infof(ctx, "%s/%d: %s", c.index.Name, i, sqlbase.PrettySpan(span, 2))
		}
	}

	return plan, nil
}

type indexConstraint struct {
	start *parser.ComparisonExpr
	end   *parser.ComparisonExpr
	// tupleMap is an ordering of the tuples within a tuple comparison such that
	// they match the ordering within the index. For example, an index on the
	// columns (a, b) and a tuple comparison "(b, a) = (1, 2)" would have a
	// tupleMap of {1, 0} indicating that the first column to be encoded is the
	// second element of the tuple. The tuple map may be shorter than the length
	// of the tuple. For example, if the index was only on (a), then the tupleMap
	// would be {1}.
	tupleMap []int
}

// numColumns returns the number of columns this constraint applies to. Only
// constraints for expressions with tuple comparisons apply to multiple columns.
func (ic indexConstraint) numColumns() int {
	if ic.tupleMap == nil {
		return 1
	}
	return len(ic.tupleMap)
}

func (ic indexConstraint) String() string {
	var buf bytes.Buffer
	if ic.start != nil {
		fmt.Fprintf(&buf, "%s", ic.start)
	}
	if ic.end != nil && ic.end != ic.start {
		if ic.start != nil {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "%s", ic.end)
	}
	return buf.String()
}

// indexConstraints is a set of constraints on a prefix of the columns
// in a single index. The constraints are ordered as the columns in the index.
// A constraint referencing a tuple accounts for several columns (the size of
// its .tupleMap).
type indexConstraints []indexConstraint

func (ic indexConstraints) String() string {
	var buf bytes.Buffer
	buf.WriteString("[")
	for i := range ic {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(ic[i].String())
	}
	buf.WriteString("]")
	return buf.String()
}

// orIndexConstraints stores multiple indexConstraints, one for each top level
// disjunction in our filtering expression. Each indexConstraints element
// generates a set of spans; these sets of spans are merged.
type orIndexConstraints []indexConstraints

func (oic orIndexConstraints) String() string {
	var buf bytes.Buffer
	for i := range oic {
		if i > 0 {
			buf.WriteString(" OR ")
		}
		buf.WriteString(oic[i].String())
	}
	return buf.String()
}

type indexInfo struct {
	desc        *sqlbase.TableDescriptor
	index       *sqlbase.IndexDescriptor
	constraints orIndexConstraints
	cost        float64
	covering    bool // Does the index cover the required IndexedVars?
	reverse     bool
	exactPrefix int
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

// analyzeExprs examines the range map to determine the cost of using the
// index.
func (v *indexInfo) analyzeExprs(exprs []parser.TypedExprs) {
	if err := v.makeOrConstraints(exprs); err != nil {
		panic(err)
	}

	// Count the number of elements used to limit the start and end keys. We then
	// boost the cost by what fraction of the index keys are being used. The
	// higher the fraction, the lower the cost.
	if len(v.constraints) == 0 {
		// The index isn't being restricted at all, bump the cost significantly to
		// make any index which does restrict the keys more desirable.
		v.cost *= 1000
	} else {
		// When we have multiple indexConstraints, each one is for a top-level
		// disjunction (OR); together they are no more restrictive than any one of
		// them. We thus calculate the cost based on the disjunction which restricts
		// the smallest number of columns.
		//
		// TODO(radu): we need to be more discriminating - constraints such as
		// "NOT NULL" are almost as useless as no constraints, whereas "exact value"
		// constraints are very restrictive.
		minNumCols := len(v.index.ColumnIDs)
		for _, cset := range v.constraints {
			numCols := 0
			for _, c := range cset {
				numCols += c.numColumns()
			}
			if numCols < minNumCols {
				minNumCols = numCols
			}
		}
		v.cost *= float64(len(v.index.ColumnIDs)) / float64(minNumCols)
	}
}

// analyzeOrdering analyzes the ordering provided by the index and determines
// if it matches the ordering requested by the query. Non-matching orderings
// increase the cost of using the index.
//
// If preferOrderMatching is true, we prefer an index that matches the desired
// ordering completely, even if it is not a covering index.
func (v *indexInfo) analyzeOrdering(
	ctx context.Context, scan *scanNode, analyzeOrdering analyzeOrderingFn, preferOrderMatching bool,
) {
	// Compute the prefix of the index for which we have exact constraints. This
	// prefix is inconsequential for ordering because the values are identical.
	v.exactPrefix = v.constraints.exactPrefix(&scan.p.evalCtx)

	// Analyze the ordering provided by the index (either forward or reverse).
	fwdIndexOrdering := scan.computeOrdering(v.index, v.exactPrefix, false)
	revIndexOrdering := scan.computeOrdering(v.index, v.exactPrefix, true)
	fwdMatch, fwdOrderCols := analyzeOrdering(fwdIndexOrdering)
	revMatch, revOrderCols := analyzeOrdering(revIndexOrdering)

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

// getColVarIdx detects whether an expression is a straightforward
// reference to a column or index variable. In this case it returns
// the index of that column's in the descriptor's []Column array.
// Used by indexInfo.makeIndexConstraints().
func getColVarIdx(expr parser.Expr) (ok bool, colIdx int) {
	switch q := expr.(type) {
	case *parser.IndexedVar:
		return true, q.Idx
	}
	return false, -1
}

// makeOrConstraints populates the indexInfo.constraints field based on the
// analyzed expressions. Each element of constraints corresponds to one
// of the top-level disjunctions and is generated using makeIndexConstraint.
func (v *indexInfo) makeOrConstraints(orExprs []parser.TypedExprs) error {
	constraints := make(orIndexConstraints, len(orExprs))
	for i, e := range orExprs {
		var err error
		constraints[i], err = v.makeIndexConstraints(e)
		if err != nil {
			return err
		}
		// If an OR branch has no constraints, we cannot have _any_
		// constraints.
		if len(constraints[i]) == 0 {
			return nil
		}
	}

	v.constraints = constraints
	return nil
}

// makeIndexConstraints generates constraints for a set of conjunctions (AND
// expressions). These expressions can be the entire filter, or they can be one
// of multiple top-level disjunctions (ORs).
//
// The constraints consist of start and end expressions for a prefix of the
// columns that make up the index. For example, consider the expression
// "a >= 1 AND b >= 2":
//
//   {a: {start: >= 1}, b: {start: >= 2}}
//
// This method generates one indexConstraint for a prefix of the columns in
// the index (except for tuple constraints which can account for more than
// one column). A prefix of the generated constraints has a .start, and
// similarly a prefix of the constraints has a .end (in other words,
// once a constraint doesn't have a .start, no further constraints will
// have one). This is because they wouldn't be useful when generating spans.
//
// makeIndexConstraints takes into account the direction of the columns in the
// index.  For ascending cols, start constraints look for comparison expressions
// with the operators >, >=, = or IN and end constraints look for comparison
// expressions with the operators <, <=, = or IN. Vice versa for descending
// cols.
//
// Whenever possible, < and > are converted to <= and >=, respectively.
// This is because we can use inclusive constraints better than exclusive ones;
// with inclusive constraints we can continue to accumulate constraints for
// next columns. Not so with exclusive ones: Consider "a < 1 AND b < 2".
// "a < 1" will be encoded as an exclusive span end; if we were to append
// anything about "b" to it, that would be incorrect.
// Note that it's not always possible to transform ">" to ">=", because some
// types do not support the Next() operation. Similarly, it is not always possible
// to transform "<" to "<=", because some types do not support the Prev() operation.
// So, the resulting constraints might contain ">" or "<" (depending on encoding
// direction), in which case that will be the last constraint with `.end` filled.
//
// TODO(pmattis): It would be more obvious to perform this transform in
// simplifyComparisonExpr, but doing so there eliminates some of the other
// simplifications. For example, "a < 1 OR a > 1" currently simplifies to "a !=
// 1", but if we performed this transform in simplifyComparisonExpr it would
// simplify to "a < 1 OR a >= 2" which is also the same as "a != 1", but not so
// obvious based on comparisons of the constants.
func (v *indexInfo) makeIndexConstraints(andExprs parser.TypedExprs) (indexConstraints, error) {
	var constraints indexConstraints

	trueStartDone := false
	trueEndDone := false

	for i := 0; i < len(v.index.ColumnIDs); i++ {
		colID := v.index.ColumnIDs[i]
		var colDir encoding.Direction
		var err error
		if colDir, err = v.index.ColumnDirections[i].ToEncodingDirection(); err != nil {
			return nil, err
		}

		var constraint indexConstraint
		// We're going to fill in that start and end of the constraint
		// by indirection, which keeps in mind the direction of the
		// column's encoding in the index.
		// This allows us to produce direction-aware constraints, but
		// still have the code below be intuitive (e.g. treat ">" always as
		// a start constraint).
		startExpr := &constraint.start
		endExpr := &constraint.end
		startDone := &trueStartDone
		endDone := &trueEndDone
		if colDir == encoding.Descending {
			// For descending index cols, c.start is an end constraint
			// and c.end is a start constraint.
			startExpr = &constraint.end
			endExpr = &constraint.start
			startDone = &trueEndDone
			endDone = &trueStartDone
		}

	exprLoop:
		for _, e := range andExprs {
			if c, ok := e.(*parser.ComparisonExpr); ok {
				var tupleMap []int

				if ok, colIdx := getColVarIdx(c.Left); ok && v.desc.Columns[colIdx].ID != colID {
					// This expression refers to a column other than the one we're
					// looking for.
					continue
				}

				if _, ok := c.Right.(parser.Datum); !ok {
					continue
				}

				if t, ok := c.Left.(*parser.Tuple); ok {
					// If we have a tuple comparison we need to rearrange the comparison
					// so that the order of the columns in the tuple matches the order in
					// the index. For example, for an index on (a, b), the tuple
					// comparison "(b, a) = (1, 2)" would be rewritten as "(a, b) = (2,
					// 1)". Note that we don't actually need to rewrite the comparison,
					// but simply provide a mapping from the order in the tuple to the
					// order in the index.
					for _, colID := range v.index.ColumnIDs[i:] {
						idx := -1
						for i, val := range t.Exprs {
							ok, colIdx := getColVarIdx(val)
							if ok && v.desc.Columns[colIdx].ID == colID {
								idx = i
								break
							}
						}
						if idx == -1 {
							break
						}
						tupleMap = append(tupleMap, idx)
					}
					if len(tupleMap) == 0 {
						// This tuple does not contain the column we're looking for.
						continue
					}
					// Skip all the next columns covered by this tuple.
					i += (len(tupleMap) - 1)
					if c.Operator != parser.In {
						// Make sure all columns specified in the tuple are in the index.
						// TODO(mjibson): support prefixes: (a,b,c) > (1,2,3) -> (a,b) >= (1,2)
						if len(t.Exprs) > len(tupleMap) {
							continue
						}
						// Since tuples comparison is lexicographic, we only support it if the tuple
						// is in the same order as the index.
						for i, v := range tupleMap {
							if i != v {
								continue exprLoop
							}
						}
						// Due to the implementation of the encoding functions, it is currently
						// difficult to support indexes of varying directions with tuples. For now,
						// restrict them to a single direction. See #6346.
						dir := v.index.ColumnDirections[i]
						for _, ti := range tupleMap {
							if dir != v.index.ColumnDirections[ti] {
								continue exprLoop
							}
						}
					}
					constraint.tupleMap = tupleMap
				}

				preStart := *startExpr
				preEnd := *endExpr
				switch c.Operator {
				case parser.EQ:
					// An equality constraint will overwrite any other type
					// of constraint.
					if !*startDone {
						*startExpr = c
					}
					if !*endDone {
						*endExpr = c
					}
				case parser.NE:
					// We rewrite "a != x" to "a IS NOT NULL", since this is all that
					// makeSpans() cares about.
					// We don't simplify "a != x" to "a IS NOT NULL" in
					// simplifyExpr because doing so affects other simplifications.
					if *startDone || *startExpr != nil {
						continue
					}
					*startExpr = parser.NewTypedComparisonExpr(
						parser.IsNot,
						c.TypedLeft(),
						parser.DNull,
					)
				case parser.In:
					// Only allow the IN constraint if the previous constraints are all
					// EQ. This is necessary to prevent overlapping spans from being
					// generated. Consider the constraints [a >= 1, a <= 2, b IN (1,
					// 2)]. This would turn into the spans /1/1-/3/2 and /1/2-/3/3.
					ok := true
					for _, c := range constraints {
						ok = ok && (c.start == c.end) && (c.start.Operator == parser.EQ)
					}
					if !ok {
						continue
					}

					if !*startDone && (*startExpr == nil || (*startExpr).Operator != parser.EQ) {
						*startExpr = c
					}
					if !*endDone && (*endExpr == nil || (*endExpr).Operator != parser.EQ) {
						*endExpr = c
					}
				case parser.GE:
					if !*startDone && *startExpr == nil {
						*startExpr = c
					}
				case parser.GT:
					// Transform ">" into ">=".
					if *startDone || (*startExpr != nil) {
						continue
					}
					if c.Right.(parser.Datum).IsMax() {
						*startExpr = parser.NewTypedComparisonExpr(
							parser.EQ,
							c.TypedLeft(),
							c.TypedRight(),
						)
					} else if nextRightVal, hasNext := c.Right.(parser.Datum).Next(); hasNext {
						*startExpr = parser.NewTypedComparisonExpr(
							parser.GE,
							c.TypedLeft(),
							nextRightVal,
						)
					} else {
						*startExpr = c
					}
				case parser.LT:
					if *endDone || (*endExpr != nil) {
						continue
					}
					// Transform "<" into "<=".
					if c.Right.(parser.Datum).IsMin() {
						*endExpr = parser.NewTypedComparisonExpr(
							parser.EQ,
							c.TypedLeft(),
							c.TypedRight(),
						)
					} else if prevRightVal, hasPrev := c.Right.(parser.Datum).Prev(); hasPrev {
						*endExpr = parser.NewTypedComparisonExpr(
							parser.LE,
							c.TypedLeft(),
							prevRightVal,
						)
					} else {
						*endExpr = c
					}
				case parser.LE:
					if !*endDone && *endExpr == nil {
						*endExpr = c
					}
				case parser.Is:
					if c.Right == parser.DNull && !*endDone {
						*endExpr = c
					}
				case parser.IsNot:
					if c.Right == parser.DNull && !*startDone && (*startExpr == nil) {
						*startExpr = c
					}
				}

				// If a new constraint includes a mixed-type comparison expression,
				// we can not include it in the index constraints because the index
				// encoding would be incorrect. See #4313.
				if preStart != *startExpr {
					if (*startExpr).IsMixedTypeComparison() {
						*startExpr = nil
					}
				}
				if preEnd != *endExpr {
					if (*endExpr).IsMixedTypeComparison() {
						*endExpr = nil
					}
				}
			}
		}

		if *endExpr != nil && (*endExpr).Operator == parser.LT {
			*endDone = true
		}

		if !*startDone && *startExpr == nil {
			// Add an IS NOT NULL constraint if there's an end constraint.
			if (*endExpr != nil) &&
				!((*endExpr).Operator == parser.Is && (*endExpr).Right == parser.DNull) {
				*startExpr = parser.NewTypedComparisonExpr(
					parser.IsNot,
					(*endExpr).TypedLeft(),
					parser.DNull,
				)
			}
		}

		if (*startExpr == nil) ||
			(((*startExpr).Operator == parser.IsNot) && ((*startExpr).Right == parser.DNull)) {
			// There's no point in allowing future start constraints after an IS NOT NULL
			// one; since NOT NULL is not actually a value present in an index,
			// values encoded after an NOT NULL don't matter.
			*startDone = true
		}

		if constraint.start != nil || constraint.end != nil {
			constraints = append(constraints, constraint)
		}

		if *endExpr == nil {
			*endDone = true
		}
		if *startDone && *endDone {
			// The rest of the expressions don't matter; when we construct index spans
			// based on these constraints we won't be able to accumulate more in either
			// the start key prefix nor the end key prefix.
			break
		}
	}
	return constraints, nil
}

// isCoveringIndex returns true if all of the columns needed from the scanNode are contained within
// the index. This allows a scan of only the index to be performed without requiring subsequent
// lookup of the full row.
func (v *indexInfo) isCoveringIndex(scan *scanNode) bool {
	if v.index == &v.desc.PrimaryIndex {
		// The primary key index always covers all of the columns.
		return true
	}

	for i, needed := range scan.valNeededForCol {
		if needed {
			colID := v.desc.Columns[i].ID
			if !v.index.ContainsColumnID(colID) {
				return false
			}
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

func encodeStartConstraintAscending(c *parser.ComparisonExpr) logicalKeyPart {
	switch c.Operator {
	case parser.IsNot:
		// A IS NOT NULL expression allows us to constrain the start of
		// the range to not include NULL.
		if c.Right != parser.DNull {
			panic(fmt.Sprintf("expected NULL operand for IS NOT operator, found %v", c.Right))
		}
		return logicalKeyPart{
			val:       parser.DNull,
			dir:       encoding.Ascending,
			inclusive: false,
		}
	case parser.NE:
		panic("'!=' operators should have been transformed to 'IS NOT NULL'")
	case parser.GE, parser.EQ, parser.GT:
		return logicalKeyPart{
			val:       c.Right.(parser.Datum),
			dir:       encoding.Ascending,
			inclusive: c.Operator != parser.GT,
		}
	default:
		panic(fmt.Sprintf("unexpected operator: %s", c))
	}
}

func encodeStartConstraintDescending(c *parser.ComparisonExpr) logicalKeyPart {
	switch c.Operator {
	case parser.Is:
		// An IS NULL expressions allows us to constrain the start of the range
		// to begin at NULL.
		if c.Right != parser.DNull {
			panic(fmt.Sprintf("expected NULL operand for IS operator, found %v", c.Right))
		}
		return logicalKeyPart{
			val:       parser.DNull,
			dir:       encoding.Descending,
			inclusive: true,
		}
	case parser.NE:
		panic("'!=' operators should have been transformed to 'IS NOT NULL'")
	case parser.LE, parser.EQ, parser.LT:
		return logicalKeyPart{
			val:       c.Right.(parser.Datum),
			dir:       encoding.Descending,
			inclusive: c.Operator != parser.LT,
		}
	default:
		panic(fmt.Sprintf("unexpected operator: %s", c))
	}
}

func encodeEndConstraintAscending(c *parser.ComparisonExpr) logicalKeyPart {
	switch c.Operator {
	case parser.Is:
		// An IS NULL expressions allows us to constrain the end of the range
		// to stop at NULL.
		if c.Right != parser.DNull {
			panic(fmt.Sprintf("expected NULL operand for IS operator, found %v", c.Right))
		}
		return logicalKeyPart{
			val:       parser.DNull,
			dir:       encoding.Ascending,
			inclusive: true,
		}
	case parser.NE:
		panic("'!=' operators should have been transformed to 'IS NOT NULL'")
	case parser.LE, parser.EQ, parser.LT:
		return logicalKeyPart{
			val:       c.Right.(parser.Datum),
			dir:       encoding.Ascending,
			inclusive: c.Operator != parser.LT,
		}
	default:
		panic(fmt.Sprintf("unexpected operator: %s", c))
	}
}

func encodeEndConstraintDescending(c *parser.ComparisonExpr) logicalKeyPart {
	switch c.Operator {
	case parser.IsNot:
		// An IS NOT NULL expressions allows us to constrain the end of the range
		// to stop at NULL.
		if c.Right != parser.DNull {
			panic(fmt.Sprintf("expected NULL operand for IS NOT operator, found %v", c.Right))
		}
		return logicalKeyPart{
			val:       parser.DNull,
			dir:       encoding.Descending,
			inclusive: false,
		}
	case parser.NE:
		panic("'!=' operators should have been transformed to 'IS NOT NULL'")
	case parser.GE, parser.EQ, parser.GT:
		return logicalKeyPart{
			val:       c.Right.(parser.Datum),
			dir:       encoding.Descending,
			inclusive: c.Operator != parser.GT,
		}
	default:
		panic(fmt.Sprintf("unexpected operator: %s", c))
	}
}

// Splits spans according to a constraint like (...) in <tuple>.
// If the constraint is (a,b) IN ((1,2),(3,4)), each input span
// will be split into two: the first one will have "1/2" appended to
// the start and/or end, the second one will have "3/4" appended to
// the start and/or end.
//
// Returns the exploded spans.
func applyInConstraint(
	spans []logicalSpan, c indexConstraint, firstCol int, index *sqlbase.IndexDescriptor,
) ([]logicalSpan, error) {
	var e *parser.ComparisonExpr
	// It might be that the IN constraint is a start constraint, an
	// end constraint, or both, depending on how whether we had
	// start and end constraints for all the previous index cols.
	if c.start != nil && c.start.Operator == parser.In {
		e = c.start
	} else {
		e = c.end
	}
	tuple := e.Right.(*parser.DTuple).D
	existingSpans := spans
	spans = make([]logicalSpan, 0, len(existingSpans)*len(tuple))
	for _, datum := range tuple {
		// parts will accumulate the constraint for the current element of the
		// tuple.
		var parts []logicalKeyPart
		switch t := datum.(type) {
		case *parser.DTuple:
			// The constraint is a tuple of tuples, meaning something like
			// (...) IN ((1,2),(3,4)).
			for j, tupleIdx := range c.tupleMap {
				dir, err := index.ColumnDirections[firstCol+j].ToEncodingDirection()
				if err != nil {
					return nil, err
				}
				parts = append(parts, logicalKeyPart{
					val:       t.D[tupleIdx],
					dir:       dir,
					inclusive: true,
				})
			}
		default:
			// The constraint is a tuple of values, meaning something like
			// a IN (1,2).
			dir, err := index.ColumnDirections[firstCol].ToEncodingDirection()
			if err != nil {
				return nil, err
			}
			parts = append(parts, logicalKeyPart{
				val:       datum,
				dir:       dir,
				inclusive: true,
			})
		}
		for _, s := range existingSpans {
			if c.start != nil {
				s.start = append(s.start, parts...)
			}
			if c.end != nil {
				s.end = append(s.end, parts...)
			}
			spans = append(spans, s)
		}
	}
	return spans, nil
}

// spanEvent corresponds to either the start or the end of a span. It is used
// internally by mergeAndSortSpans.
type spanEvent struct {
	start bool
	key   roachpb.Key
}

type spanEvents []spanEvent

// implement Sort.Interface
func (a spanEvents) Len() int      { return len(a) }
func (a spanEvents) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a spanEvents) Less(i, j int) bool {
	cmp := a[i].key.Compare(a[j].key)
	if cmp != 0 {
		return cmp < 0
	}
	// For the same key, prefer "start" events.
	return a[i].start && !a[j].start
}

// makeSpans constructs the spans for an index given the orIndexConstraints by
// merging the spans for the disjunctions (top-level OR branches). The resulting
// spans are non-overlapping and ordered.
func makeSpans(
	constraints orIndexConstraints,
	tableDesc *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
) (roachpb.Spans, error) {
	if len(constraints) == 0 {
		return roachpb.Spans{tableDesc.IndexSpan(index.ID)}, nil
	}
	var allLogicalSpans []logicalSpan
	for _, c := range constraints {
		s, err := makeLogicalSpansForIndexConstraints(c, tableDesc, index)
		if err != nil {
			return nil, err
		}
		allLogicalSpans = append(allLogicalSpans, s...)
	}
	allSpans, err := spansFromLogicalSpans(allLogicalSpans, tableDesc, index)
	if err != nil {
		return nil, err
	}
	return mergeAndSortSpans(allSpans), nil
}

// logicalSpan is a higher-level representation of a span that uses Datums
// instead of bytes. TODO(radu): In the future, I think we probably want to
// remove the expressions in indexConstraints as well and directly put in
// logicalKeyParts.
type logicalSpan struct {
	start, end []logicalKeyPart
}

type logicalKeyPart struct {
	val       parser.Datum
	dir       encoding.Direction
	inclusive bool
}

func spansFromLogicalSpans(
	logicalSpans []logicalSpan, tableDesc *sqlbase.TableDescriptor, index *sqlbase.IndexDescriptor,
) (roachpb.Spans, error) {
	spans := make(roachpb.Spans, len(logicalSpans))
	interstices := make([][]byte, len(index.ColumnDirections)+1)
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
			interstices[sharedPrefixLen] = encoding.EncodeNotNullDescending(interstices[sharedPrefixLen])
		}
		interstices[sharedPrefixLen] =
			encoding.EncodeUvarintAscending(interstices[sharedPrefixLen], uint64(tableDesc.ID))
		interstices[sharedPrefixLen] =
			encoding.EncodeUvarintAscending(interstices[sharedPrefixLen], uint64(index.ID))
	}
	for i, ls := range logicalSpans {
		var s roachpb.Span
		var err error
		if s, err = spanFromLogicalSpan(ls, interstices); err != nil {
			return nil, err
		}
		spans[i] = s
	}
	return spans, nil
}

// interstices[i] is inserted right before the ith key part. The last element of
// interstices is inserted at the end (if all key parts are present).
func spanFromLogicalSpan(ls logicalSpan, interstices [][]byte) (roachpb.Span, error) {
	var s roachpb.Span
	for i := 0; ; i++ {
		s.Key = append(s.Key, interstices[i]...)
		if i >= len(ls.start) {
			break
		}
		part := ls.start[i]
		var err error
		s.Key, err = encodeLogicalKeyPart(s.Key, part)
		if err != nil {
			return roachpb.Span{}, err
		}
		if !part.inclusive {
			if i != len(ls.start)-1 {
				return roachpb.Span{}, errors.New("exclusive start constraint must be last")
			}
			// NotNull is already exclusive.
			if part.val != parser.DNull {
				s.Key = s.Key.PrefixEnd()
			}
			break
		}
	}
	// Since the end of a span is exclusive, if the last constraint is an
	// inclusive one, we might need to make the key exclusive by applying a
	// PrefixEnd(). We normally avoid doing this by transforming "a = x" to "a =
	// x±1" for the last end constraint, depending on the encoding direction
	// (since this keeps the key nice and pretty-printable). However, we might not
	// be able to do the ±1.
	if n := len(ls.end); n > 0 && ls.end[n-1].inclusive {
		last := &ls.end[n-1]
		nextVal, hasNext := nextInDirection(last.val, last.dir)
		if hasNext {
			last.val = nextVal
			last.inclusive = false
		}
	}
	for i := 0; ; i++ {
		s.EndKey = append(s.EndKey, interstices[i]...)
		if i >= len(ls.end) {
			break
		}
		part := ls.end[i]
		var err error
		s.EndKey, err = encodeLogicalKeyPart(s.EndKey, part)
		if err != nil {
			return roachpb.Span{}, err
		}
		if !part.inclusive {
			if i != len(ls.end)-1 {
				return roachpb.Span{}, errors.New("exclusive end constraint must be last")
			}
			return s, nil
		}
	}
	s.EndKey = s.EndKey.PrefixEnd()
	return s, nil
}

func encodeLogicalKeyPart(b []byte, part logicalKeyPart) ([]byte, error) {
	if part.val == parser.DNull && !part.inclusive {
		if part.dir == encoding.Ascending {
			return encoding.EncodeNotNullAscending(b), nil
		}
		return encoding.EncodeNotNullDescending(b), nil
	}
	return sqlbase.EncodeTableKey(b, part.val, part.dir)
}

func nextInDirection(val parser.Datum, dir encoding.Direction) (parser.Datum, bool) {
	if dir == encoding.Ascending {
		if val.IsMax() {
			return nil, false
		}
		return val.Next()
	}
	if val.IsMin() {
		return nil, false
	}
	return val.Prev()
}

// mergeAndSortSpans is used to merge a set of potentially overlapping spans
// into a sorted set of non-overlapping spans.
func mergeAndSortSpans(s roachpb.Spans) roachpb.Spans {
	// This is the classic 1D geometry problem of merging overlapping segments
	// on the X axis. It can be solved using a scan algorithm: we go through all
	// segment starting and ending points in X order (as "events") and keep
	// track of how many open segments we have at each point.
	events := make(spanEvents, 0, 2*len(s))
	for i := range s {
		// Remove any spans which are empty. This can happen for constraints such as
		// "a > 1 AND a < 2" which we do not simplify to false but which is treated
		// as "a >= 2 AND a < 2" for span generation.
		if s[i].Key.Compare(s[i].EndKey) < 0 {
			events = append(events,
				spanEvent{start: true, key: s[i].Key},
				spanEvent{start: false, key: s[i].EndKey})
		}
	}
	sort.Sort(events)
	openSpans := 0
	s = s[:0]
	for _, e := range events {
		if e.start {
			if openSpans == 0 {
				// Start a new span. Because for equal keys the start events
				// come first, there can't be end events for this key.
				// The end of the span will be adjusted as we move forward.
				s = append(s, roachpb.Span{Key: e.key, EndKey: e.key})
			}
			openSpans++
		} else {
			openSpans--
			if openSpans < 0 {
				panic("end span with no spans started")
			} else if openSpans == 0 {
				// Adjust the end of the last span.
				s[len(s)-1].EndKey = e.key
			}
		}
	}
	if openSpans != 0 {
		panic("scan ended with open spans")
	}
	return s
}

// makeLogicalSpansForIndexConstraints constructs the spans for an index given
// an instance of indexConstraints. The resulting spans are non-overlapping (by
// virtue of the input constraints being disjunct).
func makeLogicalSpansForIndexConstraints(
	constraints indexConstraints, tableDesc *sqlbase.TableDescriptor, index *sqlbase.IndexDescriptor,
) ([]logicalSpan, error) {
	// We have one constraint per column, so each contributes something
	// to the start and/or the end key of the span.
	// But we also have (...) IN <tuple> constraints that span multiple columns.
	// These constraints split each span, and that's how we can end up with
	// multiple spans.
	resultSpans := []logicalSpan{{}}

	colIdx := 0
	for _, c := range constraints {
		// IN is handled separately.
		if (c.start != nil && c.start.Operator == parser.In) ||
			(c.end != nil && c.end.Operator == parser.In) {
			var err error
			resultSpans, err = applyInConstraint(resultSpans, c, colIdx, index)
			if err != nil {
				return nil, err
			}
		} else {
			dir, err := index.ColumnDirections[colIdx].ToEncodingDirection()
			if err != nil {
				return nil, err
			}
			if c.start != nil {
				var part logicalKeyPart
				if dir == encoding.Ascending {
					part = encodeStartConstraintAscending(c.start)
				} else {
					part = encodeStartConstraintDescending(c.start)
				}
				for i := range resultSpans {
					resultSpans[i].start = append(resultSpans[i].start, part)
				}
			}
			if c.end != nil {
				var part logicalKeyPart
				if dir == encoding.Ascending {
					part = encodeEndConstraintAscending(c.end)
				} else {
					part = encodeEndConstraintDescending(c.end)
				}
				for i := range resultSpans {
					resultSpans[i].end = append(resultSpans[i].end, part)
				}
			}
		}
		colIdx += c.numColumns()
	}
	return resultSpans, nil
}

// exactPrefix returns the count of the columns of the index for which an exact
// prefix match was requested in the indexConstraints (which can be one of
// multiple OR branches in the WHERE clause). For example, if an index was
// defined on the columns (a, b, c) and the index constraints are derived from
// "(a, b) = (1, 2)", exactPrefix() would return 2.
func (ic indexConstraints) exactPrefix() int {
	prefix := 0
	for _, c := range ic {
		if c.start == nil || c.end == nil || c.start != c.end {
			return prefix
		}
		switch c.start.Operator {
		case parser.EQ:
			prefix++
		case parser.In:
			if tuple, ok := c.start.Right.(*parser.DTuple); !ok || len(tuple.D) != 1 {
				// TODO(radu): we may still have an exact prefix if the first
				// value in each tuple is the same, e.g.
				// `(a, b) IN ((1, 2), (1, 3))`
				return prefix
			}
			if _, ok := c.start.Left.(*parser.Tuple); ok {
				prefix += len(c.tupleMap)
			} else {
				prefix++
			}
		default:
			return prefix
		}
	}
	return prefix

}

// exactPrefixDatums returns the first num exact prefix values as Datums; num
// must be at most ic.exactPrefix()
func (ic indexConstraints) exactPrefixDatums(num int) []parser.Datum {
	if num == 0 {
		return nil
	}
	datums := make([]parser.Datum, 0, num)
	for _, c := range ic {
		if c.start == nil || c.end == nil || c.start != c.end {
			break
		}
		switch c.start.Operator {
		case parser.EQ:
			datums = append(datums, c.start.Right.(parser.Datum))
		case parser.In:
			right := c.start.Right.(*parser.DTuple).D[0]
			if _, ok := c.start.Left.(*parser.Tuple); ok {
				// We have something like `(a,b,c) IN (1,2,3)`
				rtuple := right.(*parser.DTuple)
				for _, tupleIdx := range c.tupleMap {
					datums = append(datums, rtuple.D[tupleIdx])
					if len(datums) == num {
						return datums
					}
				}
			} else {
				// We have something like `a IN (1)`.
				datums = append(datums, right)
			}
		default:
			panic("asking for too many datums")
		}
		if len(datums) == num {
			return datums
		}
	}
	panic("asking for too many datums")
}

// exactPrefix returns the count of the columns of the index for which an exact
// prefix match was requested. Some examples if an index was defined on the
// columns (a, b, c):
//
//    |----------------------------------------------------------|
//    |            WHERE clause                    | exactPrefix |
//    |----------------------------------------------------------|
//    |  (a, b) = (1, 2)                           |      2      |
//    |  (a, b) = (1, 2) OR (a, b, c) = (1, 3, 0)  |      1      |
//    |  (a, b) = (1, 2) OR (a, b, c) = (3, 4, 0)  |      0      |
//    |  (a, b) = (1, 2) OR a = 1                  |      1      |
//    |  (a, b) = (1, 2) OR a = 2                  |      0      |
//    |----------------------------------------------------------|
func (oic orIndexConstraints) exactPrefix(evalCtx *parser.EvalContext) int {
	if len(oic) == 0 {
		return 0
	}
	// To have an exact prefix of length L:
	//  - all "or" constraints must have an exact prefix of at least L
	//  - for each of the L columns in the exact prefix, all constraints must
	//    resolve to the *same* value for that column.

	// We start by finding the minimum length of all exact prefixes.
	minPrefix := oic[0].exactPrefix()
	for i := 1; i < len(oic) && minPrefix > 0; i++ {
		p := oic[i].exactPrefix()
		if minPrefix > p {
			minPrefix = p
		}
	}
	if minPrefix == 0 || len(oic) == 1 {
		return minPrefix
	}

	// Get the exact values for the first constraint.
	datums := oic[0].exactPrefixDatums(minPrefix)

	for i := 1; i < len(oic); i++ {
		iDatums := oic[i].exactPrefixDatums(len(datums))
		// Compare the exact values of this constraint, keep the matching
		// prefix.
		for i, d := range datums {
			if !(d.ResolvedType().Equivalent(iDatums[i].ResolvedType()) &&
				d.Compare(evalCtx, iDatums[i]) == 0) {
				datums = datums[:i]
				break
			}
		}
	}
	return len(datums)
}

// applyConstraints applies the constraints on values specified by constraints
// to an expression, simplifying the expression where possible. For example, if
// the expression is "a = 1" and the constraint is "a = 1", the expression can
// be simplified to "true". If the expression is "a = 1 AND b > 2" and the
// constraint is "a = 1", the expression is simplified to "b > 2".
//
// Note that applyConstraints currently only handles simple cases.
func applyIndexConstraints(
	evalCtx *parser.EvalContext, typedExpr parser.TypedExpr, constraints orIndexConstraints,
) parser.TypedExpr {
	if len(constraints) != 1 {
		// We only support simplifying the expressions if there aren't multiple
		// disjunctions (top-level OR branches).
		return typedExpr
	}
	v := &applyConstraintsVisitor{evalCtx: evalCtx}
	expr := typedExpr.(parser.Expr)
	for _, c := range constraints[0] {
		// Apply the start constraint.
		v.constraints = expandConstraint(v.constraints, c.start, c.tupleMap)
		expr, _ = parser.WalkExpr(v, expr)

		if c.start != c.end {
			// If there is a range (x >= 3 AND x <= 10), apply the
			// end-of-range constraint as well.
			v.constraints = expandConstraint(v.constraints, c.end, c.tupleMap)
			expr, _ = parser.WalkExpr(v, expr)
		}

		// We can only continue to apply the constraints if the constraints we have
		// applied so far are equality constraints. There are two cases to
		// consider, both of which satisfy c.start == c.end.
		if c.start == c.end {
			// The first is that both the start and end constraints are
			// equality.
			if c.start.Operator == parser.EQ {
				continue
			}
			// The second case is that both the start and end constraint are an IN
			// operator with only a single value.
			if c.start.Operator == parser.In && len(c.start.Right.(*parser.DTuple).D) == 1 {
				continue
			}
		}
		break
	}
	if expr == parser.DBoolTrue {
		return nil
	}
	return expr.(parser.TypedExpr)
}

// expandConstraint transforms a potentially complex constraint
// expression into one or more simple constraints suitable for the
// VisitPre() code below.
// The following are transformed:
// - (a,b,c) = (x,y,z)    -> [a=x, b=y, c=z]     (same for !=)
// - (a,b,c) <= (x,y,z)   -> [a<=x]              (same for <=)
// - (a,b,c) < (x,y,z)    -> [a<=x]              (same for >)
// - (a,b,c) IN ((x,y,z)) -> [a=x, b=y, c=z] but only for the part
//                           of the tuple (a,b,c) that was matched by the index.
// - (a,b,c) IS TRUE, (a,b,c) IS NULL, etc -> constraint ignored
func expandConstraint(
	a []*parser.ComparisonExpr, c *parser.ComparisonExpr, tupleMap []int,
) []*parser.ComparisonExpr {
	if c == nil {
		return a
	}
	if vars, ok := c.Left.(*parser.Tuple); ok {
		if c.Operator == parser.Is || c.Operator == parser.IsNot {
			// The syntax <tuple> IS <val> or <tuple> IS NOT <val> is
			// always false.
			return a
		}
		vals := *c.Right.(*parser.DTuple)

		switch c.Operator {
		case parser.NE, parser.EQ:
			// (a,b,c) != (x,y,z)  ->  [a!=x, b!=y, c!=z]
			// (a,b,c) = (x,y,z)   ->  [a=x, b=y, c=z]
			for i, v := range vars.Exprs {
				a = append(a, &parser.ComparisonExpr{Operator: c.Operator, Left: v, Right: vals.D[i]})
			}

		case parser.LT:
			// (a,b,c) < (x,y,z)  -> [a<=x]
			a = append(a,
				&parser.ComparisonExpr{Operator: parser.LE, Left: vars.Exprs[0], Right: vals.D[0]})
		case parser.GT:
			// (a,b,c) > (x,y,z)  -> [a>=x]
			a = append(a,
				&parser.ComparisonExpr{Operator: parser.GE, Left: vars.Exprs[0], Right: vals.D[0]})
		case parser.LE, parser.GE:
			// (a,b,c) <= (x,y,z)  -> [a<=x]
			a = append(a,
				&parser.ComparisonExpr{Operator: c.Operator, Left: vars.Exprs[0], Right: vals.D[0]})

		case parser.In:
			// (a,b,c) IN ((x,y,z)) -> [a=x, b=y, c=z]
			// But beware of which part of the tuple has been matched by the index.
			if len(vals.D) == 1 {
				if oneTuplep, ok := vals.D[0].(*parser.DTuple); ok {
					oneTuple := *oneTuplep
					for i := range tupleMap {
						a = append(a,
							&parser.ComparisonExpr{Operator: parser.EQ, Left: vars.Exprs[i], Right: oneTuple.D[i]})
					}
				}
			}
		}
	} else {
		a = append(a, c)
	}
	return a
}

type applyConstraintsVisitor struct {
	evalCtx     *parser.EvalContext
	constraints []*parser.ComparisonExpr
}

func (v *applyConstraintsVisitor) VisitPre(expr parser.Expr) (recurse bool, newExpr parser.Expr) {
	if t, ok := expr.(*parser.ComparisonExpr); ok {

		// If looking at an expression of the form (a,b,c) IN ((x,y,z)), decompose it.
		if tLeft, ok2 := t.Left.(*parser.Tuple); ok2 {
			if tRight, ok3 := t.Right.(*parser.DTuple); ok3 && len(tRight.D) == 1 {
				if vRight, ok4 := tRight.D[0].(*parser.DTuple); ok4 {
					andExprs := make([]parser.TypedExpr, len(vRight.D))
					for i, v := range tLeft.Exprs {
						tv := v.(parser.TypedExpr)
						andExprs[i] = parser.NewTypedComparisonExpr(parser.EQ, tv, vRight.D[i])
					}
					return true, joinAndExprs(andExprs)
				}
			}
		}

		for _, c := range v.constraints {
			expr = applyConstraint(v.evalCtx, t, c)
			t, ok = expr.(*parser.ComparisonExpr)
			if !ok {
				break
			}
		}
	}
	return true, expr
}

func (v *applyConstraintsVisitor) VisitPost(expr parser.Expr) parser.Expr {
	switch t := expr.(type) {
	case *parser.AndExpr:
		if t.Left == parser.DBoolTrue && t.Right == parser.DBoolTrue {
			return parser.DBoolTrue
		} else if t.Left == parser.DBoolTrue {
			return t.Right
		} else if t.Right == parser.DBoolTrue {
			return t.Left
		}
	}
	return expr
}

// applyConstraint tries to simplify the expression t on the left
// assuming that the expression c on the right (the constraint) is
// true.
func applyConstraint(
	evalCtx *parser.EvalContext, t *parser.ComparisonExpr, c *parser.ComparisonExpr,
) parser.Expr {
	// Check that both expressions have the same variable on the left.
	// It is always true for the constraint, and
	// simplifyExpr() has ensured this is true in most sub-expressions of t.
	varLeft := c.Left.(*parser.IndexedVar)
	if varRight, ok := t.Left.(*parser.IndexedVar); !ok || varLeft.Idx != varRight.Idx {
		return t
	}

	// applyConstraint() is only defined over comparisons that
	// have a Datum on the right side.
	datum, ok := t.Right.(parser.Datum)
	if !ok {
		return t
	}
	cdatum := c.Right.(parser.Datum)

	switch c.Operator {
	case parser.IsNot:
		if cdatum == parser.DNull {
			switch t.Operator {
			case parser.Is:
				return parser.MakeDBool(datum != parser.DNull)
			case parser.IsNot:
				return parser.MakeDBool(datum == parser.DNull)
			}
		} else {
			return applyConstraintFlat(evalCtx, t, datum, parser.NE, cdatum)
		}
	case parser.Is:
		if cdatum == parser.DNull {
			switch t.Operator {
			case parser.Is:
				return parser.MakeDBool(datum == parser.DNull)
			case parser.IsNot:
				return parser.MakeDBool(datum != parser.DNull)
			default:
				// A NULL var compared to anything is always false.
				return parser.DBoolFalse
			}
		} else {
			return applyConstraintFlat(evalCtx, t, datum, parser.EQ, cdatum)
		}
	default:
		return applyConstraintFlat(evalCtx, t, datum, c.Operator, cdatum)
	}
	return t
}

// applyConstraintFlat is the common branch of applyConstraint().
// tries to simplify the expression t on the right which has the form
// [x OP datum] assuming that the expression [x cOp cdatum] (the
// constraint) is true.
func applyConstraintFlat(
	evalCtx *parser.EvalContext,
	t *parser.ComparisonExpr,
	datum parser.Datum,
	cOp parser.ComparisonOperator,
	cdatum parser.Datum,
) parser.Expr {
	// Special casing: expression queries IS NULL or IS NOT NULL.
	if (t.Operator == parser.Is || t.Operator == parser.IsNot) && datum == parser.DNull {
		switch cOp {
		case parser.EQ, parser.GE, parser.GT, parser.IN:
			// If the constraint says the value is equal to something, it
			// cannot be NULL.
			// If the constraint says the value is greater to something, it
			// cannot be NULL, because NULL sorts before anything else.
			// If the constraint says the value is smaller than something,
			// then we don't know, because NULL is smaller than everything
			// else too.
			// That's why the cases for parser.LE and parser.LT are missing
			// above.
			// This may need to be revisited once NULL
			// ordering becomes configurable.
			return parser.MakeDBool(t.Operator == parser.IsNot)
		}
		return t
	}

	// Additional special casing.
	switch t.Operator {
	case parser.LE, parser.LT, parser.GE,
		parser.GT, parser.NE, parser.IsNot:
		if cOp == parser.In {
			// The general case only handles range constraints.
			// Stop here.

			// Note: if we ever support other constraints than IN and the
			// range operators, the condition on cOp here must be extended.
			return t
		}

		// Otherwise, the general case below knows about all these
		// cases. Go forward.

	case parser.Is, parser.EQ:
		// (Expr IS ...) / (Expr = ...)

		if cOp == parser.In {
			// constraint says "a IN (x, y, z...)"
			// Expr asks "= Cst" or IS TRUE / IS FALSE: check whether the
			// value is in the IN set. If it is not, we're sure it never
			// matches. Otherwise we don't know.
			ctuple := cdatum.(*parser.DTuple)
			i := sort.Search(len(ctuple.D), func(i int) bool {
				return ctuple.D[i].(parser.Datum).Compare(evalCtx, datum) >= 0
			})
			if i == len(ctuple.D) || ctuple.D[i].Compare(evalCtx, datum) != 0 {
				return parser.DBoolFalse
			}
			return t
		}
		// Continue to the general case below.

	case parser.In:
		// Expr: "a IN (t, u, v, ...)"

		switch cOp {
		case parser.In:
			// constraint says "a IN (x, y, z...)"
			// true if (x, y, z, ...) is a subset of (t, u, v, ...)
			// unknown otherwise
			if ttuple, ok := datum.(*parser.DTuple); ok {
				// Note: A is a subset of B iff A \ B == empty.
				ctuple := cdatum.(*parser.DTuple)
				diff := ctuple.SortedDifference(evalCtx, ttuple)
				if len(diff.D) == 0 {
					return parser.DBoolTrue
				}
			}

		case parser.EQ:
			// constraint says "a = Cst"
			// true if the known value is in the IN set, false otherwise.
			if tuple, ok := datum.(*parser.DTuple); ok {
				i := sort.Search(len(tuple.D), func(i int) bool {
					return tuple.D[i].(parser.Datum).Compare(evalCtx, cdatum) >= 0
				})
				if i < len(tuple.D) && tuple.D[i].Compare(evalCtx, cdatum) == 0 {
					return parser.DBoolTrue
				}
			}
			return parser.DBoolFalse
		}

		// The general case below doesn't know about
		// IN expressions. Can't go further.
		return t

	case parser.NotIn:
		// Expr: "a NOT IN (t, u, v, ...)"

		switch cOp {
		case parser.In:
			// constraint says "a IN (x, y, z...)"
			// True if none of the values in (x, y, z...) are in (t, u, v, ...)
			// unknown otherwise
			if ttuple, ok := datum.(*parser.DTuple); ok {
				// Note: A inter B is empty iff A \ B == A
				ctuple := cdatum.(*parser.DTuple)
				diff := ctuple.SortedDifference(evalCtx, ttuple)
				if len(diff.D) < len(ctuple.D) {
					return parser.DBoolTrue
				}
			}

		case parser.EQ:
			// constraint says "a = Cst"
			// false if the known value is in the NOT IN set, true otherwise.
			if tuple, ok := datum.(*parser.DTuple); ok {
				i := sort.Search(len(tuple.D), func(i int) bool {
					return tuple.D[i].(parser.Datum).Compare(evalCtx, cdatum) >= 0
				})
				if i < len(tuple.D) && tuple.D[i].Compare(evalCtx, cdatum) == 0 {
					return parser.DBoolFalse
				}
			}
			return parser.DBoolTrue
		}

		// The general case below doesn't know about
		// NOT IN expressions. Can't go further.
		return t

	default:
		// We have a comparison expression with a comparator which
		// is not handled below (IN, = ANY, etc). Can't go further.
		return t
	}

	// General case: constraint and expression both specify a range.

	// This comparison uses some magic courtesy of Radu.
	//
	// We map a few interesting points of the datum space to the integer
	// axis as follows:
	//
	//                  /- the smaller between datum and cdatum
	//                  |
	// -infinity        |      /- the larger between datum and cdatum
	//     |            |      |
	//     |            |      |            /- +infinity
	//     v            v      v            v
	//     |------------|------|------------|
	//   -100           0      10          100
	//
	// If the datums are equal, they both map to 0.
	//
	// Constraints become closed intervals on this axis. For example: a
	// "GT" constraint with the smaller datum becomes [0, 100].  For
	// exclusive constraints we add or subtract 1 from the datum value:
	// a "GE" constraint with the smaller datum becomes [1, 100]; a "LE"
	// constraint becomes [-100, -1].
	//
	// Checking for overlap between the constraints then becomes
	// equivalent to checking for overlap between the closed intervals
	// (which is easy).

	cmp := datum.Compare(evalCtx, cdatum)
	tStart, tEnd, tOk := makeComparisonInterval(t.Operator, cmp > 0)
	cStart, cEnd, cOk := makeComparisonInterval(cOp, cmp < 0)

	disjoint := cStart > tEnd || tStart > cEnd
	overlapping := tStart <= cStart && cEnd <= tEnd

	if tOk && cOk {
		if disjoint {
			return parser.DBoolFalse
		}
		if overlapping {
			return parser.DBoolTrue
		}
		return t
	}
	// BOOM: just handled 5*5 combinations of operators!
	//
	// If the constraint is an interval and the expression is NE/IsNot,
	// we can till use the intervals (hence the NE case in
	// makeInterval).
	if cOk && (t.Operator == parser.NE || t.Operator == parser.IsNot) && disjoint {
		return parser.DBoolTrue
	}
	return t
}

// makeComparisonInterval supports the range comparison in applyConstraintsFlat above.
// See the comment at the point of call for more details.
// The boolean return value indicates whether the comparison actually
// defines a range.
func makeComparisonInterval(op parser.ComparisonOperator, largerDatum bool) (int, int, bool) {
	x := 0
	if largerDatum {
		x = 10
	}

	switch op {
	case parser.EQ, parser.Is:
		return x, x, true
	case parser.LE:
		return -100, x, true
	case parser.LT:
		return -100, x - 1, true
	case parser.GE:
		return x, 100, true
	case parser.GT:
		return x + 1, 100, true
	case parser.NE, parser.IsNot:
		return x, x, false
	default:
		return 0, 0, false
	}
}
