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
	"reflect"
	"sort"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
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
// In addition, singleKey indicates if the ordering is such that we only need to
// retrieve one key (cases like `SELECT MIN(x) ..` and we have an index on x).
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
type analyzeOrderingFn func(indexOrdering orderingInfo) (matchingCols, totalCols int, singleKey bool)

// selectIndex analyzes the scanNode to determine if there is an index
// available that can fulfill the query with a more restrictive scan.
//
// Analysis currently consists of a simplification of the filter expression,
// replacing expressions which are not usable by indexes by "true". The
// simplified expression is then considered for each index and a set of range
// constraints is created for the index. The candidate indexes are ranked using
// these constraints and the best index is selected. The contraints are then
// transformed into a set of spans to scan within the index.
//
// The analyzeOrdering function is used to determine how useful the ordering of
// an index is. If no particular ordering is desired, it can be nil.
//
// If preferOrderMatching is true, we prefer an index that matches the desired
// ordering completely, even if it is not a covering index.
func selectIndex(
	s *scanNode, analyzeOrdering analyzeOrderingFn, preferOrderMatching bool,
) (planNode, error) {
	if s.desc.isEmpty() || (s.filter == nil && analyzeOrdering == nil && s.specifiedIndex == nil) {
		// No table or no where-clause, no ordering, and no specified index.
		s.initOrdering(0)
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
		exprs, equivalent := analyzeExpr(s.filter)
		if log.V(2) {
			log.Infof("analyzeExpr: %s -> %s [equivalent=%v]", s.filter, exprs, equivalent)
		}

		// Check to see if the filter simplified to a constant.
		if len(exprs) == 1 && len(exprs[0]) == 1 {
			if d, ok := exprs[0][0].(parser.DBool); ok && bool(!d) {
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
			c.analyzeOrdering(s, analyzeOrdering, preferOrderMatching)
		}
	}

	indexInfoByCost(candidates).Sort()

	if log.V(2) {
		for i, c := range candidates {
			log.Infof("%d: selectIndex(%s): cost=%v constraints=%s reverse=%t",
				i, c.index.Name, c.cost, c.constraints, c.reverse)
		}
	}

	// After sorting, candidates[0] contains the best index. Copy its info into
	// the scanNode.
	c := candidates[0]
	s.index = c.index
	s.isSecondaryIndex = (c.index != &s.desc.PrimaryIndex)
	s.spans = makeSpans(c.constraints, c.desc.ID, c.index)
	if len(s.spans) == 0 {
		// There are no spans to scan.
		return &emptyNode{}, nil
	}
	s.filter = applyConstraints(s.filter, c.constraints)
	noFilter := (s.filter == nil)
	s.reverse = c.reverse

	var plan planNode
	if c.covering {
		s.initOrdering(c.exactPrefix)
		plan = s
	} else {
		// Note: makeIndexJoin can modify s.filter.
		plan = makeIndexJoin(s, c.exactPrefix)
	}

	// If we have no filter, we can request a single key in some cases.
	if noFilter && analyzeOrdering != nil {
		_, _, singleKey := analyzeOrdering(plan.Ordering())
		if singleKey {
			s.spans = s.spans[:1]
			s.spans[0].count = 1
		}
	}

	if log.V(3) {
		log.Infof("%s: filter=%v", c.index.Name, s.filter)
		for i, span := range s.spans {
			log.Infof("%s/%d: %s", c.index.Name, i, prettySpan(span, 2))
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
	desc        *TableDescriptor
	index       *IndexDescriptor
	constraints orIndexConstraints
	cost        float64
	covering    bool // Does the index cover the required qvalues?
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
func (v *indexInfo) analyzeExprs(exprs []parser.Exprs) {
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
func (v *indexInfo) analyzeOrdering(scan *scanNode, analyzeOrdering analyzeOrderingFn,
	preferOrderMatching bool) {
	// Compute the prefix of the index for which we have exact constraints. This
	// prefix is inconsequential for ordering because the values are identical.
	v.exactPrefix = v.constraints.exactPrefix()

	// Analyze the ordering provided by the index (either forward or reverse).
	fwdIndexOrdering := scan.computeOrdering(v.index, v.exactPrefix, false)
	revIndexOrdering := scan.computeOrdering(v.index, v.exactPrefix, true)
	fwdMatch, orderCols, _ := analyzeOrdering(fwdIndexOrdering)
	revMatch, orderCols, _ := analyzeOrdering(revIndexOrdering)

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
		log.Infof("%s: analyzeOrdering: weight=%0.2f reverse=%v index=%d",
			v.index.Name, weight, v.reverse, fwdIndexOrdering)
	}
}

func getQValColIdx(expr parser.Expr) (ok bool, colIdx int) {
	switch q := expr.(type) {
	case *qvalue:
		return true, q.colRef.colIdx

	case *scanQValue:
		return true, q.colIdx
	}
	return false, -1
}

// makeOrConstraints populates the indexInfo.constraints field based on the
// analyzed expressions. Each element of constraints corresponds to one
// of the top-level disjunctions and is generated using makeIndexConstraint.
func (v *indexInfo) makeOrConstraints(orExprs []parser.Exprs) error {
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
// similarly a prefix of the contraints has a .end (in other words,
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
// 1", but if we performed this transform in simpilfyComparisonExpr it would
// simplify to "a < 1 OR a >= 2" which is also the same as "a != 1", but not so
// obvious based on comparisons of the constants.
func (v *indexInfo) makeIndexConstraints(andExprs parser.Exprs) (indexConstraints, error) {
	var constraints indexConstraints

	trueStartDone := false
	trueEndDone := false

	for i := 0; i < len(v.index.ColumnIDs); i++ {
		colID := v.index.ColumnIDs[i]
		var colDir encoding.Direction
		var err error
		if colDir, err = v.index.ColumnDirections[i].toEncodingDirection(); err != nil {
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

		for _, e := range andExprs {
			if c, ok := e.(*parser.ComparisonExpr); ok {
				var tupleMap []int

				if ok, colIdx := getQValColIdx(c.Left); ok && v.desc.Columns[colIdx].ID != colID {
					// This expression refers to a column other than the one we're
					// looking for.
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
							ok, colIdx := getQValColIdx(val)
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
				}

				if _, ok := c.Right.(parser.Datum); !ok {
					continue
				}
				if tupleMap != nil && c.Operator != parser.In {
					// We can only handle tuples in IN expressions.
					continue
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
					*startExpr = &parser.ComparisonExpr{
						Operator: parser.IsNot,
						Left:     c.Left,
						Right:    parser.DNull,
					}
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
						constraint.tupleMap = tupleMap
					}
					if !*endDone && (*endExpr == nil || (*endExpr).Operator != parser.EQ) {
						*endExpr = c
						constraint.tupleMap = tupleMap
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
						*startExpr = &parser.ComparisonExpr{
							Operator: parser.EQ,
							Left:     c.Left,
							Right:    c.Right,
						}
					} else if c.Right.(parser.Datum).HasNext() {
						*startExpr = &parser.ComparisonExpr{
							Operator: parser.GE,
							Left:     c.Left,
							Right:    c.Right.(parser.Datum).Next(),
						}
					} else {
						*startExpr = c
					}
				case parser.LT:
					if *endDone || (*endExpr != nil) {
						continue
					}
					// Transform "<" into "<=".
					if c.Right.(parser.Datum).IsMin() {
						*endExpr = &parser.ComparisonExpr{
							Operator: parser.EQ,
							Left:     c.Left,
							Right:    c.Right,
						}
					} else if c.Right.(parser.Datum).HasPrev() {
						*endExpr = &parser.ComparisonExpr{
							Operator: parser.LE,
							Left:     c.Left,
							Right:    c.Right.(parser.Datum).Prev(),
						}
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
					if mixed, err := isMixedTypeComparison(*startExpr); err != nil {
						return nil, err
					} else if mixed {
						*startExpr = nil
					}
				}
				if preEnd != *endExpr {
					if mixed, err := isMixedTypeComparison(*endExpr); err != nil {
						return nil, err
					} else if mixed {
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
				*startExpr = &parser.ComparisonExpr{
					Operator: parser.IsNot,
					Left:     (*endExpr).Left,
					Right:    parser.DNull,
				}
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
			if !v.index.containsColumnID(colID) {
				return false
			}
		}
	}
	return true
}

func isMixedTypeComparison(c *parser.ComparisonExpr) (bool, error) {
	switch c.Operator {
	case parser.In, parser.NotIn:
		tuple := c.Right.(parser.DTuple)
		for _, expr := range tuple {
			if mixed, err := sameTypeExprs(c.Left, expr); mixed || err != nil {
				return mixed, err
			}
		}
		return false, nil
	default:
		return sameTypeExprs(c.Left, c.Right)
	}
}

func sameTypeExprs(left, right parser.Expr) (bool, error) {
	dummyLeft, err := left.TypeCheck(nil)
	if err != nil || dummyLeft == parser.DNull {
		return false, err
	}
	dummyRight, err := right.TypeCheck(nil)
	if err != nil || dummyRight == parser.DNull {
		return false, err
	}
	return !dummyLeft.TypeEqual(dummyRight), nil
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

func encodeStartConstraintAscending(spans []span, c *parser.ComparisonExpr) {
	switch c.Operator {
	case parser.IsNot:
		// A IS NOT NULL expression allows us to constrain the start of
		// the range to not include NULL.
		if c.Right != parser.DNull {
			panic(fmt.Sprintf("expected NULL operand for IS NOT operator, found %v", c.Right))
		}
		for i := range spans {
			spans[i].start = encoding.EncodeNotNullAscending(spans[i].start)
		}
	case parser.NE:
		panic("'!=' operators should have been transformed to 'IS NOT NULL'")
	case parser.GE, parser.EQ:
		datum := c.Right.(parser.Datum)
		key, err := encodeTableKey(nil, datum, encoding.Ascending)
		if err != nil {
			panic(err)
		}
		// Append the constraint to all of the existing spans.
		for i := range spans {
			spans[i].start = append(spans[i].start, key...)
		}
	case parser.GT:
		// A ">" constraint is the last start constraint. Since the constraint
		// is exclusive and the start key is inclusive, we're going to apply
		// a .PrefixEnd(). Note that a ">" is usually transformed to a ">=".
		datum := c.Right.(parser.Datum)
		key, pErr := encodeTableKey(nil, datum, encoding.Ascending)
		if pErr != nil {
			panic(pErr)
		}
		// Append the constraint to all of the existing spans.
		for i := range spans {
			spans[i].start = append(spans[i].start, key...)
			spans[i].start = spans[i].start.PrefixEnd()
		}
	default:
		panic(fmt.Sprintf("unexpected operator: %s", c))
	}
}

func encodeStartConstraintDescending(
	spans []span, c *parser.ComparisonExpr) {
	switch c.Operator {
	case parser.Is:
		// An IS NULL expressions allows us to constrain the start of the range
		// to begin at NULL.
		if c.Right != parser.DNull {
			panic(fmt.Sprintf("expected NULL operand for IS operator, found %v", c.Right))
		}
		for i := range spans {
			spans[i].start = encoding.EncodeNullDescending(spans[i].start)
		}
	case parser.NE:
		panic("'!=' operators should have been transformed to 'IS NOT NULL'")
	case parser.LE, parser.EQ:
		datum := c.Right.(parser.Datum)
		key, pErr := encodeTableKey(nil, datum, encoding.Descending)
		if pErr != nil {
			panic(pErr)
		}
		// Append the constraint to all of the existing spans.
		for i := range spans {
			spans[i].start = append(spans[i].start, key...)
		}
	case parser.LT:
		// A "<" constraint is the last start constraint. Since the constraint
		// is exclusive and the start key is inclusive, we're going to apply
		// a .PrefixEnd(). Note that a "<" is usually transformed to a "<=".
		datum := c.Right.(parser.Datum)
		key, pErr := encodeTableKey(nil, datum, encoding.Descending)
		if pErr != nil {
			panic(pErr)
		}
		// Append the constraint to all of the existing spans.
		for i := range spans {
			spans[i].start = append(spans[i].start, key...)
			spans[i].start = spans[i].start.PrefixEnd()
		}

	default:
		panic(fmt.Sprintf("unexpected operator: %s", c))
	}
}

func encodeEndConstraintAscending(spans []span, c *parser.ComparisonExpr,
	isLastEndConstraint bool) {
	switch c.Operator {
	case parser.Is:
		// An IS NULL expressions allows us to constrain the end of the range
		// to stop at NULL.
		if c.Right != parser.DNull {
			panic(fmt.Sprintf("expected NULL operand for IS operator, found %v", c.Right))
		}
		for i := range spans {
			spans[i].end = encoding.EncodeNotNullAscending(spans[i].end)
		}
	default:
		datum := c.Right.(parser.Datum)
		if c.Operator != parser.LT {
			for i := range spans {
				spans[i].end = encodeInclusiveEndValue(
					spans[i].end, datum, encoding.Ascending, isLastEndConstraint)
			}
			break
		}
		if !isLastEndConstraint {
			panic(fmt.Sprintf("can't have other end constraints after a '<' constraint, found %v", c.Operator))
		}
		key, err := encodeTableKey(nil, datum, encoding.Ascending)
		if err != nil {
			panic(err)
		}
		// Append the constraint to all of the existing spans.
		for i := range spans {
			spans[i].end = append(spans[i].end, key...)
		}
	}
}

func encodeEndConstraintDescending(spans []span, c *parser.ComparisonExpr,
	isLastEndConstraint bool) {
	switch c.Operator {
	case parser.IsNot:
		// An IS NOT NULL expressions allows us to constrain the end of the range
		// to stop at NULL.
		if c.Right != parser.DNull {
			panic(fmt.Sprintf("expected NULL operand for IS NOT operator, found %v", c.Right))
		}
		for i := range spans {
			spans[i].end = encoding.EncodeNotNullDescending(spans[i].end)
		}
	default:
		datum := c.Right.(parser.Datum)
		if c.Operator != parser.GT {
			for i := range spans {
				spans[i].end = encodeInclusiveEndValue(
					spans[i].end, datum, encoding.Descending, isLastEndConstraint)
			}
			break
		}
		if !isLastEndConstraint {
			panic(fmt.Sprintf("can't have other end constraints after a '>' constraint, found %v", c.Operator))
		}
		key, err := encodeTableKey(nil, datum, encoding.Descending)
		if err != nil {
			panic(err)
		}
		// Append the constraint to all of the existing spans.
		for i := range spans {
			spans[i].end = append(spans[i].end, key...)
		}
	}
}

// Encodes datum at the end of key, using direction `dir` for the encoding.
// The key is a span end key, which is exclusive, but `val` needs to
// be inclusive. So if datum is the last end constraint, we transform it accordingly.
func encodeInclusiveEndValue(
	key roachpb.Key, datum parser.Datum, dir encoding.Direction,
	isLastEndConstraint bool) roachpb.Key {
	// Since the end of a span is exclusive, if the last constraint is an
	// inclusive one, we might need to make the key exclusive by applying a
	// PrefixEnd().  We normally avoid doing this by transforming "a = x" to
	// "a = x±1" for the last end constraint, depending on the encoding direction
	// (since this keeps the key nice and pretty-printable).
	// However, we might not be able to do the ±1.
	needExclusiveKey := false
	if isLastEndConstraint {
		if dir == encoding.Ascending {
			if datum.IsMax() || !datum.HasNext() {
				needExclusiveKey = true
			} else {
				datum = datum.Next()
			}
		} else {
			if datum.IsMin() || !datum.HasPrev() {
				needExclusiveKey = true
			} else {
				datum = datum.Prev()
			}
		}
	}
	key, pErr := encodeTableKey(key, datum, dir)
	if pErr != nil {
		panic(pErr)
	}
	if needExclusiveKey {
		key = key.PrefixEnd()
	}
	return key
}

// Splits spans according to a constraint like (...) in <tuple>.
// If the constraint is (a,b) IN ((1,2),(3,4)), each input span
// will be split into two: the first one will have "1/2" appended to
// the start and/or end, the second one will have "3/4" appended to
// the start and/or end.
//
// Returns the exploded spans.
func applyInConstraint(spans []span, c indexConstraint, firstCol int,
	index *IndexDescriptor, isLastEndConstraint bool) []span {
	var e *parser.ComparisonExpr
	// It might be that the IN constraint is a start constraint, an
	// end constraint, or both, depending on how whether we had
	// start and end constraints for all the previous index cols.
	if c.start != nil && c.start.Operator == parser.In {
		e = c.start
	} else {
		e = c.end
	}
	tuple := e.Right.(parser.DTuple)
	existingSpans := spans
	spans = make([]span, 0, len(existingSpans)*len(tuple))
	for _, datum := range tuple {
		// start and end will accumulate the end constraint for
		// the current element of the tuple.
		var start, end []byte

		switch t := datum.(type) {
		case parser.DTuple:
			// The constraint is a tuple of tuples, meaning something like
			// (...) IN ((1,2),(3,4)).
			for j, tupleIdx := range c.tupleMap {
				var err error
				var colDir encoding.Direction
				if colDir, err = index.ColumnDirections[firstCol+j].toEncodingDirection(); err != nil {
					panic(err)
				}

				if start, err = encodeTableKey(start, t[tupleIdx], colDir); err != nil {
					panic(err)
				}
				end = encodeInclusiveEndValue(
					end, t[tupleIdx], colDir, isLastEndConstraint && (j == len(c.tupleMap)-1))
			}
		default:
			// The constraint is a tuple of values, meaning something like
			// a IN (1,2).
			var colDir encoding.Direction
			var err error
			if colDir, err = index.ColumnDirections[firstCol].toEncodingDirection(); err != nil {
				panic(err)
			}
			if start, err = encodeTableKey(nil, datum, colDir); err != nil {
				panic(err)
			}

			end = encodeInclusiveEndValue(nil, datum, colDir, isLastEndConstraint)
			// TODO(andrei): assert here that we end is not \xff\xff...
			// encodeInclusiveEndValue sometimes calls key.PrefixEnd(),
			// which doesn't work if the input is \xff\xff... However,
			// that shouldn't happen: datum should not have that encoding.
		}
		for _, s := range existingSpans {
			if c.start != nil {
				s.start = append(append(roachpb.Key(nil), s.start...), start...)
			}
			if c.end != nil {
				s.end = append(append(roachpb.Key(nil), s.end...), end...)
			}
			spans = append(spans, s)
		}
	}
	return spans
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
func makeSpans(constraints orIndexConstraints, tableID ID, index *IndexDescriptor) spans {
	if len(constraints) == 0 {
		return makeSpansForIndexConstraints(nil, tableID, index)
	}
	var allSpans spans
	for _, c := range constraints {
		s := makeSpansForIndexConstraints(c, tableID, index)
		allSpans = append(allSpans, s...)
	}
	return mergeAndSortSpans(allSpans)
}

// mergeAndSortSpans is used to merge a set of potentially overlapping spans
// into a sorted set of non-overlapping spans.
func mergeAndSortSpans(s spans) spans {
	// This is the classic 1D geometry problem of merging overlapping segments
	// on the X axis. It can be solved using a scan algorithm: we go through all
	// segment starting and ending points in X order (as "events") and keep
	// track of how many open segments we have at each point.
	events := make(spanEvents, 2*len(s))
	for i := range s {
		events[2*i] = spanEvent{start: true, key: s[i].start}
		events[2*i+1] = spanEvent{start: false, key: s[i].end}
		if s[i].start.Compare(s[i].end) >= 0 {
			panic(fmt.Sprintf("invalid input span %s", prettySpan(s[i], 0)))
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
				s = append(s, span{start: e.key, end: e.key})
			}
			openSpans++
		} else {
			openSpans--
			if openSpans < 0 {
				panic("end span with no spans started")
			} else if openSpans == 0 {
				// Adjust the end of the last span.
				s[len(s)-1].end = e.key
			}
		}
	}
	if openSpans != 0 {
		panic("scan ended with open spans")
	}
	return s
}

// makeSpansForIndexConstraints constructs the spans for an index given an
// instance of indexConstraints. The resulting spans are non-overlapping (by
// virtue of the input constraints being disjunct).
func makeSpansForIndexConstraints(constraints indexConstraints, tableID ID,
	index *IndexDescriptor) spans {
	prefix := roachpb.Key(MakeIndexKeyPrefix(tableID, index.ID))
	// We have one constraint per column, so each contributes something
	// to the start and/or the end key of the span.
	// But we also have (...) IN <tuple> constraints that span multiple columns.
	// These constraints split each span, and that's how we can end up with
	// multiple spans.
	resultSpans := spans{{
		start: append(roachpb.Key(nil), prefix...),
		end:   append(roachpb.Key(nil), prefix...),
	}}

	colIdx := 0
	for i, c := range constraints {
		// We perform special processing on the last end constraint to account for
		// the exclusive nature of the scan end key.
		lastEnd := (c.end != nil) &&
			(i+1 == len(constraints) || constraints[i+1].end == nil)

		// IN is handled separately, since it can affect multiple columns.
		if (c.start != nil && c.start.Operator == parser.In) ||
			(c.end != nil && c.end.Operator == parser.In) {
			resultSpans = applyInConstraint(resultSpans, c, colIdx, index, lastEnd)
		} else {
			if c.numColumns() != 1 {
				panic(fmt.Sprintf("non-IN constraint applies to %d columns", c.numColumns()))
			}
			dir, err := index.ColumnDirections[colIdx].toEncodingDirection()
			if err != nil {
				panic(err)
			}
			if c.start != nil {
				if dir == encoding.Ascending {
					encodeStartConstraintAscending(resultSpans, c.start)
				} else {
					encodeStartConstraintDescending(resultSpans, c.start)
				}
			}
			if c.end != nil {
				if dir == encoding.Ascending {
					encodeEndConstraintAscending(resultSpans, c.end, lastEnd)
				} else {
					encodeEndConstraintDescending(resultSpans, c.end, lastEnd)
				}
			}
		}
		colIdx += c.numColumns()
	}

	// If we had no end constraints, make it so that we scan the whole index.
	if len(constraints) == 0 || constraints[0].end == nil {
		for i := range resultSpans {
			resultSpans[i].end = resultSpans[i].end.PrefixEnd()
		}
	}

	// Remove any spans which are empty. This can happen for constraints such as
	// "a > 1 AND a < 2" which we do not simplify to false but which is treated
	// as "a >= 2 AND a < 2" for span generation.
	n := 0
	for _, s := range resultSpans {
		if bytes.Compare(s.start, s.end) < 0 {
			resultSpans[n] = s
			n++
		}
	}
	return resultSpans[:n]
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
			if tuple, ok := c.start.Right.(parser.DTuple); !ok || len(tuple) != 1 {
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
			right := c.start.Right.(parser.DTuple)[0]
			if _, ok := c.start.Left.(*parser.Tuple); ok {
				// We have something like `(a,b,c) IN (1,2,3)`
				rtuple := right.(parser.DTuple)
				for _, tupleIdx := range c.tupleMap {
					datums = append(datums, rtuple[tupleIdx])
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
func (oic orIndexConstraints) exactPrefix() int {
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
			if !(d.TypeEqual(iDatums[i]) && d.Compare(iDatums[i]) == 0) {
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
func applyConstraints(expr parser.Expr, constraints orIndexConstraints) parser.Expr {
	if len(constraints) != 1 {
		// We only support simplifying the expressions if there aren't multiple
		// disjunctions (top-level OR branches).
		return expr
	}
	v := &applyConstraintsVisitor{}
	for _, c := range constraints[0] {
		v.constraint = c
		expr, _ = parser.WalkExpr(v, expr)
		// We can only continue to apply the constraints if the constraints we have
		// applied so far are equality constraints. There are two cases to
		// consider: the first is that both the start and end constraints are
		// equality.
		if c.start == c.end {
			if c.start.Operator == parser.EQ {
				continue
			}
			// The second case is that both the start and end constraint are an IN
			// operator with only a single value.
			if c.start.Operator == parser.In && len(c.start.Right.(parser.DTuple)) == 1 {
				continue
			}
		}
		break
	}
	if expr == parser.DBool(true) {
		return nil
	}
	return expr
}

type applyConstraintsVisitor struct {
	constraint indexConstraint
}

func (v *applyConstraintsVisitor) VisitPre(expr parser.Expr) (recurse bool, newExpr parser.Expr) {
	switch t := expr.(type) {
	case *parser.AndExpr, *parser.NotExpr:
		return true, expr

	case *parser.ComparisonExpr:
		c := v.constraint.start
		if c == nil {
			return true, expr
		}
		if !varEqual(t.Left, c.Left) {
			return true, expr
		}
		if !isDatum(t.Right) || !isDatum(c.Right) {
			return true, expr
		}
		if tuple, ok := c.Left.(*parser.Tuple); ok {
			// Do not apply a constraint on a tuple which does not use the entire
			// tuple.
			//
			// TODO(peter): The current code is conservative. We could trim the
			// tuple instead.
			if len(tuple.Exprs) != len(v.constraint.tupleMap) {
				return true, expr
			}
		}

		datum := t.Right.(parser.Datum)
		cdatum := c.Right.(parser.Datum)

		switch t.Operator {
		case parser.EQ:
			if v.constraint.start != v.constraint.end {
				return true, expr
			}

			switch c.Operator {
			case parser.EQ:
				// Expr: "a = <val>", constraint: "a = <val>".
				if reflect.TypeOf(datum) != reflect.TypeOf(cdatum) {
					return true, expr
				}
				cmp := datum.Compare(cdatum)
				if cmp == 0 {
					return false, parser.DBool(true)
				}
			case parser.In:
				// Expr: "a = <val>", constraint: "a IN (<vals>)".
				ctuple := cdatum.(parser.DTuple)
				if reflect.TypeOf(datum) != reflect.TypeOf(ctuple[0]) {
					return true, expr
				}
				i := sort.Search(len(ctuple), func(i int) bool {
					return ctuple[i].(parser.Datum).Compare(datum) >= 0
				})
				if i < len(ctuple) && ctuple[i].Compare(datum) == 0 {
					return false, parser.DBool(true)
				}
			}

		case parser.In:
			if v.constraint.start != v.constraint.end {
				return true, expr
			}

			switch c.Operator {
			case parser.In:
				// Expr: "a IN (<vals>)", constraint: "a IN (<vals>)".
				if reflect.TypeOf(datum) != reflect.TypeOf(cdatum) {
					return true, expr
				}
				diff := diffSorted(datum.(parser.DTuple), cdatum.(parser.DTuple))
				if len(diff) == 0 {
					return false, parser.DBool(true)
				}
				t.Right = diff
			}

		case parser.IsNot:
			switch c.Operator {
			case parser.IsNot:
				if datum == parser.DNull && cdatum == parser.DNull {
					// Expr: "a IS NOT NULL", constraint: "a IS NOT NULL"
					return false, parser.DBool(true)
				}
			}
		}

	default:
		return false, expr
	}

	return true, expr
}

func (v *applyConstraintsVisitor) VisitPost(expr parser.Expr) parser.Expr {
	switch t := expr.(type) {
	case *parser.AndExpr:
		if t.Left == parser.DBool(true) && t.Right == parser.DBool(true) {
			return parser.DBool(true)
		} else if t.Left == parser.DBool(true) {
			return t.Right
		} else if t.Right == parser.DBool(true) {
			return t.Left
		}
	}

	return expr
}

func diffSorted(a, b parser.DTuple) parser.DTuple {
	var r parser.DTuple
	for len(a) > 0 && len(b) > 0 {
		switch a[0].Compare(b[0]) {
		case -1:
			r = append(r, a[0])
			a = a[1:]
		case 0:
			a = a[1:]
			b = b[1:]
		case 1:
			b = b[1:]
		}
	}
	return r
}
