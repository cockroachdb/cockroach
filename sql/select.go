// Copyright 2015 The Cockroach Authors.
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

type selectNode struct {
	planner *planner

	// A planNode containing the "from" data (normally a scanNode). For
	// performance purposes, this node can be aware of the filters, grouping
	// etc.
	from planNode

	pErr *roachpb.Error
}

// For now scanNode implements all the logic and selectNode just proxies the
// calls.

func (s *selectNode) Columns() []column {
	return s.from.Columns()
}

func (s *selectNode) Ordering() (ordering []int, prefix int) {
	return s.from.Ordering()
}

func (s *selectNode) Values() parser.DTuple {
	return s.from.Values()
}

func (s *selectNode) Next() bool {
	return s.from.Next()
}

func (s *selectNode) PErr() *roachpb.Error {
	if s.pErr != nil {
		return s.pErr
	}
	return s.from.PErr()
}

func (s *selectNode) ExplainPlan() (name, description string, children []planNode) {
	return s.from.ExplainPlan()
}

// Select selects rows from a single table. Select is the workhorse of the SQL
// statements. In the slowest and most general case, select must perform full
// table scans across multiple tables and sort and join the resulting rows on
// arbitrary columns. Full table scans can be avoided when indexes can be used
// to satisfy the where-clause.
//
// Privileges: SELECT on table
//   Notes: postgres requires SELECT. Also requires UPDATE on "FOR UPDATE".
//          mysql requires SELECT.
func (p *planner) Select(parsed *parser.Select) (planNode, *roachpb.Error) {
	node := &selectNode{planner: p}
	return p.initSelect(node, parsed)
}

func (p *planner) initSelect(s *selectNode, parsed *parser.Select) (planNode, *roachpb.Error) {
	if pErr := s.initFrom(p, parsed); pErr != nil {
		return nil, pErr
	}

	// TODO(radu): for now we assume from is always a scanNode
	scan := s.from.(*scanNode)

	// NB: both orderBy and groupBy are passed and can modify `scan` but orderBy must do so first.
	sort, pErr := p.orderBy(parsed, scan)
	if pErr != nil {
		return nil, pErr
	}
	group, pErr := p.groupBy(parsed, scan)
	if pErr != nil {
		return nil, pErr
	}

	if scan.filter != nil && group != nil {
		// Allow the group-by to add an implicit "IS NOT NULL" filter.
		scan.filter = group.isNotNullFilter(scan.filter)
	}

	// Get the ordering for index selection (if any).
	var ordering []int
	var grouping bool

	if group != nil {
		ordering = group.desiredOrdering
		grouping = true
	} else if sort != nil {
		ordering, _ = sort.Ordering()
	}

	plan, pErr := p.selectIndex(scan, ordering, grouping)
	if pErr != nil {
		return nil, pErr
	}

	// Update s.from with the new plan.
	s.from = plan

	// Wrap this node as necessary.
	limit, pErr := p.limit(parsed, p.distinct(parsed, sort.wrap(group.wrap(s))))
	if pErr != nil {
		return nil, pErr
	}
	return limit, nil
}

// Initializes the from node, given the parsed select expression
func (s *selectNode) initFrom(p *planner, parsed *parser.Select) *roachpb.Error {
	scan := &scanNode{planner: p, txn: p.txn}

	from := parsed.From
	switch len(from) {
	case 0:
		// Nothing to do

	case 1:
		ate, ok := from[0].(*parser.AliasedTableExpr)
		if !ok {
			return roachpb.NewErrorf("TODO(pmattis): unsupported FROM: %s", from)
		}
		s.pErr = scan.initTableExpr(p, ate)
		if s.pErr != nil {
			return s.pErr
		}
	default:
		s.pErr = roachpb.NewErrorf("TODO(pmattis): unsupported FROM: %s", from)
		return s.pErr
	}

	s.pErr = scan.init(parsed)
	if s.pErr != nil {
		return s.pErr
	}
	s.from = scan
	return nil
}

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
// If grouping is true, the ordering is the desired ordering for grouping.
func (p *planner) selectIndex(s *scanNode, ordering []int, grouping bool) (planNode, *roachpb.Error) {
	if s.desc == nil || (s.filter == nil && ordering == nil) {
		// No table or no where-clause and no ordering.
		s.initOrdering(0)
		return s, nil
	}

	candidates := make([]*indexInfo, 0, len(s.desc.Indexes)+1)
	if s.isSecondaryIndex {
		// An explicit secondary index was requested. Only add it to the candidate
		// indexes list.
		candidates = append(candidates, &indexInfo{
			desc:  s.desc,
			index: s.index,
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
				s.desc = nil
				s.index = nil
				return s, nil
			}
		}

		// If the simplified expression is equivalent and there is a single
		// disjunction, use it for the filter instead of the original expression.
		if equivalent && len(exprs) == 1 {
			s.filter = joinAndExprs(exprs[0])
		}

		// TODO(pmattis): If "len(exprs) > 1" then we have multiple disjunctive
		// expressions. For example, "a=1 OR a=3" will get translated into "[[a=1],
		// [a=3]]". We need to perform index selection independently for each of
		// the disjunctive expressions and then take the resulting index info and
		// determine if we're performing distinct scans in the indexes or if the
		// scans overlap. If the scans overlap we'll need to union the output
		// keys. If the scans are distinct (such as in the "a=1 OR a=3" case) then
		// we can sort the scans by start key.
		//
		// There are complexities: if there are a large number of disjunctive
		// expressions we should limit how many indexes we use. We probably should
		// optimize the common case of "a IN (1, 3)" so that we only perform index
		// selection once even though we generate multiple scan ranges for the
		// index.
		//
		// Each disjunctive expression might generate multiple ranges of an index
		// to scan. An examples of this is "a IN (1, 2, 3)".

		for _, c := range candidates {
			c.analyzeExprs(exprs)
		}
	}

	if ordering != nil {
		for _, c := range candidates {
			c.analyzeOrdering(s, ordering)
		}
	}

	indexInfoByCost(candidates).Sort()

	if log.V(2) {
		for i, c := range candidates {
			log.Infof("%d: selectIndex(%s): cost=%v constraints=%s",
				i, c.index.Name, c.cost, c.constraints)
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
		s.desc = nil
		s.index = nil
		return s, nil
	}
	s.filter = applyConstraints(s.filter, c.constraints)
	s.reverse = c.reverse

	var plan planNode
	if c.covering {
		s.initOrdering(c.exactPrefix)
		plan = s
	} else {
		var pErr *roachpb.Error
		plan, pErr = makeIndexJoin(s, c.exactPrefix)
		if pErr != nil {
			return nil, pErr
		}
	}

	if grouping && len(ordering) == 1 && len(s.spans) == 1 && s.filter == nil {
		// If grouping has a desired order and there is a single span for which the
		// filter is true, check to see if the ordering matches the desired
		// ordering. If it does we can limit the scan to a single key.
		existingOrdering, prefix := plan.Ordering()
		match := computeOrderingMatch(ordering, existingOrdering, prefix, +1)
		if match == 1 {
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

func (c indexConstraint) String() string {
	var buf bytes.Buffer
	if c.start != nil {
		fmt.Fprintf(&buf, "%s", c.start)
	}
	if c.end != nil && c.end != c.start {
		if c.start != nil {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "%s", c.end)
	}
	return buf.String()
}

// indexConstraints is a set of constraints on a prefix of the columns
// in a single index. The constraints are ordered as the columns in the index.
// A constraint referencing a tuple accounts for several columns (the size of
// its .tupleMap).
type indexConstraints []indexConstraint

func (c indexConstraints) String() string {
	var buf bytes.Buffer
	buf.WriteString("[")
	for i := range c {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(c[i].String())
	}
	buf.WriteString("]")
	return buf.String()
}

type indexInfo struct {
	desc        *TableDescriptor
	index       *IndexDescriptor
	constraints indexConstraints
	cost        float64
	covering    bool // Does the index cover the required qvalues?
	reverse     bool
	exactPrefix int
}

func (v *indexInfo) init(s *scanNode) {
	v.covering = v.isCoveringIndex(s.qvals)

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
			v.cost *= 10
		}
	}
}

// analyzeExprs examines the range map to determine the cost of using the
// index.
func (v *indexInfo) analyzeExprs(exprs []parser.Exprs) {
	if err := v.makeConstraints(exprs); err != nil {
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
		v.cost *= float64(len(v.index.ColumnIDs)) / float64(len(v.constraints))
	}
}

// analyzeOrdering analyzes the ordering provided by the index and determines
// if it matches the ordering requested by the query. Non-matching orderings
// increase the cost of using the index.
func (v *indexInfo) analyzeOrdering(scan *scanNode, ordering []int) {
	// Compute the prefix of the index for which we have exact constraints. This
	// prefix is inconsequential for ordering because the values are identical.
	v.exactPrefix = exactPrefix(v.constraints)

	// Compute the ordering provided by the index.
	colIds, _ := v.index.fullColumnIDs()
	indexOrdering := scan.computeOrdering(colIds)

	// Compute how much of the index ordering matches the requested ordering for
	// both forward and reverse scans.
	fwdMatch := computeOrderingMatch(ordering, indexOrdering, v.exactPrefix, +1)
	revMatch := computeOrderingMatch(ordering, indexOrdering, v.exactPrefix, -1)

	// Weight the cost by how much of the ordering matched.
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
	weight := float64(len(ordering)+1) / float64(match+1)
	v.cost *= weight

	if log.V(2) {
		log.Infof("%s: analyzeOrdering: weight=%0.2f reverse=%v index=%d requested=%d",
			v.index.Name, weight, v.reverse, indexOrdering, ordering)
	}
}

// makeConstraints populates the indexInfo.constraints field based on the
// analyzed expressions. The constraints are a start and end expressions for a
// prefix of the columns that make up the index. For example, consider
// the expression "a >= 1 AND b >= 2":
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
// makeConstraints takes into account the direction of the columns in the index.
// For ascending cols, start constraints look for comparison expressions with the
// operators >=, = or IN and end constraints look for comparison expressions
// with the operators <, <=, = or IN. Vice versa for descending cols.
//
// Whenever possible, < and > are converted to <= and >=, respectively.
// This is because we can use inclusive constraints better than exclusive ones;
// with inclusive constraints we can continue accumulate constraints for
// next columns. Not so with exclusive ones: Consider "a < 1 AND b < 2".
// "a < 1" will be encoded as an exclusive span end; if we were to append
// anything about "b" to it, that would be incorrect.
// Note that it's not always possible to transform "<" to "<=", because some
// types do not support the Prev() operation.
// So, the resulting constraints will never contain ">". They might contain
// "<", in which case that will be the last constraint with `.end` filled.
//
// TODO(pmattis): It would be more obvious to perform this transform in
// simplifyComparisonExpr, but doing so there eliminates some of the other
// simplifications. For example, "a < 1 OR a > 1" currently simplifies to "a !=
// 1", but if we performed this transform in simpilfyComparisonExpr it would
// simplify to "a < 1 OR a >= 2" which is also the same as "a != 1", but not so
// obvious based on comparisons of the constants.
func (v *indexInfo) makeConstraints(exprs []parser.Exprs) error {
	if len(exprs) != 1 {
		// TODO(andrei): what should we do with ORs?
		return nil
	}

	andExprs := exprs[0]
	trueStartDone := false
	trueEndDone := false

	for i := 0; i < len(v.index.ColumnIDs); i++ {
		colID := v.index.ColumnIDs[i]
		var colDir encoding.Direction
		var err error
		if colDir, err = v.index.ColumnDirections[i].toEncodingDirection(); err != nil {
			return err
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
				switch t := c.Left.(type) {
				case *qvalue:
					if t.col.ID != colID {
						// This expression refers to a column other than the one we're
						// looking for.
						continue
					}

				case parser.Tuple:
					// If we have a tuple comparison we need to rearrange the comparison
					// so that the order of the columns in the tuple matches the order in
					// the index. For example, for an index on (a, b), the tuple
					// comparison "(b, a) = (1, 2)" would be rewritten as "(a, b) = (2,
					// 1)". Note that we don't actually need to rewrite the comparison,
					// but simply provide a mapping from the order in the tuple to the
					// order in the index.
					for _, colID := range v.index.ColumnIDs[i:] {
						idx := findColumnInTuple(t, colID)
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
					for _, c := range v.constraints {
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
					} else {
						*startExpr = &parser.ComparisonExpr{
							Operator: parser.GE,
							Left:     c.Left,
							Right:    c.Right.(parser.Datum).Next(),
						}
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
			v.constraints = append(v.constraints, constraint)
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
	return nil
}

// isCoveringIndex returns true if all of the columns referenced by the target
// expressions and where clause are contained within the index. This allows a
// scan of only the index to be performed without requiring subsequent lookup
// of the full row.
func (v *indexInfo) isCoveringIndex(qvals qvalMap) bool {
	if v.index == &v.desc.PrimaryIndex {
		// The primary key index always covers all of the columns.
		return true
	}

	for colID := range qvals {
		if !v.index.containsColumnID(colID) {
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

func encodeStartConstraintAscending(spans []span, c *parser.ComparisonExpr) {
	switch c.Operator {
	case parser.IsNot:
		// A IS NOT NULL expression allows us to constrain the start of
		// the range to not include NULL.
		for i := range spans {
			spans[i].start = encoding.EncodeNotNullAscending(spans[i].start)
		}
	case parser.GT:
		panic("'>' operators should have been transformed to '>='.")
	case parser.NE:
		panic("'!=' operators should have been transformed to 'IS NOT NULL'")
	default:
		if datum, ok := c.Right.(parser.Datum); ok {
			key, err := encodeTableKey(nil, datum, encoding.Ascending)
			if err != nil {
				panic(err)
			}
			// Append the constraint to all of the existing spans.
			for i := range spans {
				spans[i].start = append(spans[i].start, key...)
			}
		}
	}
}

func encodeEndConstraintAscending(spans []span, c *parser.ComparisonExpr,
	isLastEndConstraint bool) {
	switch c.Operator {
	case parser.Is:
		// An IS NULL expressions allows us to constrain the end of the range
		// to stop at NULL.
		if c.Right != parser.DNull {
			panic("Expected NULL operand for IS operator.")
		}
		for i := range spans {
			spans[i].end = encoding.EncodeNotNullAscending(spans[i].end)
		}
	default:
		if datum, ok := c.Right.(parser.Datum); ok {
			if c.Operator != parser.LT {
				for i := range spans {
					spans[i].end = encodeInclusiveEndValue(
						spans[i].end, datum, encoding.Ascending, isLastEndConstraint)
				}
				break
			}
			if !isLastEndConstraint {
				panic("Can't have other end constraints after a '<' constraint.")
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
}

func encodeStartConstraintDescending(
	spans []span, c *parser.ComparisonExpr) {
	switch c.Operator {
	case parser.Is:
		// An IS NULL expressions allows us to constrain the start of the range
		// to begin at NULL.
		if c.Right != parser.DNull {
			panic("Expected NULL operand for IS operator.")
		}
		for i := range spans {
			spans[i].start = encoding.EncodeNullDescending(spans[i].start)
		}
	case parser.LE, parser.EQ:
		if datum, ok := c.Right.(parser.Datum); ok {
			key, pErr := encodeTableKey(nil, datum, encoding.Descending)
			if pErr != nil {
				panic(pErr)
			}
			// Append the constraint to all of the existing spans.
			for i := range spans {
				spans[i].start = append(spans[i].start, key...)
			}
		}
	case parser.LT:
		// A "<" constraint is the last start constraint. Since the constraint
		// is exclusive and the start key is inclusive, we're going to apply
		// a .PrefixEnd().
		if datum, ok := c.Right.(parser.Datum); ok {
			key, pErr := encodeTableKey(nil, datum, encoding.Descending)
			if pErr != nil {
				panic(pErr)
			}
			// Append the constraint to all of the existing spans.
			for i := range spans {
				spans[i].start = append(spans[i].start, key...)
				spans[i].start = spans[i].start.PrefixEnd()
			}
		}
	default:
		panic(fmt.Errorf("unexpected operator: %s", c.String()))
	}
}

func encodeEndConstraintDescending(spans []span, c *parser.ComparisonExpr,
	isLastEndConstraint bool) {
	switch c.Operator {
	case parser.IsNot:
		// An IS NULL expressions allows us to constrain the end of the range
		// to stop at NULL.
		if c.Right != parser.DNull {
			panic("Expected NULL operand for IS NOT operator.")
		}
		for i := range spans {
			spans[i].end = encoding.EncodeNotNullDescending(spans[i].end)
		}
	case parser.GE, parser.EQ:
		datum := c.Right.(parser.Datum)
		for i := range spans {
			spans[i].end = encodeInclusiveEndValue(
				spans[i].end, datum, encoding.Descending, isLastEndConstraint)
		}
	case parser.GT:
		panic("'>' operators should have been transformed to '>='.")
	default:
		panic(fmt.Errorf("unexpected operator: %s", c.String()))
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
			if datum.IsMax() {
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
// Returns the exploded spans and the number of index columns covered
// by this constraint (i.e. 1, if the left side is a qvalue or
// len(tupleMap) if it's a tuple).
func applyInConstraint(spans []span, c indexConstraint, firstCol int,
	index *IndexDescriptor, isLastEndConstraint bool) ([]span, int) {
	var e *parser.ComparisonExpr
	var coveredColumns int
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
			coveredColumns = len(c.tupleMap)
			for j, tupleIdx := range c.tupleMap {
				var err error
				var colDir encoding.Direction
				if colDir, err = index.ColumnDirections[firstCol+j].toEncodingDirection(); err != nil {
					panic(err)
				}

				var pErr *roachpb.Error
				if start, pErr = encodeTableKey(start, t[tupleIdx], colDir); pErr != nil {
					panic(pErr)
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
			coveredColumns = 1
			var pErr *roachpb.Error
			if start, pErr = encodeTableKey(nil, datum, colDir); pErr != nil {
				panic(pErr)
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
	return spans, coveredColumns
}

// makeSpans constructs the spans for an index given a set of constraints.
func makeSpans(constraints indexConstraints,
	tableID ID, index *IndexDescriptor) []span {
	prefix := roachpb.Key(MakeIndexKeyPrefix(tableID, index.ID))
	// We have one constraint per column, so each contributes something
	// to the start and/or the end key of the span.
	// But we also have (...) IN <tuple> constraints that span multiple columns.
	// These constraints split each span, and that's how we can end up with
	// multiple spans.
	spans := []span{{
		start: append(roachpb.Key(nil), prefix...),
		end:   append(roachpb.Key(nil), prefix...),
	}}

	colIdx := -1
	for i, c := range constraints {
		colIdx++
		// We perform special processing on the last end constraint to account for
		// the exclusive nature of the scan end key.
		lastEnd := (c.end != nil) &&
			(i+1 == len(constraints) || constraints[i+1].end == nil)

		// IN is handled separately, since it can affect multiple columns.
		if ((c.start != nil) && (c.start.Operator == parser.In)) ||
			((c.end != nil) && (c.end.Operator == parser.IN)) {
			var coveredCols int
			spans, coveredCols = applyInConstraint(spans, c, colIdx, index, lastEnd)
			// Skip over all the columns contained in the tuple.
			colIdx += coveredCols - 1
			continue
		}
		var dir encoding.Direction
		var err error
		if dir, err = index.ColumnDirections[colIdx].toEncodingDirection(); err != nil {
			panic(err)
		}
		if c.start != nil {
			if dir == encoding.Ascending {
				encodeStartConstraintAscending(spans, c.start)
			} else {
				encodeStartConstraintDescending(spans, c.start)
			}
		}
		if c.end != nil {
			if dir == encoding.Ascending {
				encodeEndConstraintAscending(spans, c.end, lastEnd)
			} else {
				encodeEndConstraintDescending(spans, c.end, lastEnd)
			}
		}
	}

	// If we had no end constraints, make it so that we scan the whole index.
	if len(constraints) == 0 || constraints[0].end == nil {
		for i := range spans {
			spans[i].end = spans[i].end.PrefixEnd()
		}
	}

	// Remove any spans which are empty. This can happen for constraints such as
	// "a > 1 AND a < 2" which we do not simplify to false but which is treated
	// as "a >= 2 AND a < 2" for span generation.
	n := 0
	for _, s := range spans {
		if bytes.Compare(s.start, s.end) < 0 {
			spans[n] = s
			n++
		}
	}
	spans = spans[:n]
	return spans
}

// exactPrefix returns the count of the columns of the index for which an exact
// prefix match was requested. For example, if an index was defined on the
// columns (a, b, c) and the WHERE clause was "(a, b) = (1, 2)", exactPrefix()
// would return 2.
func exactPrefix(constraints []indexConstraint) int {
	prefix := 0
	for _, c := range constraints {
		if c.start == nil || c.end == nil || c.start != c.end {
			return prefix
		}
		switch c.start.Operator {
		case parser.EQ:
			prefix++
			continue
		case parser.In:
			if tuple, ok := c.start.Right.(parser.DTuple); !ok || len(tuple) != 1 {
				return prefix
			}
			if _, ok := c.start.Left.(parser.Tuple); ok {
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

// applyConstraints applies the constraints on values specified by constraints
// to an expression, simplifying the expression where possible. For example, if
// the expression is "a = 1" and the constraint is "a = 1", the expression can
// be simplified to "true". If the expression is "a = 1 AND b > 2" and the
// constraint is "a = 1", the expression is simplified to "b > 2".
//
// Note that applyConstraints currently only handles simple cases.
func applyConstraints(expr parser.Expr, constraints indexConstraints) parser.Expr {
	v := &applyConstraintsVisitor{}
	for _, c := range constraints {
		v.constraint = c
		expr = parser.WalkExpr(v, expr)
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

func (v *applyConstraintsVisitor) Visit(expr parser.Expr, pre bool) (parser.Visitor, parser.Expr) {
	if pre {
		switch t := expr.(type) {
		case *parser.AndExpr, *parser.NotExpr:
			return v, expr

		case *parser.ComparisonExpr:
			c := v.constraint.start
			if c == nil {
				return v, expr
			}
			if !varEqual(t.Left, c.Left) {
				return v, expr
			}
			if !isDatum(t.Right) || !isDatum(c.Right) {
				return v, expr
			}
			if tuple, ok := c.Left.(parser.Tuple); ok {
				// Do not apply a constraint on a tuple which does not use the entire
				// tuple.
				//
				// TODO(peter): The current code is conservative. We could trim the
				// tuple instead.
				if len(tuple) != len(v.constraint.tupleMap) {
					return v, expr
				}
			}

			datum := t.Right.(parser.Datum)
			cdatum := c.Right.(parser.Datum)

			switch t.Operator {
			case parser.EQ:
				if v.constraint.start != v.constraint.end {
					return v, expr
				}

				switch c.Operator {
				case parser.EQ:
					// Expr: "a = <val>", constraint: "a = <val>".
					if reflect.TypeOf(datum) != reflect.TypeOf(cdatum) {
						return v, expr
					}
					cmp := datum.Compare(cdatum)
					if cmp == 0 {
						return nil, parser.DBool(true)
					}
				case parser.In:
					// Expr: "a = <val>", constraint: "a IN (<vals>)".
					ctuple := cdatum.(parser.DTuple)
					if reflect.TypeOf(datum) != reflect.TypeOf(ctuple[0]) {
						return v, expr
					}
					i := sort.Search(len(ctuple), func(i int) bool {
						return ctuple[i].(parser.Datum).Compare(datum) >= 0
					})
					if i < len(ctuple) && ctuple[i].Compare(datum) == 0 {
						return nil, parser.DBool(true)
					}
				}

			case parser.In:
				if v.constraint.start != v.constraint.end {
					return v, expr
				}

				switch c.Operator {
				case parser.In:
					// Expr: "a IN (<vals>)", constraint: "a IN (<vals>)".
					if reflect.TypeOf(datum) != reflect.TypeOf(cdatum) {
						return v, expr
					}
					diff := diffSorted(datum.(parser.DTuple), cdatum.(parser.DTuple))
					if len(diff) == 0 {
						return nil, parser.DBool(true)
					}
					t.Right = diff
				}

			case parser.IsNot:
				switch c.Operator {
				case parser.IsNot:
					if datum == parser.DNull && cdatum == parser.DNull {
						// Expr: "a IS NOT NULL", constraint: "a IS NOT NULL"
						return nil, parser.DBool(true)
					}
				}
			}

		default:
			return nil, expr
		}

		return v, expr
	}

	switch t := expr.(type) {
	case *parser.AndExpr:
		if t.Left == parser.DBool(true) && t.Right == parser.DBool(true) {
			return nil, parser.DBool(true)
		} else if t.Left == parser.DBool(true) {
			return nil, t.Right
		} else if t.Right == parser.DBool(true) {
			return nil, t.Left
		}
	}

	return v, expr
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
