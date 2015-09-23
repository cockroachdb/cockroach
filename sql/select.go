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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// Select selects rows from a single table. Select is the workhorse of the SQL
// statements. In the slowest and most general case, select must perform full
// table scans across multiple tables and sort and join the resulting rows on
// arbitrary columns. Full table scans can be avoided when indexes can be used
// to satisfy the where-clause.
//
// Privileges: SELECT on table
//   Notes: postgres requires SELECT. Also requires UPDATE on "FOR UPDATE".
//          mysql requires SELECT.
func (p *planner) Select(n *parser.Select) (planNode, error) {
	scan := &scanNode{planner: p, txn: p.txn}
	if err := scan.initFrom(p, n.From); err != nil {
		return nil, err
	}
	if err := scan.initWhere(n.Where); err != nil {
		return nil, err
	}
	if err := scan.initTargets(n.Exprs); err != nil {
		return nil, err
	}
	group, err := p.groupBy(n, scan)
	if err != nil {
		return nil, err
	}
	if group != nil && n.OrderBy != nil {
		// TODO(pmattis): orderBy currently uses deep knowledge of the
		// scanNode. Need to lift that out or make orderBy compatible with
		// groupNode as well.
		return nil, util.Errorf("TODO(pmattis): unimplemented ORDER BY with GROUP BY/aggregation")
	}
	sort, err := p.orderBy(n, scan)
	if err != nil {
		return nil, err
	}

	// TODO(pmattis): Consider aggregation functions during index
	// selection. Specifically, MIN(k) and MAX(k) where k is the first column in
	// an index can be satisfied with a single read.
	plan, err := p.selectIndex(scan, sort.Ordering())
	if err != nil {
		return nil, err
	}

	limit, err := p.limit(n, p.distinct(n, sort.wrap(group.wrap(plan))))
	if err != nil {
		return nil, err
	}
	return limit, nil
}

// selectIndex analyzes the scanNode to determine if there is an index
// available that can fulfill the query with a more restrictive scan.
//
// The current occurs in two passes. The first pass performs a limited form of
// value range propagation for the qvalues (i.e. the columns). The second pass
// takes the value range information and determines which indexes can fulfill
// the query (we currently only support covering indexes) and selects the
// "best" index from that set. The cost model based on keys per row, key size
// and number of index elements used.
func (p *planner) selectIndex(s *scanNode, ordering []int) (planNode, error) {
	if s.desc == nil || (s.filter == nil && ordering == nil) {
		// No table or no where-clause and no ordering.
		s.initOrdering()
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
		exprs := analyzeExpr(s.filter)
		if log.V(2) {
			log.Infof("analyzeExpr: %s -> %s", s.filter, exprs)
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
			c.analyzeRanges(exprs)
		}
	}

	if ordering != nil {
		for _, c := range candidates {
			c.analyzeOrdering(s, ordering)
		}
	}

	sort.Sort(indexInfoByCost(candidates))

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
	s.spans = makeSpans(c.constraints, c.desc.ID, c.index.ID)
	s.reverse = c.reverse

	if log.V(3) {
		for i, span := range s.spans {
			log.Infof("%s/%d: start=%s end=%s", c.index.Name, i, span.start, span.end)
		}
	}

	if c.covering {
		s.initOrdering()
		return s, nil
	}
	return makeIndexJoin(s)
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

type indexConstraints []indexConstraint

func (c indexConstraints) String() string {
	var buf bytes.Buffer
	buf.WriteString("[")
	for i := range c {
		if i > 0 {
			buf.WriteString(", ")
		}
		if c[i].start != nil {
			fmt.Fprintf(&buf, "%s", c[i].start)
		}
		if c[i].end != nil && c[i].end != c[i].start {
			if c[i].start != nil {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "%s", c[i].end)
		}
	}
	buf.WriteString("]")
	return buf.String()
}

type indexInfo struct {
	desc        *TableDescriptor
	index       *IndexDescriptor
	constraints indexConstraints
	cost        float64
	covering    bool // indicates whether the index covers the required qvalues
	reverse     bool
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

// analyzeRanges examines the range map to determine the cost of using the
// index.
func (v *indexInfo) analyzeRanges(exprs []parser.Exprs) {
	v.makeConstraints(exprs)

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
	// Compute the ordering provided by the index.
	indexOrdering := scan.computeOrdering(v.index.fullColumnIDs())

	// Compute how much of the index ordering matches the requested ordering for
	// both forward and reverse scans.
	fwdMatch, revMatch := 0, 0
	for ; fwdMatch < len(ordering); fwdMatch++ {
		if fwdMatch >= len(indexOrdering) || ordering[fwdMatch] != indexOrdering[fwdMatch] {
			break
		}
	}
	for ; revMatch < len(ordering); revMatch++ {
		if revMatch >= len(indexOrdering) || ordering[revMatch] != -indexOrdering[revMatch] {
			break
		}
	}

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
// prefix of the columns that make up the index. For example, consider an index
// on the columns (a, b, c). For the expressions "a > 1 AND b > 2" we would
// have the constraints:
//
//   {a: {start: > 1}
//
// Why is there no constraint on "b"? Because the start constraint was > and
// such a constraint does not allow us to consider further columns in the
// index. What about the expression "a >= 1 AND b > 2":
//
//   {a: {start: >= 1}, b: {start: > 2}}
//
// Start constraints look for comparison expressions with the operators >, >=,
// = or IN. End constraints look for comparison expressions with the operators
// <, <=, = or IN.
func (v *indexInfo) makeConstraints(exprs []parser.Exprs) {
	if len(exprs) != 1 {
		return
	}

	andExprs := exprs[0]
	startDone := false
	endDone := false

	for i := 0; i < len(v.index.ColumnIDs); i++ {
		colID := v.index.ColumnIDs[i]
		var constraint indexConstraint

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
					i += (len(tupleMap) - 1)
				}

				if _, ok := c.Right.(parser.Datum); !ok {
					continue
				}
				if c.Operator == parser.NE {
					// Give-up when we encounter a != expression.
					return
				}
				if tupleMap != nil && c.Operator != parser.In {
					// We can only handle tuples in IN expressions.
					continue
				}

				switch c.Operator {
				case parser.EQ:
					if !startDone {
						constraint.start = c
					}
					if !endDone {
						constraint.end = c
					}
				case parser.In:
					if !startDone && (constraint.start == nil || constraint.start.Operator != parser.EQ) {
						constraint.start = c
						constraint.tupleMap = tupleMap
					}
					if !endDone && (constraint.end == nil || constraint.end.Operator != parser.EQ) {
						constraint.end = c
						constraint.tupleMap = tupleMap
					}
				case parser.GT, parser.GE:
					if !startDone && constraint.start == nil {
						constraint.start = c
					}
				case parser.LT, parser.LE:
					if !endDone && constraint.end == nil {
						constraint.end = c
					}
				}
			}
		}

		if constraint.start != nil && constraint.start.Operator == parser.GT {
			// Transform a > constraint into a >= constraint so that we play
			// nicer with the inclusive nature of the scan start key.
			//
			// TODO(pmattis): It would be more obvious to perform this
			// transform in simplifyComparisonExpr, but doing so there
			// eliminates some of the other simplifications. For example, "a <
			// 1 OR a > 1" currently simplifies to "a != 1", but if we
			// performed this transform in simpilfyComparisonExpr it would
			// simplify to "a < 1 OR a >= 2" which is also the same as "a !=
			// 1", but not so obvious based on comparisons of the constants.
			constraint.start = &parser.ComparisonExpr{
				Operator: parser.GE,
				Left:     constraint.start.Left,
				Right:    constraint.start.Right.(parser.Datum).Next(),
			}
		}
		if constraint.end != nil && constraint.end.Operator == parser.LT {
			endDone = true
		}

		if constraint.start != nil || constraint.end != nil {
			v.constraints = append(v.constraints, constraint)
		}

		if constraint.start == nil {
			startDone = true
		}
		if constraint.end == nil {
			endDone = true
		}
		if startDone && endDone {
			break
		}
	}
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

// makeSpans constructs the spans for an index given a set of constraints.
func makeSpans(constraints indexConstraints, tableID ID, indexID IndexID) []span {
	prefix := proto.Key(MakeIndexKeyPrefix(tableID, indexID))
	spans := []span{{
		start: append(proto.Key(nil), prefix...),
		end:   append(proto.Key(nil), prefix...),
	}}
	var buf [100]byte

	for i, c := range constraints {
		// Is this the last end constraint? We perform special processing on the
		// last end constraint to account for the exclusive nature of the scan end
		// key.
		lastEnd := c.end != nil &&
			(i+1 == len(constraints) || constraints[i+1].end == nil)

		if (c.start != nil && c.start.Operator == parser.In) ||
			(c.end != nil && c.end.Operator == parser.In) {
			var e *parser.ComparisonExpr
			if c.start != nil && c.start.Operator == parser.In {
				e = c.start
			} else {
				e = c.end
			}

			// Special handling of IN exprssions. Such expressions apply to both the
			// start and end key, but also cause an explosion in the number of spans
			// searched within an index.
			tuple, ok := e.Right.(parser.DTuple)
			if !ok {
				break
			}

			// For each of the existing spans and for each value in the tuple, create
			// a new span.
			existingSpans := spans
			spans = make([]span, 0, len(existingSpans)*len(tuple))
			for _, datum := range tuple {
				var start, end []byte

				switch t := datum.(type) {
				case parser.DTuple:
					start = buf[:0]
					for _, i := range c.tupleMap {
						var err error
						if start, err = encodeTableKey(start, t[i]); err != nil {
							panic(err)
						}
					}

					end = start
					if lastEnd {
						end = nil
						for i := range c.tupleMap {
							d := t[c.tupleMap[i]]
							if i+1 == len(c.tupleMap) {
								d = d.Next()
							}
							var err error
							if end, err = encodeTableKey(end, d); err != nil {
								panic(err)
							}
						}
					}

				default:
					var err error
					if start, err = encodeTableKey(buf[:0], datum); err != nil {
						panic(err)
					}

					end = start
					if lastEnd {
						var err error
						if end, err = encodeTableKey(nil, datum.Next()); err != nil {
							panic(err)
						}
					}
				}

				for _, s := range existingSpans {
					if c.start != nil {
						s.start = append(append(proto.Key(nil), s.start...), start...)
					}
					if c.end != nil {
						s.end = append(append(proto.Key(nil), s.end...), end...)
					}
					spans = append(spans, s)
				}
			}

			continue
		}

		if c.start != nil {
			// We have a start constraint.
			if datum, ok := c.start.Right.(parser.Datum); ok {
				key, err := encodeTableKey(buf[:0], datum)
				if err != nil {
					panic(err)
				}
				// Append the constraint to all of the existing spans.
				for i := range spans {
					spans[i].start = append(spans[i].start, key...)
				}
			}
		}

		if c.end != nil {
			// We have an end constraint.
			if datum, ok := c.end.Right.(parser.Datum); ok {
				if lastEnd && c.end.Operator != parser.LT {
					datum = datum.Next()
				}
				key, err := encodeTableKey(buf[:0], datum)
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

	if len(constraints) == 0 || constraints[0].end == nil {
		for i := range spans {
			spans[i].end = spans[i].end.PrefixEnd()
		}
	}

	return spans
}
