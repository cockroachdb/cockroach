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

// fromNode is a planNode that appears in the FROM part of a SELECT query.
type fromNode interface {
	planNode
	isColumnHidden(idx int) bool
}

var _ planNode = &scanNode{}
var _ planNode = &indexJoinNode{}

// fromInfo contains the information for a select "source" (FROM).
type fromInfo struct {
	// node which can be used to retrieve the data (normally a scanNode). For performance purposes,
	// this node can be aware of the filters, grouping etc.
	node fromNode

	// alias (if no alias is given and the source is a table, this is the table name)
	alias string

	// resultColumns which match the node.Columns() 1-to-1. However the column names might be
	// different if the statement renames them using AS.
	columns []ResultColumn
}

// selectNode encapsulates the core logic of a select statement: retrieving filtered results from
// the sources. Grouping, sorting, distinct and limits are implemented on top of this node (as
// wrappers), though these are taken into consideration at the selectNode level for optimization
// (e.g. index selection).
type selectNode struct {
	planner *planner

	from fromInfo

	// Map of qvalues encountered in expressions.
	qvals qvalMap

	pErr *roachpb.Error

	// Rendering expressions for rows and corresponding output columns.
	render  []parser.Expr
	columns []ResultColumn

	// The rendered row, with one value for each render expression.
	row parser.DTuple

	// Filtering expression for rows.
	filter parser.Expr

	// The number of initial columns - before adding any internal render targets for grouping or
	// ordering. The original columns are columns[:numOriginalCols], the internally added ones are
	// columns[numOriginalCols:].
	numOriginalCols int

	explain explainMode

	ordering orderingInfo
}

// Columns for explainDebug mode.
var debugColumns = []ResultColumn{
	{Name: "RowIdx", Typ: parser.DummyInt},
	{Name: "Key", Typ: parser.DummyString},
	{Name: "Value", Typ: parser.DummyString},
	{Name: "Output", Typ: parser.DummyBool},
}

func (s *selectNode) Columns() []ResultColumn {
	if s.explain == explainDebug {
		return debugColumns
	}
	return s.columns
}

func (s *selectNode) Ordering() orderingInfo {
	return s.ordering
}

func (s *selectNode) Values() parser.DTuple {
	return s.row
}

func (s *selectNode) Next() bool {
	for {
		if !s.from.node.Next() {
			return false
		}
		row := s.from.node.Values()

		if s.explain == explainDebug {
			if len(s.row) != 4 {
				s.row = make(parser.DTuple, 4)
			}
			debugValues := row[len(row)-4:]

			if debugValues[3] != parser.DNull && s.filter != nil {
				// We are at the end of the row and we have a filtering expression
				s.populateQVals(row)
				output := s.filterRow()
				if s.pErr != nil {
					return false
				}
				debugValues[3] = parser.DBool(output)
			}
			copy(s.row, debugValues)
			return true
		}

		s.populateQVals(row)
		output := s.filterRow()
		if s.pErr != nil {
			return false
		}

		if output {
			s.renderRow()
			return true
		}
		// Row was filtered out; grab the next row.
	}
}

func (s *selectNode) PErr() *roachpb.Error {
	if s.pErr != nil {
		return s.pErr
	}
	return s.from.node.PErr()
}

func (s *selectNode) ExplainPlan() (name, description string, children []planNode) {
	return s.from.node.ExplainPlan()
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
	s.qvals = make(qvalMap)

	if pErr := s.initFrom(p, parsed); pErr != nil {
		return nil, pErr
	}

	if pErr := s.initTargets(parsed.Exprs); pErr != nil {
		return nil, pErr
	}

	if pErr := s.initWhere(parsed.Where); pErr != nil {
		return nil, pErr
	}

	// TODO(radu): for now we assume from is always a scanNode
	scan := s.from.node.(*scanNode)

	// NB: both orderBy and groupBy are passed and can modify `scan` but orderBy must do so first.
	sort, pErr := p.orderBy(parsed, s)
	if pErr != nil {
		return nil, pErr
	}
	group, pErr := p.groupBy(parsed, s)
	if pErr != nil {
		return nil, pErr
	}

	if s.filter != nil && group != nil {
		// Allow the group-by to add an implicit "IS NOT NULL" filter.
		s.filter = group.isNotNullFilter(s.filter)
	}

	// Find the set of columns that we actually need values for. This is an optimization to avoid
	// unmarshaling unnecessary values and is also used for index selection.
	for i := range scan.valNeededForCol {
		_, ok := s.qvals[columnRef{&s.from, i}]
		scan.valNeededForCol[i] = ok
	}

	// Get the ordering for index selection (if any).
	var ordering columnOrdering
	var grouping bool

	if group != nil {
		ordering = group.desiredOrdering
		grouping = true
	} else if sort != nil {
		ordering = sort.Ordering().ordering
	}

	plan, pErr := p.selectIndex(s, scan, ordering, grouping)
	if pErr != nil {
		return nil, pErr
	}

	// Update s.from with the new plan.
	s.from.node = plan

	s.ordering = s.computeOrdering(plan.Ordering())

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
		// TODO(radu): we shouldn't be using a scanNode in this case at all.

	case 1:
		ate, ok := from[0].(*parser.AliasedTableExpr)
		if !ok {
			return roachpb.NewErrorf("TODO(pmattis): unsupported FROM: %s", from)
		}

		table, ok := ate.Expr.(*parser.QualifiedName)
		if !ok {
			return roachpb.NewErrorf("TODO(pmattis): unsupported FROM: %s", from)
		}

		s.from.alias, s.pErr = scan.initTable(p, table)
		if s.pErr != nil {
			return s.pErr
		}
		if ate.As.Alias != "" {
			// If an alias was specified, use that.
			s.from.alias = string(ate.As.Alias)
		}
	default:
		s.pErr = roachpb.NewErrorf("TODO(pmattis): unsupported FROM: %s", from)
		return s.pErr
	}

	s.from.node = scan
	s.from.columns = scan.Columns()
	return nil
}

func (s *selectNode) initTargets(targets parser.SelectExprs) *roachpb.Error {
	// Loop over the select expressions and expand them into the expressions
	// we're going to use to generate the returned column set and the names for
	// those columns.
	for _, target := range targets {
		if s.pErr = s.addRender(target); s.pErr != nil {
			return s.pErr
		}
	}
	// `groupBy` or `orderBy` may internally add additional columns which we
	// do not want to include in validation of e.g. `GROUP BY 2`. We record the
	// current (initial) number of columns.
	s.numOriginalCols = len(s.columns)
	if len(s.render) != len(s.columns) {
		panic(fmt.Sprintf("%d renders but %d columns!", len(s.render), len(s.columns)))
	}
	return nil
}

func (s *selectNode) initWhere(where *parser.Where) *roachpb.Error {
	if where == nil {
		return nil
	}
	s.filter, s.pErr = s.resolveQNames(where.Expr)
	if s.pErr != nil {
		return s.pErr
	}

	whereType, err := s.filter.TypeCheck(s.planner.evalCtx.Args)
	if err != nil {
		s.pErr = roachpb.NewError(err)
		return s.pErr
	}
	if !(whereType == parser.DummyBool || whereType == parser.DNull) {
		s.pErr = roachpb.NewUErrorf("argument of WHERE must be type %s, not type %s",
			parser.DummyBool.Type(), whereType.Type())
		return s.pErr
	}

	// Normalize the expression (this will also evaluate any branches that are
	// constant).
	s.filter, err = s.planner.parser.NormalizeExpr(s.planner.evalCtx, s.filter)
	if err != nil {
		s.pErr = roachpb.NewError(err)
		return s.pErr
	}
	s.filter, s.pErr = s.planner.expandSubqueries(s.filter, 1)
	return s.pErr
}

// colIndex takes an expression that refers to a column using an integer, verifies it refers to a
// valid render target and returns the corresponding column index. For example:
//    SELECT a from T ORDER by 1
// Here "1" refers to the first render target "a". The returned index is 0.
func (s *selectNode) colIndex(expr parser.Expr) (int, error) {
	switch i := expr.(type) {
	case parser.DInt:
		index := int(i)
		if numCols := s.numOriginalCols; index < 1 || index > numCols {
			return -1, fmt.Errorf("invalid column index: %d not in range [1, %d]", index, numCols)
		}
		return index - 1, nil

	case parser.Datum:
		return -1, fmt.Errorf("non-integer constant column index: %s", expr)

	default:
		// expr doesn't look like a col index (i.e. not a constant).
		return -1, nil
	}
}

func (s *selectNode) addRender(target parser.SelectExpr) *roachpb.Error {
	// When generating an output column name it should exactly match the original
	// expression, so determine the output column name before we perform any
	// manipulations to the expression (such as star expansion).
	var outputName string
	if target.As != "" {
		outputName = string(target.As)
	} else {
		outputName = target.Expr.String()
	}

	// If a QualifiedName has a StarIndirection suffix we need to match the
	// prefix of the qualified name to one of the tables in the query and
	// then expand the "*" into a list of columns.
	if qname, ok := target.Expr.(*parser.QualifiedName); ok {
		if s.pErr = roachpb.NewError(qname.NormalizeColumnName()); s.pErr != nil {
			return s.pErr
		}
		if qname.IsStar() {
			if s.from.alias == "" {
				return roachpb.NewUErrorf("\"%s\" with no tables specified is not valid", qname)
			}
			if target.As != "" {
				return roachpb.NewUErrorf("\"%s\" cannot be aliased", qname)
			}
			// TODO(radu): support multiple FROMs, consolidate with logic in findColumn
			tableName := qname.Table()
			if tableName != "" && !equalName(s.from.alias, tableName) {
				return roachpb.NewUErrorf("table \"%s\" not found", tableName)
			}

			for idx, col := range s.from.node.Columns() {
				if s.from.node.isColumnHidden(idx) {
					continue
				}
				qval := s.getQVal(columnRef{&s.from, idx})
				s.columns = append(s.columns, ResultColumn{Name: col.Name, Typ: qval.datum})
				s.render = append(s.render, qval)
			}

			return nil
		}
	}

	// Resolve qualified names. This has the side-effect of normalizing any
	// qualified name found.
	var resolved parser.Expr
	if resolved, s.pErr = s.resolveQNames(target.Expr); s.pErr != nil {
		return s.pErr
	}
	if resolved, s.pErr = s.planner.expandSubqueries(resolved, 1); s.pErr != nil {
		return s.pErr
	}
	var typ parser.Datum
	var err error
	typ, err = resolved.TypeCheck(s.planner.evalCtx.Args)
	s.pErr = roachpb.NewError(err)
	if s.pErr != nil {
		return s.pErr
	}
	var normalized parser.Expr
	normalized, err = s.planner.parser.NormalizeExpr(s.planner.evalCtx, resolved)
	s.pErr = roachpb.NewError(err)
	if s.pErr != nil {
		return s.pErr
	}
	s.render = append(s.render, normalized)

	if target.As == "" {
		switch t := target.Expr.(type) {
		case *parser.QualifiedName:
			// If the expression is a qualified name, use the column name, not the
			// full qualification as the column name to return.
			outputName = t.Column()
		}
	}
	s.columns = append(s.columns, ResultColumn{Name: outputName, Typ: typ})
	return nil
}

// filterRow checks to see if the current row matches the filter (i.e. the where-clause). Assumes
// the qvals have been populated with the current row. May set n.pErr if an error occurs during
// expression evaluation.
func (s *selectNode) filterRow() bool {
	if s.filter == nil {
		return true
	}

	var d parser.Datum
	var err error
	d, err = s.filter.Eval(s.planner.evalCtx)
	s.pErr = roachpb.NewError(err)
	if s.pErr != nil {
		return false
	}

	return d != parser.DNull && bool(d.(parser.DBool))
}

// renderRow renders the row by evaluating the render expressions. Assumes the qvals have been
// populated with the current row. May set n.pErr if an error occurs during expression evaluation.
func (s *selectNode) renderRow() {
	if s.row == nil {
		s.row = make([]parser.Datum, len(s.render))
	}
	for i, e := range s.render {
		var err error
		s.row[i], err = e.Eval(s.planner.evalCtx)
		s.pErr = roachpb.NewError(err)
		if s.pErr != nil {
			return
		}
	}
}

// Searches for a render target that matches the given column reference.
func (s *selectNode) findRenderIndexForCol(colRef columnRef) (idx int, ok bool) {
	for i, r := range s.render {
		if qval, ok := r.(*qvalue); ok && qval.colRef == colRef {
			return i, true
		}
	}
	return -1, false
}

// Computes ordering information for the select node, given ordering information for the "from"
// node.
//
//    SELECT a, b FROM t@abc ...
//    	the ordering is: first by column 0 (a), then by column 1 (b)
//
//    SELECT a, b FROM t@abc WHERE a = 1 ...
//    	the ordering is: exact match column (a), ordered by column 1 (b)
//
//    SELECT b, a FROM t@abc ...
//      the ordering is: first by column 1 (a), then by column 0 (a)
//
//    SELECT a, c FROM t@abc ...
//      the ordering is: just by column 0 (a). Here we don't have b as a render target so we
//      cannot possibly use (or even express) the second-rank order by b (which makes any lower
//      ranks unusable as well).
//
//      Note that for queries like
//         SELECT a, c FROM t@abc ORDER by a,b,c
//      we internally add b as a render target. The same holds for any targets required for
//      grouping.
func (s *selectNode) computeOrdering(fromOrder orderingInfo) orderingInfo {
	var ordering orderingInfo

	// See if any of the "exact match" columns have render targets. We can ignore any columns that
	// don't have render targets. For example, assume we are using an ascending index on (k, v) with
	// the query:
	//
	//   SELECT v FROM t WHERE k = 1
	//
	// The rows from the index are ordered by k then by v, but since k is an exact match
	// column the results are also ordered just by v.
	for colIdx := range fromOrder.exactMatchCols {
		colRef := columnRef{&s.from, colIdx}
		if renderIdx, ok := s.findRenderIndexForCol(colRef); ok {
			ordering.addExactMatchColumn(renderIdx)
		}
	}
	// Find the longest prefix of columns that have render targets. Once we find a column that is
	// not part of the output, the rest of the ordered columns aren't useful.
	//
	// For example, assume we are using an ascending index on (k, v) with the query:
	//
	//   SELECT v FROM t WHERE k > 1
	//
	// The rows from the index are ordered by k then by v. We cannot make any use of this
	// ordering as an ordering on v.
	for _, colOrder := range fromOrder.ordering {
		colRef := columnRef{&s.from, colOrder.colIdx}
		renderIdx, ok := s.findRenderIndexForCol(colRef)
		if !ok {
			break
		}
		ordering.addColumn(renderIdx, colOrder.direction)
	}
	return ordering
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
func (p *planner) selectIndex(sel *selectNode, s *scanNode, ordering columnOrdering, grouping bool) (fromNode, *roachpb.Error) {
	if s.desc == nil || (sel.filter == nil && ordering == nil) {
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

	if sel.filter != nil {
		// Analyze the filter expression, simplifying it and splitting it up into
		// possibly overlapping ranges.
		exprs, equivalent := analyzeExpr(sel.filter)
		if log.V(2) {
			log.Infof("analyzeExpr: %s -> %s [equivalent=%v]", sel.filter, exprs, equivalent)
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
			sel.filter = joinAndExprs(exprs[0])
		}

		// TODO(pmattis): If "len(exprs) > 1" then we have multiple disjunctive
		// expressions. For example, "a=1 OR a=3" will get translated into "[[a=1],
		// [a=3]]".
		// Right now we don't generate any constraints if we have multiple disjunctions.
		// We would need to perform index selection independently for each of
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
			c.analyzeOrdering(sel, s, ordering)
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
		s.desc = nil
		s.index = nil
		return s, nil
	}
	sel.filter = applyConstraints(sel.filter, c.constraints)
	s.reverse = c.reverse

	var plan fromNode
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

	if grouping && len(ordering) == 1 && len(s.spans) == 1 && sel.filter == nil {
		// If grouping has a desired order and there is a single span for which the
		// filter is true, check to see if the ordering matches the desired
		// ordering. If it does we can limit the scan to a single key.
		existingOrdering := plan.Ordering()
		match := computeOrderingMatch(ordering, existingOrdering, false)
		if match == 1 {
			s.spans[0].count = 1
		}
	}

	if log.V(3) {
		log.Infof("%s: filter=%v", c.index.Name, sel.filter)
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
func (v *indexInfo) analyzeOrdering(sel *selectNode, scan *scanNode, ordering columnOrdering) {
	// Compute the prefix of the index for which we have exact constraints. This
	// prefix is inconsequential for ordering because the values are identical.
	v.exactPrefix = exactPrefix(v.constraints)

	// Compute the ordering provided by the index.
	indexOrdering := sel.computeOrdering(scan.computeOrdering(v.index, v.exactPrefix, false))

	// Compute how much of the index ordering matches the requested ordering for
	// both forward and reverse scans.
	fwdMatch := computeOrderingMatch(ordering, indexOrdering, false)
	revMatch := computeOrderingMatch(ordering, indexOrdering, true)

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

func (v *indexInfo) findColumnInTuple(tuple parser.Tuple, colID ColumnID) int {
	for i, val := range tuple {
		qval, ok := val.(*qvalue)
		// TODO(radu): when we will have multiple FROMs, we should check
		// that the qval refers to us.
		if ok && v.desc.Columns[qval.colRef.colIdx].ID == colID {
			return i
		}
	}
	return -1
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
// operators >, >=, = or IN and end constraints look for comparison expressions
// with the operators <, <=, = or IN. Vice versa for descending cols.
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
					if v.desc.Columns[t.colRef.colIdx].ID != colID {
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
						idx := v.findColumnInTuple(t, colID)
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
			colID := scan.visibleCols[i].ID
			if !v.index.containsColumnID(colID) {
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
// The resulting spans are non-overlapping (by virtue of the input constraints
// being disjunct) and are ordered as the index is (i.e. scanning them in order
// would require only iterating forward through the index).
func makeSpans(constraints indexConstraints,
	tableID ID, index *IndexDescriptor) []span {
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
			resultSpans, coveredCols = applyInConstraint(resultSpans, c, colIdx, index, lastEnd)
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
	resultSpans = resultSpans[:n]
	// Sort the spans to return them in index order.
	sort.Sort(resultSpans)
	return resultSpans
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
