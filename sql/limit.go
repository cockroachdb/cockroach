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
// Author: Tamir Duberstein (tamird@gmail.com)

package sql

import (
	"bytes"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/sql/parser"
)

// limitNode represents a node that limits the number of rows
// returned or only return them past a given number (offset).
type limitNode struct {
	p          *planner
	top        *selectTopNode
	plan       planNode
	countExpr  parser.TypedExpr
	offsetExpr parser.TypedExpr
	count      int64
	offset     int64
	rowIndex   int64
	explain    explainMode
	debugVals  debugValues
}

// limit constructs a limitNode based on the LIMIT and OFFSET clauses.
func (p *planner) Limit(n *parser.Limit) (*limitNode, error) {
	if n == nil || (n.Count == nil && n.Offset == nil) {
		// No LIMIT nor OFFSET; there is nothing special to do.
		return nil, nil
	}

	res := limitNode{p: p}

	data := []struct {
		name string
		src  parser.Expr
		dst  *parser.TypedExpr
	}{
		{"LIMIT", n.Count, &res.countExpr},
		{"OFFSET", n.Offset, &res.offsetExpr},
	}

	for _, datum := range data {
		if datum.src != nil {
			if p.parser.AggregateInExpr(datum.src) {
				return nil, fmt.Errorf("aggregate functions are not allowed in %s", datum.name)
			}

			normalized, err := p.analyzeExpr(datum.src, nil, nil, parser.TypeInt, true, datum.name)
			if err != nil {
				return nil, err
			}
			*datum.dst = normalized
		}
	}
	return &res, nil
}

func (n *limitNode) wrap(plan planNode) planNode {
	if n == nil {
		return plan
	}
	n.plan = plan
	return n
}

func (n *limitNode) expandPlan() error {
	// We do not need to recurse into the child node here; selectTopNode
	// does this for us.

	if err := n.p.expandSubqueryPlans(n.countExpr); err != nil {
		return err
	}
	return n.p.expandSubqueryPlans(n.offsetExpr)
}

func (n *limitNode) Start() error {
	if err := n.plan.Start(); err != nil {
		return err
	}

	if err := n.evalLimit(); err != nil {
		return err
	}

	// Propagate the local limit upstream.
	// Note that this must be called *after* all upstream nodes have been started.
	n.plan.SetLimitHint(getLimit(n.count, n.offset), false /* hard */)

	return nil
}

// estimateLimit returns the Count and Offset fields if they are constants,
// otherwise MaxInt64, 0. Used by index selection.
// This must be called after type checking and constant folding.
func (n *limitNode) estimateLimit() (count, offset int64) {
	if n == nil {
		return math.MaxInt64, 0
	}

	n.count = math.MaxInt64
	n.offset = 0

	data := []struct {
		src parser.TypedExpr
		dst *int64
	}{
		{n.countExpr, &n.count},
		{n.offsetExpr, &n.offset},
	}
	for _, datum := range data {
		if datum.src != nil {
			// Use simple integer datum if available.
			// The limit can be a simple DInt here either because it was
			// entered as such in the query, or as a result of constant
			// folding prior to type checking.
			if d, ok := datum.src.(*parser.DInt); ok {
				*datum.dst = int64(*d)
			}
		}
	}
	return n.count, n.offset
}

// evalLimit evaluates the Count and Offset fields. If Count is missing, the
// value is MaxInt64. If Offset is missing, the value is 0
func (n *limitNode) evalLimit() error {
	n.count = math.MaxInt64
	n.offset = 0

	data := []struct {
		name string
		src  parser.TypedExpr
		dst  *int64
	}{
		{"LIMIT", n.countExpr, &n.count},
		{"OFFSET", n.offsetExpr, &n.offset},
	}

	for _, datum := range data {
		if datum.src != nil {
			if err := n.p.startSubqueryPlans(datum.src); err != nil {
				return err
			}

			dstDatum, err := datum.src.Eval(&n.p.evalCtx)
			if err != nil {
				return err
			}

			if dstDatum == parser.DNull {
				// Use the default value.
				continue
			}

			dstDInt := *dstDatum.(*parser.DInt)
			val := int64(dstDInt)
			if val < 0 {
				return fmt.Errorf("negative value for %s", datum.name)
			}
			*datum.dst = val
		}
	}
	return nil
}

func (n *limitNode) ExplainTypes(regTypes func(string, string)) {
	if n.countExpr != nil {
		regTypes("count", parser.AsStringWithFlags(n.countExpr, parser.FmtShowTypes))
	}
	if n.offsetExpr != nil {
		regTypes("offset", parser.AsStringWithFlags(n.offsetExpr, parser.FmtShowTypes))
	}
}

// setTop connects the limitNode back to the selectTopNode that caused
// its existence. This is needed because the limitNode needs to refer
// to other nodes in the selectTopNode before its expandPlan() method
// has ran and its child plan is known and connected.
func (n *limitNode) setTop(top *selectTopNode) {
	if n != nil {
		n.top = top
	}
}

func (n *limitNode) Columns() []ResultColumn {
	if n.plan != nil {
		return n.plan.Columns()
	}
	// Pre-prepare: not connected yet. Ask the top select node.
	return n.top.Columns()
}

func (n *limitNode) Values() parser.DTuple  { return n.plan.Values() }
func (n *limitNode) Ordering() orderingInfo { return n.plan.Ordering() }

func (n *limitNode) MarkDebug(mode explainMode) {
	if mode != explainDebug {
		panic(fmt.Sprintf("unknown debug mode %d", mode))
	}
	n.explain = mode
	n.plan.MarkDebug(mode)
}

func (n *limitNode) DebugValues() debugValues {
	if n.explain != explainDebug {
		panic(fmt.Sprintf("node not in debug mode (mode %d)", n.explain))
	}
	return n.debugVals
}

func (n *limitNode) Next() (bool, error) {
	// n.rowIndex is the 0-based index of the next row.
	// We don't do (n.rowIndex >= n.offset + n.count) to avoid overflow (count can be MaxInt64).
	if n.rowIndex-n.offset >= n.count {
		return false, nil
	}

	for {
		if next, err := n.plan.Next(); !next {
			return false, err
		}

		if n.explain == explainDebug {
			n.debugVals = n.plan.DebugValues()
			if n.debugVals.output != debugValueRow {
				// Let the non-row debug values pass through.
				break
			}
		}

		n.rowIndex++
		if n.rowIndex > n.offset {
			// Row within limits, return it.
			break
		}

		if n.explain == explainDebug {
			// Return as a filtered row.
			n.debugVals.output = debugValueFiltered
			break
		}
		// Fetch the next row.
	}
	return true, nil
}

func (n *limitNode) ExplainPlan(_ bool) (string, string, []planNode) {
	var buf bytes.Buffer
	subplans := []planNode{n.plan}
	prefix := ""
	if n.countExpr != nil {
		buf.WriteString("count: ")
		n.countExpr.Format(&buf, parser.FmtSimple)
		subplans = n.p.collectSubqueryPlans(n.countExpr, subplans)
		prefix = ", "
	}
	if n.offsetExpr != nil {
		buf.WriteString(prefix)
		buf.WriteString("offset: ")
		n.offsetExpr.Format(&buf, parser.FmtSimple)
		subplans = n.p.collectSubqueryPlans(n.offsetExpr, subplans)
	}
	return "limit", buf.String(), subplans
}

// getLimit computes the actual number of rows to request from the
// data source to honour both the required count and offset together.
// This also ensures that the resulting number of rows does not
// overflow.
func getLimit(count, offset int64) int64 {
	if offset > math.MaxInt64-count {
		count = math.MaxInt64 - offset
	}
	return count + offset
}

func (n *limitNode) SetLimitHint(count int64, soft bool) {
	// A higher-level limitNode or EXPLAIN is pushing a limit down onto
	// this node. Accept it unless the local limit is definitely
	// smaller, in which case we propagate that as a hard limit instead.
	// TODO(radu): we may get a smaller "soft" limit from the upper node
	// and we may have a larger "hard" limit locally. In general, it's
	// not clear which of those results in less work.
	hintCount := count
	if hintCount > n.count {
		hintCount = n.count
		soft = false
	}
	n.plan.SetLimitHint(getLimit(hintCount, n.offset), soft)
}
