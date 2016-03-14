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
	"fmt"
	"math"
	"strconv"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
)

// limit constructs a limitNode based on the LIMIT and OFFSET clauses.
func (p *planner) limit(limit *parser.Limit, plan planNode) (planNode, error) {
	if limit == nil {
		return plan, nil
	}

	var count, offset int64

	data := []struct {
		name       string
		src        parser.Expr
		dst        *int64
		defaultVal int64
	}{
		{"LIMIT", limit.Count, &count, math.MaxInt64},
		{"OFFSET", limit.Offset, &offset, 0},
	}

	for _, datum := range data {
		if datum.src == nil {
			*datum.dst = datum.defaultVal
		} else {
			if parser.ContainsVars(datum.src) {
				return nil, util.Errorf("argument of %s must not contain variables", datum.name)
			}

			normalized, err := p.parser.NormalizeExpr(p.evalCtx, datum.src)
			if err != nil {
				return nil, err
			}
			dstDatum, err := normalized.Eval(p.evalCtx)
			if err != nil {
				return nil, err
			}

			if dstDatum == parser.DNull {
				*datum.dst = datum.defaultVal
				continue
			}

			if dstDInt, ok := dstDatum.(parser.DInt); ok {
				*datum.dst = int64(dstDInt)
				continue
			}

			return nil, fmt.Errorf("argument of %s must be type %s, not type %s", datum.name, parser.DummyInt.Type(), dstDatum.Type())
		}
	}

	if count != math.MaxInt64 {
		plan.SetLimitHint(offset + count)
	}

	return &limitNode{planNode: plan, count: count, offset: offset}, nil
}

type limitNode struct {
	planNode
	count     int64
	offset    int64
	rowIndex  int64
	explain   explainMode
	debugVals debugValues
}

func (n *limitNode) MarkDebug(mode explainMode) {
	if mode != explainDebug {
		panic(fmt.Sprintf("unknown debug mode %d", mode))
	}
	n.explain = mode
	n.planNode.MarkDebug(mode)
}

func (n *limitNode) DebugValues() debugValues {
	if n.explain != explainDebug {
		panic(fmt.Sprintf("node not in debug mode (mode %d)", n.explain))
	}
	return n.debugVals
}

func (n *limitNode) Next() bool {
	// n.rowIndex is the 0-based index of the next row.
	// We don't do (n.rowIndex >= n.offset + n.count) to avoid overflow (count can be MaxInt64).
	if n.rowIndex >= n.offset && (n.rowIndex-n.offset) >= n.count {
		return false
	}

	for {
		if !n.planNode.Next() {
			return false
		}

		if n.explain == explainDebug {
			n.debugVals = n.planNode.DebugValues()
			if n.debugVals.output != debugValueRow {
				// Let the non-row debug values pass through.
				return true
			}
		}

		n.rowIndex++
		if n.rowIndex > n.offset {
			// Row within limits, return it.
			return true
		}

		if n.explain == explainDebug {
			// Return as a filtered row.
			n.debugVals.output = debugValueFiltered
			return true
		}
		// Fetch the next row.
	}
}

func (n *limitNode) ExplainPlan() (string, string, []planNode) {
	var count string
	if n.count == math.MaxInt64 {
		count = "ALL"
	} else {
		count = strconv.FormatInt(n.count, 10)
	}

	return "limit", fmt.Sprintf("count: %s, offset: %d", count, n.offset), []planNode{n.planNode}
}

func (*limitNode) SetLimitHint(_ int64) {}
