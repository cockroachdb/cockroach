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
)

// limit constructs a limitNode based on the LIMIT and OFFSET clauses.
func (p *planner) limit(n *parser.Select, plan planNode) (planNode, error) {
	if n.Limit == nil {
		return plan, nil
	}

	var count, offset int64

	data := []struct {
		name       string
		src        parser.Expr
		dst        *int64
		defaultVal int64
	}{
		{"LIMIT", n.Limit.Count, &count, math.MaxInt64},
		{"OFFSET", n.Limit.Offset, &offset, 0},
	}

	for _, datum := range data {
		if datum.src == nil {
			*datum.dst = datum.defaultVal
		} else {
			if parser.ContainsVars(datum.src) {
				return nil, fmt.Errorf("argument of %s must not contain variables", datum.name)
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

	return &limitNode{planNode: plan, count: count, offset: offset}, nil
}

type limitNode struct {
	planNode
	count          int64
	offset         int64
	rowIndex       int64
	outputRowIndex int64
}

func (n *limitNode) Next() bool {
	if n.outputRowIndex >= n.count {
		return false
	}

	for n.rowIndex < n.offset && n.planNode.Next() {
		n.rowIndex++
	}

	n.outputRowIndex++
	return n.planNode.Next()
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
