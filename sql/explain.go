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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/sql/parser"
)

type explainMode int

const (
	explainNone explainMode = iota
	explainDebug
	explainPlan
)

// Explain executes the explain statement, providing debugging and analysis
// info about a DELETE, INSERT, SELECT or UPDATE statement.
//
// Privileges: the same privileges as the statement being explained.
func (p *planner) Explain(n *parser.Explain) (planNode, error) {
	mode := explainNone
	if len(n.Options) == 1 && strings.EqualFold(n.Options[0], "DEBUG") {
		mode = explainDebug
	} else if len(n.Options) == 0 {
		mode = explainPlan
	}
	if mode == explainNone {
		return nil, fmt.Errorf("unsupported EXPLAIN options: %s", n)
	}

	plan, err := p.makePlan(n.Statement)
	if err != nil {
		return nil, err
	}
	switch mode {
	case explainDebug:
		plan, err = markDebug(plan, mode)
		if err != nil {
			return nil, fmt.Errorf("%v: %s", err, n)
		}
		return plan, nil
	case explainPlan:
		v := &valuesNode{}
		v.columns = []string{"Level", "Type", "Description"}
		populateExplain(v, plan, 0)
		plan = v
	default:
		return nil, fmt.Errorf("unsupported EXPLAIN mode: %d", mode)
	}
	return plan, nil
}

func markDebug(plan planNode, mode explainMode) (planNode, error) {
	switch t := plan.(type) {
	case *scanNode:
		// Mark the node as being explained.
		t.columns = []string{"RowIdx", "Key", "Value", "Output"}
		t.explain = mode
		return t, nil

	case *sortNode:
		return markDebug(t.plan, mode)

	default:
		return nil, fmt.Errorf("TODO(pmattis): unimplemented %T", plan)
	}
}

func populateExplain(v *valuesNode, plan planNode, level int) {
	name, description, children := plan.ExplainPlan()

	row := parser.DTuple{
		parser.DInt(level),
		parser.DString(name),
		parser.DString(description),
	}
	v.rows = append(v.rows, row)

	for _, child := range children {
		populateExplain(v, child, level+1)
	}
}
