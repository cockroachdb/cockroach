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

package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type explainMode int

const (
	explainNone explainMode = iota
	explainPlan
	// explainDistSQL shows the physical distsql plan for a query and whether a
	// query would be run in "auto" DISTSQL mode. See explainDistSQLNode for
	// details.
	explainDistSQL
)

var explainStrings = map[explainMode]string{
	explainPlan:    "plan",
	explainDistSQL: "distsql",
}

// Explain executes the explain statement, providing debugging and analysis
// info about the wrapped statement.
//
// Privileges: the same privileges as the statement being explained.
func (p *Planner) Explain(ctx context.Context, n *tree.Explain) (planNode, error) {
	mode := explainNone

	optimized := true
	expanded := true
	normalizeExprs := true
	explainer := explainer{
		showMetadata: false,
		showExprs:    false,
		showTypes:    false,
	}

	for _, opt := range n.Options {
		optLower := strings.ToLower(opt)
		newMode := explainNone
		// Search for the string in `explainStrings`.
		for mode, modeStr := range explainStrings {
			if optLower == modeStr {
				newMode = mode
				break
			}
		}
		if newMode == explainNone {
			switch optLower {
			case "types":
				newMode = explainPlan
				explainer.showExprs = true
				explainer.showTypes = true
				// TYPES implies METADATA.
				explainer.showMetadata = true

			case "symvars":
				explainer.symbolicVars = true

			case "metadata":
				explainer.showMetadata = true

			case "qualify":
				explainer.qualifyNames = true

			case "verbose":
				// VERBOSE implies EXPRS.
				explainer.showExprs = true
				// VERBOSE implies QUALIFY.
				explainer.qualifyNames = true
				// VERBOSE implies METADATA.
				explainer.showMetadata = true

			case "exprs":
				explainer.showExprs = true

			case "noexpand":
				expanded = false

			case "nonormalize":
				normalizeExprs = false

			case "nooptimize":
				optimized = false

			default:
				return nil, fmt.Errorf("unsupported EXPLAIN option: %s", opt)
			}
		}
		if newMode != explainNone {
			if mode != explainNone {
				return nil, fmt.Errorf("cannot set EXPLAIN mode more than once: %s", opt)
			}
			mode = newMode
		}
	}
	if mode == explainNone {
		mode = explainPlan
	}

	p.evalCtx.SkipNormalize = !normalizeExprs

	plan, err := p.newPlan(ctx, n.Statement, nil)
	if err != nil {
		return nil, err
	}
	switch mode {
	case explainDistSQL:
		return &explainDistSQLNode{
			plan: plan,
		}, nil

	case explainPlan:
		// We may want to show placeholder types, so ensure no values
		// are missing.
		p.semaCtx.Placeholders.PermitUnassigned()
		return p.makeExplainPlanNode(explainer, expanded, optimized, n.Statement, plan), nil

	default:
		return nil, fmt.Errorf("unsupported EXPLAIN mode: %d", mode)
	}
}
