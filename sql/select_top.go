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
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package sql

import "github.com/cockroachdb/cockroach/sql/parser"

// selectTopNode encapsulate the whole logic of a select statement.
// This exposes the selectNode, groupNode, sortNode, distinctNode and limitNode
// side-by-side so that they can "see" each other during query optimization.
type selectTopNode struct {
	// The various nodes involved in obtaining the results.
	source   planNode
	group    *groupNode
	sort     *sortNode
	distinct *distinctNode
	limit    *limitNode
	// The result node that actually runs the query.
	// Populated during expandPlan() by connecting the nodes above
	// together.
	plan planNode
}

func (n *selectTopNode) ExplainTypes(f func(string, string)) {
	if n.plan == nil {
		// The sub-nodes are not connected yet.
		// Ask them for typing individually.
		if n.limit != nil {
			n.limit.ExplainTypes(f)
		}
		if n.distinct != nil {
			n.distinct.ExplainTypes(f)
		}
		if n.sort != nil {
			n.sort.ExplainTypes(f)
		}
		if n.group != nil {
			n.group.ExplainTypes(f)
		}

		// The source is always reported as sub-plan by ExplainPlan,
		// so it will explain its own types.
	}
}

func (n *selectTopNode) SetLimitHint(numRows int64, soft bool) {
	n.plan.SetLimitHint(numRows, soft)
}

func (n *selectTopNode) expandPlan() error {
	if n.plan != nil {
		panic("can't expandPlan twice!")
	}

	var squash bool

	n.plan = n.source
	if err := n.source.expandPlan(); err != nil {
		return err
	}

	n.plan = n.group.wrap(n.plan)
	if n.group != nil {
		if err := n.group.expandPlan(); err != nil {
			return err
		}
	}

	squash, n.plan = n.sort.wrap(n.plan)
	if squash {
		n.sort = nil
	}
	if n.sort != nil {
		if err := n.sort.expandPlan(); err != nil {
			return err
		}
	}

	n.plan = n.distinct.wrap(n.plan)
	if n.distinct != nil {
		if err := n.distinct.expandPlan(); err != nil {
			return err
		}
	}

	n.plan = n.limit.wrap(n.plan)
	if n.limit != nil {
		if err := n.limit.expandPlan(); err != nil {
			return err
		}
	}
	return nil
}

func (n *selectTopNode) ExplainPlan(v bool) (name, description string, subplans []planNode) {
	if !v {
		return n.plan.ExplainPlan(v)
	}

	if n.plan != nil {
		subplans = []planNode{n.plan}
	} else {
		// We are not connected yet, but we may still be interested in the
		// sub-query plans. Get them.
		subplans = []planNode{}
		if n.limit != nil {
			_, _, plans := n.limit.ExplainPlan(false)
			subplans = append(subplans, plans[1:]...)
		}
		if n.distinct != nil {
			_, _, plans := n.distinct.ExplainPlan(false)
			subplans = append(subplans, plans[1:]...)
		}
		if n.sort != nil {
			_, _, plans := n.sort.ExplainPlan(false)
			subplans = append(subplans, plans[1:]...)
		}
		if n.group != nil {
			_, _, plans := n.group.ExplainPlan(false)
			subplans = append(subplans, plans[1:]...)
		}
		subplans = append(subplans, n.source)
	}
	return "select", "", subplans
}

func (n *selectTopNode) Columns() []ResultColumn {
	// sort, group and source may have different ideas about the
	// result columns. Ask them in turn.
	// (We cannot ask n.plan because it may not be connected yet.)
	if n.sort != nil {
		return n.sort.Columns()
	}
	if n.group != nil {
		return n.group.Columns()
	}
	return n.source.Columns()
}

func (n *selectTopNode) Ordering() orderingInfo {
	if n.plan == nil {
		return n.source.Ordering()
	}
	return n.plan.Ordering()
}

func (n *selectTopNode) MarkDebug(mode explainMode) { n.plan.MarkDebug(mode) }
func (n *selectTopNode) Start() error               { return n.plan.Start() }
func (n *selectTopNode) Next() (bool, error)        { return n.plan.Next() }
func (n *selectTopNode) Values() parser.DTuple      { return n.plan.Values() }
func (n *selectTopNode) DebugValues() debugValues   { return n.plan.DebugValues() }
