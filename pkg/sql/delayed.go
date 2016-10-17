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
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package sql

import "github.com/cockroachdb/cockroach/pkg/sql/parser"

// delayedNode wraps a planNode in cases where the planNode
// constructor must be dealyed during query execution (as opposed to
// SQL prepare) for resource tracking purposes.
type delayedNode struct {
	p           *planner
	name        string
	columns     ResultColumns
	constructor nodeConstructor
	plan        planNode
}

type nodeConstructor func(p *planner) (planNode, error)

func (d *delayedNode) SetLimitHint(_ int64, _ bool) {}
func (d *delayedNode) expandPlan() error {
	v, err := d.constructor(d.p)
	if err != nil {
		return err
	}
	if err := v.expandPlan(); err != nil {
		v.Close()
		return err
	}
	d.plan = v
	return nil
}

func (d *delayedNode) Close() {
	if d.plan != nil {
		d.plan.Close()
		d.plan = nil
	}
}

func (d *delayedNode) ExplainPlan(verbose bool) (name, description string, children []planNode) {
	if d.plan != nil {
		children = []planNode{d.plan}
	}
	return "virtual table", d.name, children
}

func (d *delayedNode) ExplainTypes(rt func(string, string)) {}
func (d *delayedNode) Columns() ResultColumns               { return d.columns }
func (d *delayedNode) Ordering() orderingInfo               { return orderingInfo{} }
func (d *delayedNode) MarkDebug(_ explainMode)              {}
func (d *delayedNode) Start() error                         { return d.plan.Start() }
func (d *delayedNode) Next() (bool, error)                  { return d.plan.Next() }
func (d *delayedNode) Values() parser.DTuple                { return d.plan.Values() }
func (d *delayedNode) DebugValues() debugValues             { return d.plan.DebugValues() }
