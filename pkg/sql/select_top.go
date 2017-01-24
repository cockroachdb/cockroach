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

import "github.com/cockroachdb/cockroach/pkg/sql/parser"

// selectTopNode encapsulate the whole logic of a select statement.
// This exposes the renderNode, groupNode, windowNode, sortNode, distinctNode and limitNode
// side-by-side so that they can "see" each other during query optimization.
type selectTopNode struct {
	// The various nodes involved in obtaining the results.
	source   planNode
	group    *groupNode
	window   *windowNode
	sort     *sortNode
	distinct *distinctNode
	limit    *limitNode
	// The result node that actually runs the query.  Populated during
	// expandSelectTopNode() by connecting the nodes above together.
	plan planNode
}

func (n *selectTopNode) Columns() ResultColumns {
	if n.plan != nil {
		return n.plan.Columns()
	}

	// sort, window, group and source may have different ideas about the
	// result columns. Ask them in turn.
	if n.sort != nil {
		return n.sort.Columns()
	}
	if n.window != nil {
		return n.window.Columns()
	}
	if n.group != nil {
		return n.group.Columns()
	}
	return n.source.Columns()
}

func (n *selectTopNode) Ordering() orderingInfo {
	if n.plan != nil {
		return n.plan.Ordering()
	}
	if n.sort != nil {
		return n.sort.Ordering()
	}
	if n.window != nil {
		return n.window.Ordering()
	}
	if n.group != nil {
		return n.group.Ordering()
	}
	return n.source.Ordering()
}

func (n *selectTopNode) MarkDebug(mode explainMode) { n.plan.MarkDebug(mode) }
func (n *selectTopNode) Start() error               { return n.plan.Start() }
func (n *selectTopNode) Next() (bool, error)        { return n.plan.Next() }
func (n *selectTopNode) Values() parser.DTuple      { return n.plan.Values() }
func (n *selectTopNode) DebugValues() debugValues   { return n.plan.DebugValues() }
func (n *selectTopNode) Close() {
	if n.plan != nil {
		n.plan.Close()
	}
}
