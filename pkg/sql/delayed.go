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

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// delayedNode wraps a planNode in cases where the planNode
// constructor must be delayed during query execution (as opposed to
// SQL prepare) for resource tracking purposes.
type delayedNode struct {
	name        string
	columns     sqlbase.ResultColumns
	constructor nodeConstructor
	plan        planNode
}

// delayedNode implements the autoCommitNode interface.
var _ autoCommitNode = &delayedNode{}

type nodeConstructor func(context.Context, *planner) (planNode, error)

func (d *delayedNode) Next(params runParams) (bool, error) { return d.plan.Next(params) }
func (d *delayedNode) Values() tree.Datums                 { return d.plan.Values() }

func (d *delayedNode) Close(ctx context.Context) {
	if d.plan != nil {
		d.plan.Close(ctx)
		d.plan = nil
	}
}

// enableAutoCommit is part of the autoCommitNode interface.
func (d *delayedNode) enableAutoCommit() {
	if ac, ok := d.plan.(autoCommitNode); ok {
		ac.enableAutoCommit()
	}
}
