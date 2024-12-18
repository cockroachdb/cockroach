// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// delayedNode wraps a planNode in cases where the planNode
// constructor must be delayed during query execution (as opposed to
// SQL prepare) for resource tracking purposes.
type delayedNode struct {
	name        string
	columns     colinfo.ResultColumns
	constructor nodeConstructor
	input       planNode
}

type nodeConstructor func(context.Context, *planner) (planNode, error)

func (d *delayedNode) Next(params runParams) (bool, error) { return d.input.Next(params) }
func (d *delayedNode) Values() tree.Datums                 { return d.input.Values() }

func (d *delayedNode) Close(ctx context.Context) {
	if d.input != nil {
		d.input.Close(ctx)
		d.input = nil
	}
}

func (n *delayedNode) InputCount() int {
	if n.input != nil {
		return 1
	}
	return 0
}

func (n *delayedNode) Input(i int) (planNode, error) {
	if i == 0 && n.input != nil {
		return n.input, nil
	}
	return nil, errors.AssertionFailedf("input index %d is out of range", i)
}

// startExec constructs the wrapped planNode now that execution is underway.
func (d *delayedNode) startExec(params runParams) error {
	if d.input != nil {
		panic("wrapped input should not yet exist")
	}

	plan, err := d.constructor(params.ctx, params.p)
	if err != nil {
		return err
	}
	d.input = plan

	// Recursively invoke startExec on new plan. Normally, startExec doesn't
	// recurse - calling children is handled by the planNode walker. The reason
	// this won't suffice here is that the child of this node doesn't exist
	// until after startExec is invoked.
	return startExec(params, plan)
}
