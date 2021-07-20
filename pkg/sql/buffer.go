// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// bufferNode consumes its input one row at a time, stores it in the buffer,
// and passes the row through. The buffered rows can be iterated over multiple
// times.
type bufferNode struct {
	plan planNode

	// typs is the schema of rows buffered by this node.
	typs       []*types.T
	rows       rowContainerHelper
	currentRow tree.Datums

	// label is a string used to describe the node in an EXPLAIN plan.
	label string
}

func (n *bufferNode) startExec(params runParams) error {
	n.typs = planTypes(n.plan)
	n.rows.init(n.typs, params.extendedEvalCtx, n.label)
	return nil
}

func (n *bufferNode) Next(params runParams) (bool, error) {
	if err := params.p.cancelChecker.Check(); err != nil {
		return false, err
	}
	ok, err := n.plan.Next(params)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	n.currentRow = n.plan.Values()
	if err = n.rows.addRow(params.ctx, n.currentRow); err != nil {
		return false, err
	}
	return true, nil
}

func (n *bufferNode) Values() tree.Datums {
	return n.currentRow
}

func (n *bufferNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
	n.rows.close(ctx)
}

// scanBufferNode behaves like an iterator into the bufferNode it is
// referencing. The bufferNode can be iterated over multiple times
// simultaneously, however, a new scanBufferNode is needed.
type scanBufferNode struct {
	buffer *bufferNode

	iterator   *rowContainerIterator
	currentRow tree.Datums

	// label is a string used to describe the node in an EXPLAIN plan.
	label string
}

func (n *scanBufferNode) startExec(params runParams) error {
	n.iterator = newRowContainerIterator(params.ctx, n.buffer.rows, n.buffer.typs)
	return nil
}

func (n *scanBufferNode) Next(runParams) (bool, error) {
	var err error
	n.currentRow, err = n.iterator.next()
	if n.currentRow == nil || err != nil {
		return false, err
	}
	return true, nil
}

func (n *scanBufferNode) Values() tree.Datums {
	return n.currentRow
}

func (n *scanBufferNode) Close(context.Context) {
	if n.iterator != nil {
		n.iterator.close()
		n.iterator = nil
	}
}
