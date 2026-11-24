// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/redact"
)

// bufferNode consumes its input one row at a time, stores it in the buffer,
// and passes the row through. The buffered rows can be iterated over multiple
// times.
type bufferNode struct {
	singleInputPlanNode

	// typs is the schema of rows buffered by this node.
	typs       []*types.T
	rows       rowContainerHelper
	currentRow tree.Datums

	// label is a string used to describe the node in an EXPLAIN plan.
	// TODO(yuzefovich/mgartner): make this redact.SafeString.
	label string
}

func (n *bufferNode) startExec(params runParams) error {
	n.typs = planTypes(n.input)
	n.rows.Init(params.ctx, n.typs, params.extendedEvalCtx,
		redact.SafeString(redact.Sprint(n.label).Redact()))
	return nil
}

func (n *bufferNode) Next(params runParams) (bool, error) {
	if err := params.p.cancelChecker.Check(); err != nil {
		return false, err
	}
	ok, err := n.input.Next(params)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	n.currentRow = n.input.Values()
	if err = n.rows.AddRow(params.ctx, n.currentRow); err != nil {
		return false, err
	}
	return true, nil
}

func (n *bufferNode) Values() tree.Datums {
	return n.currentRow
}

func (n *bufferNode) Close(ctx context.Context) {
	n.input.Close(ctx)
	n.rows.Close(ctx)
}

// scanBufferNode behaves like an iterator into the bufferNode it is
// referencing. The bufferNode can be iterated over multiple times
// simultaneously, however, a new scanBufferNode is needed.
type scanBufferNode struct {
	zeroInputPlanNode

	// mu, if non-nil, protects access buffer as well as creation and closure of
	// iterator (rowcontainer.RowIterator which is wrapped by
	// rowContainerIterator is safe for concurrent usage outside of creation and
	// closure).
	mu *syncutil.Mutex

	buffer *bufferNode

	iterator   *rowContainerIterator
	currentRow tree.Datums

	// label is a string used to describe the node in an EXPLAIN plan.
	label string
}

// makeConcurrencySafe can be called to synchronize access to bufferNode across
// scanBufferNodes that run in parallel.
func (n *scanBufferNode) makeConcurrencySafe(mu *syncutil.Mutex) {
	n.mu = mu
}

func (n *scanBufferNode) startExec(params runParams) error {
	if n.mu != nil {
		n.mu.Lock()
		defer n.mu.Unlock()
	}
	n.iterator = newRowContainerIterator(params.ctx, n.buffer.rows)
	return nil
}

func (n *scanBufferNode) Next(runParams) (bool, error) {
	var err error
	n.currentRow, err = n.iterator.Next()
	if n.currentRow == nil || err != nil {
		return false, err
	}
	return true, nil
}

func (n *scanBufferNode) Values() tree.Datums {
	return n.currentRow
}

func (n *scanBufferNode) Close(context.Context) {
	if n.mu != nil {
		n.mu.Lock()
		defer n.mu.Unlock()
	}
	if n.iterator != nil {
		n.iterator.Close()
		n.iterator = nil
	}
}
