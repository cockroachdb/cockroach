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

	// iterMu synchronizes creation and closure of iterators over rows across
	// the scanBufferNodes reading from this buffer. Multiple scanBufferNodes
	// referencing this buffer can start concurrently (e.g. as inputs of a
	// parallel unordered synchronizer), and rowcontainer.RowIterator (which
	// rowContainerIterator wraps) is only safe for concurrent usage outside
	// of creation and closure. scanBufferNodes must create iterators via
	// newIterator, which locks iterMu (as does the iterator's Close method).
	iterMu syncutil.Mutex

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

// newIterator returns an iterator over the buffered rows. See iterMu.
func (n *bufferNode) newIterator(ctx context.Context) *bufferIterator {
	n.iterMu.Lock()
	defer n.iterMu.Unlock()
	return &bufferIterator{
		iter: newRowContainerIterator(ctx, n.rows),
		buf:  n,
	}
}

// bufferIterator is a rowContainerIterator handed out by newIterator and owned
// by the single scanBufferNode that asked for it. Next runs unsynchronized;
// only creation and Close take the buffer's iterMu, serializing them against
// the other scanBufferNodes iterating the same buffer. See bufferNode.iterMu
// for why that is where the boundary sits.
//
// rowContainerIterator is wrapped rather than embedded: were it embedded, a
// method added to it later would join this type's API automatically and
// unsynchronized. Forwarding makes each one a deliberate choice.
type bufferIterator struct {
	iter *rowContainerIterator
	buf  *bufferNode
}

func (i *bufferIterator) Next() (tree.Datums, error) {
	return i.iter.Next()
}

func (i *bufferIterator) Close() {
	i.buf.iterMu.Lock()
	defer i.buf.iterMu.Unlock()
	i.iter.Close()
}

// scanBufferNode behaves like an iterator into the bufferNode it is
// referencing. The bufferNode can be iterated over multiple times
// simultaneously, however, a new scanBufferNode is needed.
type scanBufferNode struct {
	zeroInputPlanNode

	buffer *bufferNode

	iterator   *bufferIterator
	currentRow tree.Datums

	// label is a string used to describe the node in an EXPLAIN plan.
	label string
}

func (n *scanBufferNode) startExec(params runParams) error {
	n.iterator = n.buffer.newIterator(params.ctx)
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
	if n.iterator != nil {
		n.iterator.Close()
		n.iterator = nil
	}
}
