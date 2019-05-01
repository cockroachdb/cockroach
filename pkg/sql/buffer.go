// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// bufferNode consumes all the input rows at once and writes them to a buffer.
// After the input has been fully consumed, it proceeds on passing the rows
// through. The buffer can be iterated over multiple times.
type bufferNode struct {
	plan planNode

	// TODO(yuzefovich): should the buffer be backed by the disk? If so, the
	// comments about TempStorage suggest that it should be used by DistSQL
	// processors, but this node is local.
	bufferedRows       *rowcontainer.RowContainer
	passThruNextRowIdx int
}

func (n *bufferNode) startExec(params runParams) error {
	n.bufferedRows = rowcontainer.NewRowContainer(
		params.EvalContext().Mon.MakeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(getPlanColumns(n.plan, false /* mut */)),
		0, /* rowCapacity */
	)
	for {
		if err := params.p.cancelChecker.Check(); err != nil {
			return err
		}
		ok, err := n.plan.Next(params)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if _, err = n.bufferedRows.AddRow(params.ctx, n.plan.Values()); err != nil {
			return err
		}
	}
}

func (n *bufferNode) Next(params runParams) (bool, error) {
	if n.passThruNextRowIdx >= n.bufferedRows.Len() {
		return false, nil
	}
	n.passThruNextRowIdx++
	return true, nil
}

func (n *bufferNode) Values() tree.Datums {
	return n.bufferedRows.At(n.passThruNextRowIdx - 1)
}

// TODO(yuzefovich): does this need to have some special behavior?
func (n *bufferNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
	n.bufferedRows.Close(ctx)
}

// scanBufferNode behaves like an iterator into the bufferNode it is
// referencing. The bufferNode can be iterated over multiple times
// simultaneously, however, a new scanBufferNode is needed. Note that
// scanBufferNode can only start its execution after the corresponding
// bufferNode finishes its execution completely.
type scanBufferNode struct {
	buffer *bufferNode

	nextRowIdx int
}

func (n *scanBufferNode) startExec(runParams) error {
	return nil
}

func (n *scanBufferNode) Next(runParams) (bool, error) {
	n.nextRowIdx++
	return n.nextRowIdx <= n.buffer.bufferedRows.Len(), nil
}

func (n *scanBufferNode) Values() tree.Datums {
	return n.buffer.bufferedRows.At(n.nextRowIdx - 1)
}

// Note that scanBufferNode does not close the corresponding to it bufferNode.
func (n *scanBufferNode) Close(context.Context) {
}
