// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// virtualTableGenerator is the function signature for the virtualTableNode
// `next` property. Each time the virtualTableGenerator function is called, it
// returns a tree.Datums corresponding to the next row of the virtual schema
// table. If there is no next row (end of table is reached), then return (nil,
// nil). If there is an error, then return (nil, error).
type virtualTableGenerator func() (tree.Datums, error)

type virtualTableGeneratorResponse struct {
	datums tree.Datums
	err    error
}

var errRowsDone = errors.New("predefined error that the generator has been canceled")

// setupGenerator takes in a worker that generates rows eagerly and transforms
// it into a lazy row generator. It returns three functions:
// * cleanup: Performs all cleanup. May only be called once.
// * startGenerator: The generator version of the input worker. Intended to
//   be started as a goroutine.
// * next: A handle that can be called to generate a row from the worker. Next
//   cannot be called once cleanup has been called.
func setupGenerator(
	worker func(addRow func(...tree.Datum) error) error,
) (cleanup func(), startGenerator func(), next virtualTableGenerator) {
	done, send := make(chan struct{}), make(chan virtualTableGeneratorResponse)
	cleanup = func() {
		close(done)
	}
	addRow := func(datums ...tree.Datum) error {
		// We need another select here so that in case cleanup has
		// been before we started waiting in addRow.
		select {
		case <-done:
			return errRowsDone
		default:
		}

		select {
		case <-done:
			return errRowsDone
		case send <- virtualTableGeneratorResponse{datums: datums}:
		}
		return nil
	}

	startGenerator = func() {
		err := worker(addRow)
		if err == errRowsDone {
			return
		}
		// Notify that we are done sending rows. This is safe because there should
		// be someone reading from next(). There would only be no one reading from
		// next() is cleanup() was called, which means that err would be equal to
		// errRowsDone.
		send <- virtualTableGeneratorResponse{err: err}
	}

	next = func() (tree.Datums, error) {
		resp := <-send
		return resp.datums, resp.err
	}
	return cleanup, startGenerator, next
}

// virtualTableNode is a planNode that constructs its rows by repeatedly
// invoking a virtualTableGenerator function.
type virtualTableNode struct {
	columns    sqlbase.ResultColumns
	next       virtualTableGenerator
	cleanup    func()
	currentRow tree.Datums
}

func (p *planner) newContainerVirtualTableNode(
	columns sqlbase.ResultColumns, capacity int, next virtualTableGenerator, cleanup func(),
) *virtualTableNode {
	return &virtualTableNode{
		columns: columns,
		next:    next,
		cleanup: cleanup,
	}
}

func (n *virtualTableNode) startExec(runParams) error {
	return nil
}

func (n *virtualTableNode) Next(params runParams) (bool, error) {
	row, err := n.next()
	if err != nil {
		return false, err
	}
	n.currentRow = row
	return row != nil, nil
}

func (n *virtualTableNode) Values() tree.Datums {
	return n.currentRow
}

func (n *virtualTableNode) Close(ctx context.Context) {
	if n.cleanup != nil {
		n.cleanup()
	}
}
