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

// cleanupFunc is a function to cleanup resources created by the generator.
type cleanupFunc func()

// rowPusher is an interface for lazy generators to push rows into
// and then suspend until the next row has been requested.
type rowPusher interface {
	// pushRow pushes the input row to the receiver of the generator. It doesn't
	// mutate the input row. It will block until the the data has been received
	// and more data has been requested. Once pushRow returns, the caller is free
	// to mutate the slice passed as input. The caller is not allowed to perform
	// operations on a transaction while blocked on a call pushRow.
	pushRow(...tree.Datum) error
}

// funcRowPusher implements rowPusher on functions.
type funcRowPusher func(...tree.Datum) error

func (f funcRowPusher) pushRow(datums ...tree.Datum) error {
	return f(datums...)
}

type virtualTableGeneratorResponse struct {
	datums tree.Datums
	err    error
}

var errRowsDone = errors.New("the generator has been canceled")

// setupGenerator takes in a worker that generates rows eagerly and transforms
// it into a lazy row generator. It returns two functions:
// * next: A handle that can be called to generate a row from the worker. Next
//   cannot be called once cleanup has been called.
// * cleanup: Performs all cleanup. This function must be called exactly once
//   to ensure that resources are cleaned up.
func setupGenerator(
	ctx context.Context, worker func(pusher rowPusher) error,
) (next virtualTableGenerator, cleanup cleanupFunc) {
	allowWork, done, send := make(chan struct{}, 1), make(chan struct{}), make(chan virtualTableGeneratorResponse, 1)
	cleanup = func() {
		close(done)
	}
	addRow := func(datums ...tree.Datum) error {
		// Check if cancellation was requested separately to avoid
		// non-deterministically picking whether to send a row or return
		// QueryCancelledError.
		select {
		case <-ctx.Done():
			return sqlbase.QueryCanceledError
		default:
		}

		select {
		case <-done:
			return errRowsDone
		case send <- virtualTableGeneratorResponse{datums: datums}:
		}

		// Block until the next call to cleanup() or next(). This allows us to
		// avoid issues with concurrent transaction usage if the worker is using
		// a transaction. Otherwise, worker could proceed running operations after
		// a call to next() has returned. That could result in the main operator
		// chain using the transaction while the worker is also running. This
		// makes it so that the worker can only run while next() is being called,
		// which effectively gives ownership of the transaction usage over to the
		// worker, and then back to the next() caller after it is done.
		select {
		case <-ctx.Done():
			return sqlbase.QueryCanceledError
		case <-done:
			return errRowsDone
		case <-allowWork:
		}
		return nil
	}

	go func() {
		// We wait until a call to next before starting the worker. This prevents
		// concurrent transaction usage during the startup phase. We also have to
		// wait on done here if cleanup is called before any calls to next() to
		// avoid leaking this goroutine. Lastly, we check if the context has
		// been cancelled before any rows are even requested.
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-allowWork:
		}
		err := worker(funcRowPusher(addRow))
		// If the query was cancelled, next() will already return a
		// QueryCancelledError, so just exit here.
		if err == errRowsDone || err == sqlbase.QueryCanceledError {
			return
		}
		// Notify that we are done sending rows. This is safe because there should
		// be someone reading from next(). There would only be no one reading from
		// next() is cleanup() was called, which means that err would be equal to
		// errRowsDone.
		send <- virtualTableGeneratorResponse{err: err}
	}()

	next = func() (tree.Datums, error) {
		allowWork <- struct{}{}
		select {
		case <-ctx.Done():
			return nil, sqlbase.QueryCanceledError
		case resp := <-send:
			return resp.datums, resp.err
		}
	}
	return next, cleanup
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
