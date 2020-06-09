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

	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
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
	// operations on a transaction while blocked on a call to pushRow.
	// If pushRow returns an error, the caller must immediately return the error.
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

// setupGenerator takes in a worker that generates rows eagerly and transforms
// it into a lazy row generator. It returns two functions:
// * next: A handle that can be called to generate a row from the worker. Next
//   cannot be called once cleanup has been called.
// * cleanup: Performs all cleanup. This function must be called exactly once
//   to ensure that resources are cleaned up.
func setupGenerator(
	ctx context.Context, worker func(pusher rowPusher) error,
) (next virtualTableGenerator, cleanup cleanupFunc) {
	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	cleanup = cancel

	// comm is the channel to manage communication between the row receiver
	// and the generator. The row receiver notifies the worker to begin
	// computation through comm, and the generator places rows to consume
	// back into comm.
	comm := make(chan virtualTableGeneratorResponse)

	addRow := func(datums ...tree.Datum) error {
		select {
		case <-ctx.Done():
			return sqlbase.QueryCanceledError
		case comm <- virtualTableGeneratorResponse{datums: datums}:
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
		case <-comm:
		}
		return nil
	}

	go func() {
		// We wait until a call to next before starting the worker. This prevents
		// concurrent transaction usage during the startup phase. We also have to
		// wait on done here if cleanup is called before any calls to next() to
		// avoid leaking this goroutine. Lastly, we check if the context has
		// been canceled before any rows are even requested.
		select {
		case <-ctx.Done():
			return
		case <-comm:
		}
		err := worker(funcRowPusher(addRow))
		// If the query was canceled, next() will already return a
		// QueryCanceledError, so just exit here.
		if errors.Is(err, sqlbase.QueryCanceledError) {
			return
		}

		// Notify that we are done sending rows.
		select {
		case <-ctx.Done():
			return
		case comm <- virtualTableGeneratorResponse{err: err}:
		}
	}()

	next = func() (tree.Datums, error) {
		// Notify the worker to begin computing a row.
		select {
		case comm <- virtualTableGeneratorResponse{}:
		case <-ctx.Done():
			return nil, sqlbase.QueryCanceledError
		}

		// Wait for the row to be sent.
		select {
		case <-ctx.Done():
			return nil, sqlbase.QueryCanceledError
		case resp := <-comm:
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

func (p *planner) newVirtualTableNode(
	columns sqlbase.ResultColumns, next virtualTableGenerator, cleanup func(),
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

// vTableLookupJoinNode implements lookup join into a virtual table that has a
// virtual index on the equality columns. For each row of the input, a virtual
// table index lookup is performed, and the rows are joined together.
type vTableLookupJoinNode struct {
	input planNode

	dbName string
	db     *sqlbase.ImmutableDatabaseDescriptor
	table  *sqlbase.TableDescriptor
	index  *sqlbase.IndexDescriptor
	// eqCol is the single equality column ordinal into the lookup table. Virtual
	// indexes only support a single indexed column currently.
	eqCol             int
	virtualTableEntry virtualDefEntry

	joinType sqlbase.JoinType

	// columns is the join's output schema.
	columns sqlbase.ResultColumns
	// pred contains the join's on condition, if any.
	pred *joinPredicate
	// inputCols is the schema of the input to this lookup join.
	inputCols sqlbase.ResultColumns
	// vtableCols is the schema of the virtual table we're looking up rows from,
	// before any projection.
	vtableCols sqlbase.ResultColumns
	// lookupCols is the projection on vtableCols to apply.
	lookupCols exec.TableColumnOrdinalSet

	// run contains the runtime state of this planNode.
	run struct {
		// row contains the next row to output.
		row tree.Datums
		// rows contains the next rows to output, except for row.
		rows   *rowcontainer.RowContainer
		keyCtx constraint.KeyContext

		// indexKeyDatums is scratch space used to construct the index key to
		// look up in the vtable.
		indexKeyDatums []tree.Datum
		// params is set to the current value of runParams on each call to Next.
		// We need to save this in this awkward way because of constraints on the
		// interfaces used in virtual table row generation.
		params *runParams
	}
}

var _ planNode = &vTableLookupJoinNode{}
var _ rowPusher = &vTableLookupJoinNode{}

// startExec implements the planNode interface.
func (v *vTableLookupJoinNode) startExec(params runParams) error {
	v.run.keyCtx = constraint.KeyContext{EvalCtx: params.EvalContext()}
	v.run.rows = rowcontainer.NewRowContainer(params.EvalContext().Mon.MakeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(v.columns), 0)
	v.run.indexKeyDatums = make(tree.Datums, len(v.columns))
	var err error
	db, err := params.p.LogicalSchemaAccessor().GetDatabaseDesc(
		params.ctx,
		params.p.txn,
		params.p.ExecCfg().Codec,
		v.dbName,
		tree.DatabaseLookupFlags{Required: true, AvoidCached: params.p.avoidCachedDescriptors},
	)
	if err != nil {
		return err
	}
	v.db = db.(*sqlbase.ImmutableDatabaseDescriptor)
	return err
}

// Next implements the planNode interface.
func (v *vTableLookupJoinNode) Next(params runParams) (bool, error) {
	// Keep a pointer to runParams around so we can reference it later from
	// pushRow, which can't take any extra arguments.
	v.run.params = &params
	for {
		// Check if there are any rows left to emit from the last input row.
		if v.run.rows.Len() > 0 {
			v.run.row = v.run.rows.At(0)
			v.run.rows.PopFirst()
			return true, nil
		}

		// Lookup more rows from the virtual table.
		ok, err := v.input.Next(params)
		if !ok || err != nil {
			return ok, err
		}
		inputRow := v.input.Values()
		var span constraint.Span
		datum := inputRow[v.eqCol]
		// Generate an index constraint from the equality column of the input.
		key := constraint.MakeKey(datum)
		span.Init(key, constraint.IncludeBoundary, key, constraint.IncludeBoundary)
		var idxConstraint constraint.Constraint
		idxConstraint.InitSingleSpan(&v.run.keyCtx, &span)

		// Create the generation function for the index constraint.
		genFunc := v.virtualTableEntry.makeConstrainedRowsGenerator(params.ctx,
			params.p, v.db, v.index,
			v.run.indexKeyDatums,
			v.table.ColumnIdxMap(),
			&idxConstraint,
			v.vtableCols,
		)
		// Add the input row to the left of the scratch row.
		v.run.row = append(v.run.row[:0], inputRow...)
		// Finally, we're ready to do the lookup. This invocation will push all of
		// the looked-up rows into v.run.rows.
		if err := genFunc(v); err != nil {
			return false, err
		}
		if v.run.rows.Len() == 0 && v.joinType == sqlbase.LeftOuterJoin {
			// No matches - construct an outer match.
			v.run.row = v.run.row[:len(v.inputCols)]
			for i := len(inputRow); i < len(v.columns); i++ {
				v.run.row = append(v.run.row, tree.DNull)
			}
			return true, nil
		}
	}
}

// pushRow implements the rowPusher interface.
func (v *vTableLookupJoinNode) pushRow(lookedUpRow ...tree.Datum) error {
	// Reset our output row to just the contents of the input row.
	v.run.row = v.run.row[:len(v.inputCols)]
	// Append the looked up row to the right of the input row.
	for i, ok := v.lookupCols.Next(0); ok; i, ok = v.lookupCols.Next(i + 1) {
		// Subtract 1 from the requested column position, to avoid the virtual
		// table's fake primary key which won't be present in the row.
		v.run.row = append(v.run.row, lookedUpRow[i-1])
	}
	// Run the predicate and exit if we don't match, or if there was an error.
	if ok, err := v.pred.eval(v.run.params.EvalContext(),
		v.run.row[:len(v.inputCols)],
		v.run.row[len(v.inputCols):]); !ok || err != nil {
		return err
	}
	_, err := v.run.rows.AddRow(v.run.params.ctx, v.run.row)
	return err
}

// Values implements the planNode interface.
func (v *vTableLookupJoinNode) Values() tree.Datums {
	return v.run.row
}

// Close implements the planNode interface.
func (v *vTableLookupJoinNode) Close(ctx context.Context) {
	v.input.Close(ctx)
	v.run.rows.Close(ctx)
}
