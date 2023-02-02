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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

type unsplitNode struct {
	optColumnsSlot

	tableDesc catalog.TableDescriptor
	index     catalog.Index
	run       unsplitRun
	rows      planNode
}

// unsplitRun contains the run-time state of unsplitNode during local execution.
type unsplitRun struct {
	lastUnsplitKey []byte
}

func (n *unsplitNode) startExec(runParams) error {
	return nil
}

func (n *unsplitNode) Next(params runParams) (bool, error) {
	if ok, err := n.rows.Next(params); err != nil || !ok {
		return ok, err
	}

	row := n.rows.Values()
	rowKey, err := getRowKey(params.ExecCfg().Codec, n.tableDesc, n.index, row)
	if err != nil {
		return false, err
	}

	if err := params.extendedEvalCtx.ExecCfg.DB.AdminUnsplit(params.ctx, rowKey); err != nil {
		ctx := params.p.EvalContext().FmtCtx(tree.FmtSimple)
		row.Format(ctx)
		return false, errors.Wrapf(err, "could not UNSPLIT AT %s", ctx)
	}

	n.run.lastUnsplitKey = rowKey

	return true, nil
}

func (n *unsplitNode) Values() tree.Datums {
	return tree.Datums{
		tree.NewDBytes(tree.DBytes(n.run.lastUnsplitKey)),
		tree.NewDString(keys.PrettyPrint(nil /* valDirs */, n.run.lastUnsplitKey)),
	}
}

func (n *unsplitNode) Close(ctx context.Context) {
	n.rows.Close(ctx)
}

type unsplitAllNode struct {
	optColumnsSlot

	tableDesc catalog.TableDescriptor
	index     catalog.Index
	run       unsplitAllRun
}

// unsplitAllRun contains the run-time state of unsplitAllNode during local execution.
type unsplitAllRun struct {
	keys           [][]byte
	lastUnsplitKey []byte
}

func (n *unsplitAllNode) startExec(params runParams) error {
	// Use the internal executor to retrieve the split keys.
	const statement = `SELECT r.start_key
FROM crdb_internal.ranges_no_leases r,
     crdb_internal.index_spans s
WHERE s.descriptor_id = $1
  AND s.index_id = $2
  AND s.start_key < r.end_key
  AND s.end_key > r.start_key
  AND r.start_key >= s.start_key -- only consider split points inside the table keyspace.
  AND split_enforced_until IS NOT NULL`
	ie := params.p.ExecCfg().InternalDB.NewInternalExecutor(params.SessionData())
	it, err := ie.QueryIteratorEx(
		params.ctx, "split points query", params.p.txn, sessiondata.NoSessionDataOverride,
		statement,
		n.tableDesc.GetID(),
		n.index.GetID(),
	)
	if err != nil {
		return err
	}
	var ok bool
	for ok, err = it.Next(params.ctx); ok; ok, err = it.Next(params.ctx) {
		n.run.keys = append(n.run.keys, []byte(*(it.Cur()[0].(*tree.DBytes))))
	}
	return err
}

func (n *unsplitAllNode) Next(params runParams) (bool, error) {
	if len(n.run.keys) == 0 {
		return false, nil
	}
	rowKey := n.run.keys[0]
	n.run.keys = n.run.keys[1:]

	if err := params.extendedEvalCtx.ExecCfg.DB.AdminUnsplit(params.ctx, rowKey); err != nil {
		return false, errors.Wrapf(err, "could not UNSPLIT AT %s", keys.PrettyPrint(nil /* valDirs */, rowKey))
	}

	n.run.lastUnsplitKey = rowKey

	return true, nil
}

func (n *unsplitAllNode) Values() tree.Datums {
	return tree.Datums{
		tree.NewDBytes(tree.DBytes(n.run.lastUnsplitKey)),
		tree.NewDString(keys.PrettyPrint(nil /* valDirs */, n.run.lastUnsplitKey)),
	}
}

func (n *unsplitAllNode) Close(ctx context.Context) {}
