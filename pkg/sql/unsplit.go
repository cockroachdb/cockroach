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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

type unsplitNode struct {
	optColumnsSlot

	tableDesc *sqlbase.TableDescriptor
	index     *sqlbase.IndexDescriptor
	run       unsplitRun
	rows      planNode
}

// unsplitRun contains the run-time state of unsplitNode during local execution.
type unsplitRun struct {
	lastUnsplitKey []byte
}

func (n *unsplitNode) startExec(params runParams) error {
	stickyBitEnabled := params.EvalContext().Settings.Version.IsActive(cluster.VersionStickyBit)
	// TODO(jeffreyxiao): Remove this error in v20.1.
	if !stickyBitEnabled {
		return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			`UNSPLIT AT requires all nodes to be upgraded to %s`,
			cluster.VersionByKey(cluster.VersionStickyBit),
		)
	}
	return nil
}

func (n *unsplitNode) Next(params runParams) (bool, error) {
	if ok, err := n.rows.Next(params); err != nil || !ok {
		return ok, err
	}

	row := n.rows.Values()
	rowKey, err := getRowKey(n.tableDesc, n.index, row)
	if err != nil {
		return false, err
	}

	if err := params.extendedEvalCtx.ExecCfg.DB.AdminUnsplit(params.ctx, rowKey); err != nil {
		ctx := tree.NewFmtCtx(tree.FmtSimple)
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

	tableDesc *sqlbase.TableDescriptor
	index     *sqlbase.IndexDescriptor
	run       unsplitAllRun
}

// unsplitAllRun contains the run-time state of unsplitAllNode during local execution.
type unsplitAllRun struct {
	keys           [][]byte
	lastUnsplitKey []byte
}

func (n *unsplitAllNode) startExec(params runParams) error {
	stickyBitEnabled := params.EvalContext().Settings.Version.IsActive(cluster.VersionStickyBit)
	// TODO(jeffreyxiao): Remove this error in v20.1.
	if !stickyBitEnabled {
		return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			`UNSPLIT AT requires all nodes to be upgraded to %s`,
			cluster.VersionByKey(cluster.VersionStickyBit),
		)
	}
	// Use the internal executor to retrieve the split keys.
	statement := `
		SELECT
			start_key
		FROM
			crdb_internal.ranges_no_leases
		WHERE
			database_name=$1 AND table_name=$2 AND index_name=$3 AND split_enforced_until IS NOT NULL
	`
	dbDesc, err := sqlbase.GetDatabaseDescFromID(params.ctx, params.p.txn, n.tableDesc.ParentID)
	if err != nil {
		return err
	}
	indexName := ""
	if n.index.ID != n.tableDesc.PrimaryIndex.ID {
		indexName = n.index.Name
	}
	ranges, err := params.p.ExtendedEvalContext().InternalExecutor.Query(
		params.ctx, "split points query", params.p.txn, statement,
		dbDesc.Name,
		n.tableDesc.Name,
		indexName,
	)
	if err != nil {
		return err
	}
	n.run.keys = make([][]byte, len(ranges))
	for i, d := range ranges {
		n.run.keys[i] = []byte(*(d[0].(*tree.DBytes)))
	}

	return nil
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
