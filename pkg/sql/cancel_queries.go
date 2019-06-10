// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type cancelQueriesNode struct {
	rows     planNode
	ifExists bool
}

func (p *planner) CancelQueries(ctx context.Context, n *tree.CancelQueries) (planNode, error) {
	rows, err := p.newPlan(ctx, n.Queries, []*types.T{types.String})
	if err != nil {
		return nil, err
	}
	cols := planColumns(rows)
	if len(cols) != 1 {
		return nil, pgerror.Newf(pgcode.Syntax,
			"CANCEL QUERIES expects a single column source, got %d columns", len(cols))
	}
	if !cols[0].Typ.Equivalent(types.String) {
		return nil, pgerror.Newf(pgcode.DatatypeMismatch,
			"CANCEL QUERIES requires string values, not type %s", cols[0].Typ)
	}

	return &cancelQueriesNode{
		rows:     rows,
		ifExists: n.IfExists,
	}, nil
}

func (n *cancelQueriesNode) startExec(runParams) error {
	return nil
}

func (n *cancelQueriesNode) Next(params runParams) (bool, error) {
	// TODO(knz): instead of performing the cancels sequentially,
	// accumulate all the query IDs and then send batches to each of the
	// nodes.

	if ok, err := n.rows.Next(params); err != nil || !ok {
		return ok, err
	}

	datum := n.rows.Values()[0]
	if datum == tree.DNull {
		return true, nil
	}

	statusServer := params.extendedEvalCtx.StatusServer
	queryIDString, ok := tree.AsDString(datum)
	if !ok {
		return false, errors.AssertionFailedf("%q: expected *DString, found %T", datum, datum)
	}

	queryID, err := StringToClusterWideID(string(queryIDString))
	if err != nil {
		return false, pgerror.Wrapf(err, pgcode.Syntax, "invalid query ID %s", datum)
	}

	// Get the lowest 32 bits of the query ID.
	nodeID := 0xFFFFFFFF & queryID.Lo

	request := &serverpb.CancelQueryRequest{
		NodeId:   fmt.Sprintf("%d", nodeID),
		QueryID:  string(queryIDString),
		Username: params.SessionData().User,
	}

	response, err := statusServer.CancelQuery(params.ctx, request)
	if err != nil {
		return false, err
	}

	if !response.Canceled && !n.ifExists {
		return false, errors.Newf("could not cancel query %s: %s", queryID, response.Error)
	}

	return true, nil
}

func (*cancelQueriesNode) Values() tree.Datums { return nil }

func (n *cancelQueriesNode) Close(ctx context.Context) {
	n.rows.Close(ctx)
}
