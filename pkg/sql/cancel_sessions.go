// Copyright 2018 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/pkg/errors"
)

type cancelSessionsNode struct {
	rows     planNode
	ifExists bool
}

func (p *planner) CancelSessions(ctx context.Context, n *tree.CancelSessions) (planNode, error) {
	rows, err := p.newPlan(ctx, n.Sessions, []types.T{types.String})
	if err != nil {
		return nil, err
	}
	cols := planColumns(rows)
	if len(cols) != 1 {
		return nil, errors.Errorf("CANCEL SESSIONS expects a single column source, got %d columns", len(cols))
	}
	if !cols[0].Typ.Equivalent(types.String) {
		return nil, errors.Errorf("CANCEL SESSIONS requires string values, not type %s", cols[0].Typ)
	}

	return &cancelSessionsNode{
		rows:     rows,
		ifExists: n.IfExists,
	}, nil
}

func (n *cancelSessionsNode) Next(params runParams) (bool, error) {
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
	sessionIDString, ok := tree.AsDString(datum)
	if !ok {
		return false, pgerror.NewAssertionErrorf("%q: expected *DString, found %T", datum, datum)
	}

	sessionID, err := StringToClusterWideID(string(sessionIDString))
	if err != nil {
		return false, errors.Wrapf(err, "invalid session ID %s", datum)
	}

	// Get the lowest 32 bits of the session ID.
	nodeID := sessionID.GetNodeID()

	request := &serverpb.CancelSessionRequest{
		NodeId:    fmt.Sprintf("%d", nodeID),
		SessionID: sessionID.GetBytes(),
		Username:  params.SessionData().User,
	}

	response, err := statusServer.CancelSession(params.ctx, request)
	if err != nil {
		return false, err
	}

	if !response.Canceled && !n.ifExists {
		return false, fmt.Errorf("could not cancel session %s: %s", sessionID, response.Error)
	}

	return true, nil
}

func (*cancelSessionsNode) Values() tree.Datums { return nil }

func (n *cancelSessionsNode) Close(ctx context.Context) {
	n.rows.Close(ctx)
}
