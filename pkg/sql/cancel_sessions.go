// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type cancelSessionsNode struct {
	singleInputPlanNode
	rowsAffectedOutputHelper
	nonReusablePlanNode
	ifExists bool
}

// startExec implements the planNode interface.
func (n *cancelSessionsNode) startExec(params runParams) error {
	// Execute the node to completion, keeping track of the affected row count.
	for {
		ok, err := n.cancelSession(params)
		if !ok || err != nil {
			return err
		}
		n.incAffectedRows()
	}
}

func (n *cancelSessionsNode) cancelSession(params runParams) (bool, error) {
	// TODO(knz): instead of performing the cancels sequentially,
	// accumulate all the query IDs and then send batches to each of the
	// nodes.

	if ok, err := n.input.Next(params); err != nil || !ok {
		return ok, err
	}

	datum := n.input.Values()[0]
	if datum == tree.DNull {
		return true, nil
	}

	sessionIDString, ok := tree.AsDString(datum)
	if !ok {
		return false, errors.AssertionFailedf("%q: expected *DString, found %T", datum, datum)
	}

	sessionID, err := clusterunique.IDFromString(string(sessionIDString))
	if err != nil {
		return false, pgerror.Wrapf(err, pgcode.Syntax, "invalid session ID %s", datum)
	}

	// Get the lowest 32 bits of the session ID.
	nodeID := sessionID.GetNodeID()

	request := &serverpb.CancelSessionRequest{
		NodeId:    fmt.Sprintf("%d", nodeID),
		SessionID: sessionID.GetBytes(),
		Username:  params.SessionData().User().Normalized(),
	}

	response, err := params.extendedEvalCtx.SQLStatusServer.CancelSession(params.ctx, request)
	if err != nil {
		return false, err
	}

	if !response.Canceled && !n.ifExists {
		return false, errors.Newf("could not cancel session %s: %s", sessionID, response.Error)
	}

	return true, nil
}

// Next implements the planNode interface.
func (n *cancelSessionsNode) Next(_ runParams) (bool, error) {
	return n.next(), nil
}

// Values implements the planNode interface.
func (n *cancelSessionsNode) Values() tree.Datums {
	return n.values()
}

func (n *cancelSessionsNode) Close(ctx context.Context) {
	n.input.Close(ctx)
}
