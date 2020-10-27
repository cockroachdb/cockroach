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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type cancelSessionsNode struct {
	rows     planNode
	ifExists bool
}

func (n *cancelSessionsNode) startExec(runParams) error {
	return nil
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

	sessionIDString, ok := tree.AsDString(datum)
	if !ok {
		return false, errors.AssertionFailedf("%q: expected *DString, found %T", datum, datum)
	}

	sessionID, err := StringToClusterWideID(string(sessionIDString))
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

func (*cancelSessionsNode) Values() tree.Datums { return nil }

func (n *cancelSessionsNode) Close(ctx context.Context) {
	n.rows.Close(ctx)
}
