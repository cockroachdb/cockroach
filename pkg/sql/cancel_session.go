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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/pkg/errors"
)

type cancelSessionNode struct {
	sessionID tree.TypedExpr
	ifExists  bool
}

func (p *planner) CancelSession(ctx context.Context, n *tree.CancelSession) (planNode, error) {
	typedSessionID, err := p.analyzeExpr(
		ctx,
		n.ID,
		nil,
		tree.IndexedVarHelper{},
		types.String,
		true, /* requireType */
		"CANCEL SESSION",
	)
	if err != nil {
		return nil, err
	}

	return &cancelSessionNode{
		sessionID: typedSessionID,
		ifExists:  n.IfExists,
	}, nil
}

func (n *cancelSessionNode) startExec(params runParams) error {
	statusServer := params.extendedEvalCtx.StatusServer

	sessionIDDatum, err := n.sessionID.Eval(params.EvalContext())
	if err != nil {
		return err
	}

	sessionIDString := tree.AsStringWithFlags(sessionIDDatum, tree.FmtBareStrings)
	sessionID, err := StringToClusterWideID(sessionIDString)
	if err != nil {
		return errors.Wrapf(err, "invalid session ID '%s'", sessionIDString)
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
		return err
	}

	if !response.Canceled && !n.ifExists {
		return fmt.Errorf("could not cancel session %s: %s", sessionID, response.Error)
	}

	return nil
}

func (n *cancelSessionNode) Next(runParams) (bool, error) { return false, nil }
func (*cancelSessionNode) Values() tree.Datums            { return nil }
func (*cancelSessionNode) Close(context.Context)          {}
