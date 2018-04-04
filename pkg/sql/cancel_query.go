// Copyright 2017 The Cockroach Authors.
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

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

type cancelQueryNode struct {
	queryID  tree.TypedExpr
	ifExists bool
}

func (p *planner) CancelQuery(ctx context.Context, n *tree.CancelQuery) (planNode, error) {
	typedQueryID, err := p.analyzeExpr(
		ctx,
		n.ID,
		nil,
		tree.IndexedVarHelper{},
		types.String,
		true, /* requireType */
		"CANCEL QUERY",
	)
	if err != nil {
		return nil, err
	}

	return &cancelQueryNode{
		queryID:  typedQueryID,
		ifExists: n.IfExists,
	}, nil
}

func (n *cancelQueryNode) startExec(params runParams) error {
	statusServer := params.extendedEvalCtx.StatusServer

	queryIDDatum, err := n.queryID.Eval(params.EvalContext())
	if err != nil {
		return err
	}

	queryIDString := tree.AsStringWithFlags(queryIDDatum, tree.FmtBareStrings)
	queryID, err := StringToClusterWideID(queryIDString)
	if err != nil {
		return errors.Wrapf(err, "invalid query ID '%s'", queryIDString)
	}

	// Get the lowest 32 bits of the query ID.
	nodeID := 0xFFFFFFFF & queryID.Lo

	request := &serverpb.CancelQueryRequest{
		NodeId:   fmt.Sprintf("%d", nodeID),
		QueryID:  queryIDString,
		Username: params.SessionData().User,
	}

	response, err := statusServer.CancelQuery(params.ctx, request)
	if err != nil {
		return err
	}

	if !response.Canceled && !n.ifExists {
		return fmt.Errorf("could not cancel query %s: %s", queryID, response.Error)
	}

	return nil
}

func (n *cancelQueryNode) Next(runParams) (bool, error) { return false, nil }
func (*cancelQueryNode) Values() tree.Datums            { return nil }
func (*cancelQueryNode) Close(context.Context)          {}
