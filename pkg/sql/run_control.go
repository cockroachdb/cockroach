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
	"fmt"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
)

type controlJobNode struct {
	jobID         tree.TypedExpr
	desiredStatus jobs.Status
}

func (*controlJobNode) Values() tree.Datums { return nil }

func (n *controlJobNode) Start(params runParams) error {
	jobIDDatum, err := n.jobID.Eval(params.evalCtx)
	if err != nil {
		return err
	}

	jobID, ok := tree.AsDInt(jobIDDatum)
	if !ok {
		return fmt.Errorf("%s is not a valid job ID", jobIDDatum)
	}

	reg := params.p.ExecCfg().JobRegistry
	switch n.desiredStatus {
	case jobs.StatusPaused:
		return reg.Pause(params.ctx, params.p.txn, int64(jobID))
	case jobs.StatusRunning:
		return reg.Resume(params.ctx, params.p.txn, int64(jobID))
	case jobs.StatusCanceled:
		return reg.Cancel(params.ctx, params.p.txn, int64(jobID))
	default:
		panic("unreachable")
	}
}

func (*controlJobNode) Close(context.Context) {}

func (n *controlJobNode) Next(runParams) (bool, error) {
	return false, nil
}

func (p *planner) PauseJob(ctx context.Context, n *tree.PauseJob) (planNode, error) {
	typedJobID, err := p.analyzeExpr(
		ctx,
		n.ID,
		nil,
		tree.IndexedVarHelper{},
		types.Int,
		true, /* requireType */
		"PAUSE JOB",
	)
	if err != nil {
		return nil, err
	}

	return &controlJobNode{
		jobID:         typedJobID,
		desiredStatus: jobs.StatusPaused,
	}, nil
}

func (p *planner) ResumeJob(ctx context.Context, n *tree.ResumeJob) (planNode, error) {
	typedJobID, err := p.analyzeExpr(
		ctx,
		n.ID,
		nil,
		tree.IndexedVarHelper{},
		types.Int,
		true, /* requireType */
		"RESUME JOB",
	)
	if err != nil {
		return nil, err
	}

	return &controlJobNode{
		jobID:         typedJobID,
		desiredStatus: jobs.StatusRunning,
	}, nil
}

func (p *planner) CancelJob(ctx context.Context, n *tree.CancelJob) (planNode, error) {
	typedJobID, err := p.analyzeExpr(
		ctx,
		n.ID,
		nil,
		tree.IndexedVarHelper{},
		types.Int,
		true, /* requireType */
		"CANCEL JOB",
	)
	if err != nil {
		return nil, err
	}

	return &controlJobNode{
		jobID:         typedJobID,
		desiredStatus: jobs.StatusCanceled,
	}, nil
}

type cancelQueryNode struct {
	queryID tree.TypedExpr
}

func (*cancelQueryNode) Values() tree.Datums { return nil }

func (n *cancelQueryNode) Start(params runParams) error {
	statusServer := params.p.session.execCfg.StatusServer

	queryIDDatum, err := n.queryID.Eval(params.evalCtx)
	if err != nil {
		return err
	}

	queryIDString := tree.AsStringWithFlags(queryIDDatum, tree.FmtBareStrings)
	queryID, err := uint128.FromString(queryIDString)
	if err != nil {
		return errors.Wrapf(err, "invalid query ID '%s'", queryIDString)
	}

	// Get the lowest 32 bits of the query ID.
	nodeID := 0xFFFFFFFF & queryID.Lo

	request := &serverpb.CancelQueryRequest{
		NodeId:   fmt.Sprintf("%d", nodeID),
		QueryID:  queryIDString,
		Username: params.p.session.User,
	}

	response, err := statusServer.CancelQuery(params.ctx, request)
	if err != nil {
		return err
	}

	if !response.Cancelled {
		return fmt.Errorf("could not cancel query %s: %s", queryID, response.Error)
	}

	return nil
}

func (*cancelQueryNode) Close(context.Context) {}

func (n *cancelQueryNode) Next(runParams) (bool, error) {
	return false, nil
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
		queryID: typedQueryID,
	}, nil
}
