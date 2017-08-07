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
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
)

type controlJobNode struct {
	p             *planner
	jobID         parser.TypedExpr
	desiredStatus jobs.Status
}

func (*controlJobNode) Values() parser.Datums { return nil }

func (n *controlJobNode) Start(params runParams) error {
	jobIDDatum, err := n.jobID.Eval(&n.p.evalCtx)
	if err != nil {
		return err
	}

	jobID, ok := parser.AsDInt(jobIDDatum)
	if !ok {
		return fmt.Errorf("%s is not a valid job ID", jobIDDatum)
	}

	job, err := params.p.ExecCfg().JobRegistry.LoadJob(params.ctx, int64(jobID))
	if err != nil {
		return err
	}

	switch n.desiredStatus {
	case jobs.StatusPaused:
		return job.Paused(params.ctx)
	case jobs.StatusRunning:
		return job.Resumed(params.ctx)
	case jobs.StatusCanceled:
		return job.Canceled(params.ctx)
	default:
		panic("unreachable")
	}
}

func (*controlJobNode) Close(context.Context) {}

func (n *controlJobNode) Next(runParams) (bool, error) {
	return false, nil
}

func (p *planner) PauseJob(ctx context.Context, n *parser.PauseJob) (planNode, error) {
	typedJobID, err := p.analyzeExpr(
		ctx,
		n.ID,
		nil,
		parser.IndexedVarHelper{},
		parser.TypeInt,
		true, /* requireType */
		"PAUSE JOB",
	)
	if err != nil {
		return nil, err
	}

	return &controlJobNode{
		p:             p,
		jobID:         typedJobID,
		desiredStatus: jobs.StatusPaused,
	}, nil
}

func (p *planner) ResumeJob(ctx context.Context, n *parser.ResumeJob) (planNode, error) {
	typedJobID, err := p.analyzeExpr(
		ctx,
		n.ID,
		nil,
		parser.IndexedVarHelper{},
		parser.TypeInt,
		true, /* requireType */
		"RESUME JOB",
	)
	if err != nil {
		return nil, err
	}

	return &controlJobNode{
		p:             p,
		jobID:         typedJobID,
		desiredStatus: jobs.StatusRunning,
	}, nil
}

func (p *planner) CancelJob(ctx context.Context, n *parser.CancelJob) (planNode, error) {
	typedJobID, err := p.analyzeExpr(
		ctx,
		n.ID,
		nil,
		parser.IndexedVarHelper{},
		parser.TypeInt,
		true, /* requireType */
		"CANCEL JOB",
	)
	if err != nil {
		return nil, err
	}

	return &controlJobNode{
		p:             p,
		jobID:         typedJobID,
		desiredStatus: jobs.StatusCanceled,
	}, nil
}

type cancelQueryNode struct {
	p       *planner
	queryID parser.TypedExpr
}

func (*cancelQueryNode) Values() parser.Datums { return nil }

func (n *cancelQueryNode) Start(params runParams) error {
	statusServer := n.p.session.execCfg.StatusServer

	queryIDDatum, err := n.queryID.Eval(&n.p.evalCtx)
	if err != nil {
		return err
	}

	queryIDString := parser.AsStringWithFlags(queryIDDatum, parser.FmtBareStrings)
	queryID, err := uint128.FromString(queryIDString)
	if err != nil {
		return errors.Wrapf(err, "Invalid query ID '%s'", queryIDString)
	}

	// Get the lowest 32 bits of the query ID.
	nodeID := 0xFFFFFFFF & queryID.Lo

	request := &serverpb.CancelQueryRequest{
		NodeId:   fmt.Sprintf("%d", nodeID),
		QueryID:  queryIDString,
		Username: n.p.session.User,
	}

	response, err := statusServer.CancelQuery(params.ctx, request)
	if err != nil {
		return err
	}

	if !response.Cancelled {
		return fmt.Errorf("Could not cancel query %s: %s", queryID, response.Error)
	}

	return nil
}

func (*cancelQueryNode) Close(context.Context) {}

func (n *cancelQueryNode) Next(runParams) (bool, error) {
	return false, nil
}

func (p *planner) CancelQuery(ctx context.Context, n *parser.CancelQuery) (planNode, error) {

	typedQueryID, err := p.analyzeExpr(
		ctx,
		n.ID,
		nil,
		parser.IndexedVarHelper{},
		parser.TypeString,
		true, /* requireType */
		"CANCEL QUERY",
	)
	if err != nil {
		return nil, err
	}

	return &cancelQueryNode{
		p:       p,
		queryID: typedQueryID,
	}, nil
}
