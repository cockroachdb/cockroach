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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
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
