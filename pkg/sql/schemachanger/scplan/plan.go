// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scplan

import (
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/deprules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/opgen"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/scopt"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/scstage"
	"github.com/cockroachdb/errors"
)

// Params holds the arguments for planning.
type Params struct {
	// ExecutionPhase indicates the phase that the plan should be constructed for.
	ExecutionPhase scop.Phase

	// JobIDGenerator is used in the PreCommit phase to generate the JobID for a
	// job if one should exist. In the PostCommit phase, the value retu
	JobIDGenerator func() jobspb.JobID
}

// A Plan is a schema change plan, primarily containing ops to be executed that
// are partitioned into stages.
type Plan struct {
	Params  Params
	Initial scpb.State
	Graph   *scgraph.Graph
	JobID   jobspb.JobID
	stages  []scstage.Stage
}

// StagesForCurrentPhase returns the stages in the execution phase specified in
// the plan params.
func (p Plan) StagesForCurrentPhase() []scstage.Stage {
	for i, s := range p.stages {
		if s.Phase > p.Params.ExecutionPhase {
			return p.stages[:i]
		}
	}
	return p.stages
}

// StagesForAllPhases returns the stages in the execution phase specified in
// the plan params and also all subsequent stages.
func (p Plan) StagesForAllPhases() []scstage.Stage {
	return p.stages
}

// MakePlan generates a Plan for a particular phase of a schema change, given
// the initial state for a set of targets.
func MakePlan(initial scpb.State, params Params) (p Plan, err error) {
	defer func() {
		if r := recover(); r != nil {
			rAsErr, ok := r.(error)
			if !ok {
				rAsErr = errors.Errorf("panic during MakePlan: %v", r)
			}
			err = errors.CombineErrors(err, rAsErr)
		}
	}()

	p = Plan{
		Initial: initial,
		Params:  params,
	}
	g, err := opgen.BuildGraph(initial)
	if err != nil {
		return p, err
	}
	p.Graph = g
	if err := deprules.Apply(g); err != nil {
		return p, err
	}
	optimizedGraph, err := scopt.OptimizePlan(g)
	if err != nil {
		return p, err
	}
	p.Graph = optimizedGraph

	p.stages = scstage.BuildStages(initial, params.ExecutionPhase, optimizedGraph)
	if err = scstage.ValidateStages(p.stages, p.Graph); err != nil {
		return p, errors.WithAssertionFailure(errors.Wrapf(err, "invalid basic execution plan"))
	}
	p.stages = scstage.CollapseStages(p.stages)
	p.stages, p.JobID = scstage.AugmentStagesForJob(p.stages, params.ExecutionPhase, params.JobIDGenerator)
	if err = scstage.ValidateStages(p.stages, p.Graph); err != nil {
		return p, errors.WithAssertionFailure(errors.Wrapf(err, "invalid improved execution plan"))
	}
	return p, nil
}
