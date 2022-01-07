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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/deprules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/opgen"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/scopt"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/scstage"
	"github.com/cockroachdb/errors"
)

// Params holds the arguments for planning.
type Params struct {
	// ExecutionPhase indicates the phase that the plan should be constructed for.
	ExecutionPhase scop.Phase

	// SchemaChangerJobIDSupplier is used to return the JobID for a
	// job if one should exist.
	SchemaChangerJobIDSupplier func() jobspb.JobID
}

// A Plan is a schema change plan, primarily containing ops to be executed that
// are partitioned into stages.
type Plan struct {
	Params  Params
	Initial scpb.CurrentState
	Graph   *scgraph.Graph
	JobID   jobspb.JobID
	Stages  []scstage.Stage
}

// StagesForCurrentPhase returns the stages in the execution phase specified in
// the plan params.
func (p Plan) StagesForCurrentPhase() []scstage.Stage {
	for i, s := range p.Stages {
		if s.Phase > p.Params.ExecutionPhase {
			return p.Stages[:i]
		}
	}
	return p.Stages
}

// MakePlan generates a Plan for a particular phase of a schema change, given
// the initial state for a set of targets.
// Returns an error when planning fails. It is up to the caller to wrap this
// error as an assertion failure and with useful debug information details.
func MakePlan(initial scpb.CurrentState, params Params) (p Plan, err error) {
	p = Plan{
		Initial: initial,
		Params:  params,
	}
	defer func() {
		if r := recover(); r != nil {
			rAsErr, ok := r.(error)
			if !ok {
				rAsErr = errors.Errorf("panic during MakePlan: %v", r)
			}
			err = errors.CombineErrors(err, rAsErr)
		}
	}()

	p.Graph = buildGraph(initial)
	p.Stages = scstage.BuildStages(initial, params.ExecutionPhase, p.Graph, params.SchemaChangerJobIDSupplier)
	if n := len(p.Stages); n > 0 && p.Stages[n-1].Phase > scop.PreCommitPhase {
		// Only get the job ID if it's actually been assigned already.
		p.JobID = params.SchemaChangerJobIDSupplier()
	}
	if err = scstage.ValidateStages(p.Stages, p.Graph); err != nil {
		panic(errors.Wrapf(err, "invalid execution plan"))
	}
	return p, nil
}

func buildGraph(initial scpb.CurrentState) *scgraph.Graph {
	g, err := opgen.BuildGraph(initial)
	if err != nil {
		panic(errors.Wrapf(err, "build graph op edges"))
	}
	err = deprules.Apply(g)
	if err != nil {
		panic(errors.Wrapf(err, "build graph dep edges"))
	}
	err = g.Validate()
	if err != nil {
		panic(errors.Wrapf(err, "validate graph"))
	}
	g, err = scopt.OptimizeGraph(g)
	if err != nil {
		panic(errors.Wrapf(err, "mark op edges as no-op"))
	}
	return g
}
