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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/opgen"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraphviz"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scstage"
	"github.com/cockroachdb/errors"
)

// Params holds the arguments for planning.
type Params struct {
	// InRollback is used to indicate whether we've already been reverted.
	// Note that when in rollback, there is no turning back and all work is
	// non-revertible. Theory dictates that this is fine because of how we
	// had carefully crafted stages to only allow entering rollback while it
	// remains safe to do so.
	InRollback bool

	// ExecutionPhase indicates the phase that the plan should be constructed for.
	ExecutionPhase scop.Phase

	// SchemaChangerJobIDSupplier is used to return the JobID for a
	// job if one should exist.
	SchemaChangerJobIDSupplier func() jobspb.JobID
}

// Exported internal types
type (
	// Graph is an exported alias of scgraph.Graph.
	Graph = scgraph.Graph

	// Stage is an exported alias of scstage.Stage.
	Stage = scstage.Stage
)

// A Plan is a schema change plan, primarily containing ops to be executed that
// are partitioned into stages.
type Plan struct {
	scpb.CurrentState
	Params Params
	Graph  *scgraph.Graph
	JobID  jobspb.JobID
	Stages []Stage
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

// DecorateErrorWithPlanDetails adds plan graphviz URLs as error details.
func (p Plan) DecorateErrorWithPlanDetails(err error) error {
	return scgraphviz.DecorateErrorWithPlanDetails(err, p.CurrentState, p.Graph, p.Stages)
}

// DependenciesURL returns a URL to render the dependency graph in the Plan.
func (p Plan) DependenciesURL() (string, error) {
	return scgraphviz.DependenciesURL(p.CurrentState, p.Graph)
}

// StagesURL returns a URL to render the stages in the Plan.
func (p Plan) StagesURL() (string, error) {
	return scgraphviz.StagesURL(p.CurrentState, p.Graph, p.Stages)
}

// MakePlan generates a Plan for a particular phase of a schema change, given
// the initial state for a set of targets.
// Returns an error when planning fails. It is up to the caller to wrap this
// error as an assertion failure and with useful debug information details.
func MakePlan(initial scpb.CurrentState, params Params) (p Plan, err error) {
	p = Plan{
		CurrentState: initial,
		Params:       params,
	}
	defer func() {
		if r := recover(); r != nil {
			rAsErr, ok := r.(error)
			if !ok {
				rAsErr = errors.Errorf("panic during MakePlan: %v", r)
			}
			err = p.DecorateErrorWithPlanDetails(rAsErr)
		}
	}()

	p.Graph = buildGraph(p.CurrentState)
	p.Stages = scstage.BuildStages(initial, params.ExecutionPhase, p.Graph, params.SchemaChangerJobIDSupplier)
	if n := len(p.Stages); n > 0 && p.Stages[n-1].Phase > scop.PreCommitPhase {
		// Only get the job ID if it's actually been assigned already.
		p.JobID = params.SchemaChangerJobIDSupplier()
	}
	if err := scstage.ValidateStages(p.TargetState, p.Stages, p.Graph); err != nil {
		panic(errors.Wrapf(err, "invalid execution plan"))
	}
	return p, nil
}

func buildGraph(cs scpb.CurrentState) *scgraph.Graph {
	g, err := opgen.BuildGraph(cs)
	if err != nil {
		panic(errors.Wrapf(err, "build graph op edges"))
	}
	err = rules.ApplyDepRules(g)
	if err != nil {
		panic(errors.Wrapf(err, "build graph dep edges"))
	}
	err = g.Validate()
	if err != nil {
		panic(errors.Wrapf(err, "validate graph"))
	}
	g, err = rules.ApplyOpRules(g)
	if err != nil {
		panic(errors.Wrapf(err, "mark op edges as no-op"))
	}
	return g
}
