// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scrun

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraphviz"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/scstage"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// RunStatementPhase executes in-transaction schema changes for the targeted
// state. These are the immediate changes which take place at DDL statement
// execution time (scop.StatementPhase).
func RunStatementPhase(
	ctx context.Context, knobs *TestingKnobs, deps scexec.Dependencies, state scpb.State,
) (scpb.State, jobspb.JobID, error) {
	return runTransactionPhase(ctx, knobs, deps, state, scop.StatementPhase)
}

// RunPreCommitPhase executes in-transaction schema changes for the targeted
// state. These are run when executing COMMIT (scop.PreCommitPhase), rather
// than the asynchronous changes which are done by the schema changer job
// after the transaction commits.
func RunPreCommitPhase(
	ctx context.Context, knobs *TestingKnobs, deps scexec.Dependencies, state scpb.State,
) (scpb.State, jobspb.JobID, error) {
	return runTransactionPhase(ctx, knobs, deps, state, scop.PreCommitPhase)
}

func runTransactionPhase(
	ctx context.Context,
	knobs *TestingKnobs,
	deps scexec.Dependencies,
	state scpb.State,
	phase scop.Phase,
) (scpb.State, jobspb.JobID, error) {
	if len(state.Nodes) == 0 {
		return scpb.State{}, jobspb.InvalidJobID, nil
	}
	sc, err := scplan.MakePlan(state, scplan.Params{
		ExecutionPhase:             phase,
		SchemaChangerJobIDSupplier: deps.TransactionalJobRegistry().SchemaChangerJobID,
	})
	if err != nil {
		return scpb.State{}, jobspb.InvalidJobID, scgraphviz.DecorateErrorWithPlanDetails(err, sc)
	}
	after := state
	stages := sc.StagesForCurrentPhase()
	for i := range stages {
		if err := executeStage(ctx, knobs, deps, sc, i, stages[i]); err != nil {
			return scpb.State{}, jobspb.InvalidJobID, err
		}
		after = stages[i].After
	}
	if len(after.Nodes) == 0 {
		return scpb.State{}, jobspb.InvalidJobID, nil
	}
	return after, sc.JobID, nil
}

// RunSchemaChangesInJob contains the business logic for the Resume method of a
// declarative schema change job, with the dependencies abstracted away.
func RunSchemaChangesInJob(
	ctx context.Context,
	knobs *TestingKnobs,
	settings *cluster.Settings,
	deps JobRunDependencies,
	jobID jobspb.JobID,
	jobDescriptorIDs []descpb.ID,
	jobDetails jobspb.NewSchemaChangeDetails,
	jobProgress jobspb.NewSchemaChangeProgress,
	rollback bool,
) error {
	state := makeState(ctx,
		settings,
		jobDetails.Targets,
		jobProgress.States,
		jobProgress.Statements,
		jobProgress.Authorization,
		rollback)
	sc, err := scplan.MakePlan(state, scplan.Params{
		ExecutionPhase:             scop.PostCommitPhase,
		SchemaChangerJobIDSupplier: func() jobspb.JobID { return jobID },
	})
	if err != nil {
		return scgraphviz.DecorateErrorWithPlanDetails(err, sc)
	}

	for i := range sc.Stages {
		// Execute each stage in its own transaction.
		if err := deps.WithTxnInJob(ctx, func(ctx context.Context, td scexec.Dependencies) error {
			return executeStage(ctx, knobs, td, sc, i, sc.Stages[i])
		}); err != nil {
			return err
		}
	}
	return nil
}

func executeStage(
	ctx context.Context,
	knobs *TestingKnobs,
	deps scexec.Dependencies,
	p scplan.Plan,
	stageIdx int,
	stage scstage.Stage,
) error {
	if knobs != nil && knobs.BeforeStage != nil {
		if err := knobs.BeforeStage(p, stageIdx); err != nil {
			return err
		}
	}
	err := scexec.ExecuteStage(ctx, deps, stage.Ops())
	if err != nil {
		err = errors.Wrapf(err, "error executing %s", stage.String())
		return scgraphviz.DecorateErrorWithPlanDetails(err, p)
	}
	return nil
}

func makeState(
	ctx context.Context,
	sv *cluster.Settings,
	protos []*scpb.Target,
	states []scpb.Status,
	statements []*scpb.Statement,
	authorization *scpb.Authorization,
	rollback bool,
) scpb.State {
	if len(protos) != len(states) {
		logcrash.ReportOrPanic(ctx, &sv.SV, "unexpected slice size mismatch %d and %d",
			len(protos), len(states))
	}
	ts := scpb.State{
		TargetState: scpb.TargetState{
			Targets:       make([]scpb.Target, len(protos)),
			Statements:    make([]scpb.Statement, len(statements)),
			Authorization: *authorization,
		},
		Nodes: make([]*scpb.Node, len(states)),
	}
	for i, proto := range protos {
		t := protoutil.Clone(proto).(*scpb.Target)
		if rollback {
			switch t.TargetStatus {
			case scpb.Status_PUBLIC:
				t.TargetStatus = scpb.Status_ABSENT
			case scpb.Status_ABSENT:
				t.TargetStatus = scpb.Status_PUBLIC
			}
		}
		ts.Targets[i] = *t
	}
	for j, stmt := range statements {
		ts.Statements[j] = *protoutil.Clone(stmt).(*scpb.Statement)
	}
	for i, status := range states {
		ts.Nodes[i] = &scpb.Node{
			Target: &ts.Targets[i],
			Status: status,
		}
	}
	return ts
}
