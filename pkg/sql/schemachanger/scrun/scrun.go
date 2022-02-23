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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// RunStatementPhase executes in-transaction schema changes for the targeted
// state. These are the immediate changes which take place at DDL statement
// execution time (scop.StatementPhase).
func RunStatementPhase(
	ctx context.Context, knobs *TestingKnobs, deps scexec.Dependencies, state scpb.CurrentState,
) (scpb.CurrentState, jobspb.JobID, error) {
	return runTransactionPhase(ctx, knobs, deps, state, scop.StatementPhase)
}

// RunPreCommitPhase executes in-transaction schema changes for the targeted
// state. These are run when executing COMMIT (scop.PreCommitPhase), rather
// than the asynchronous changes which are done by the schema changer job
// after the transaction commits.
func RunPreCommitPhase(
	ctx context.Context, knobs *TestingKnobs, deps scexec.Dependencies, state scpb.CurrentState,
) (scpb.CurrentState, jobspb.JobID, error) {
	return runTransactionPhase(ctx, knobs, deps, state, scop.PreCommitPhase)
}

func runTransactionPhase(
	ctx context.Context,
	knobs *TestingKnobs,
	deps scexec.Dependencies,
	state scpb.CurrentState,
	phase scop.Phase,
) (scpb.CurrentState, jobspb.JobID, error) {
	if len(state.Current) == 0 {
		return scpb.CurrentState{}, jobspb.InvalidJobID, nil
	}
	sc, err := scplan.MakePlan(state, scplan.Params{
		ExecutionPhase:             phase,
		SchemaChangerJobIDSupplier: deps.TransactionalJobRegistry().SchemaChangerJobID,
	})
	if err != nil {
		return scpb.CurrentState{}, jobspb.InvalidJobID, err
	}
	after := state.Current
	if len(after) == 0 {
		return scpb.CurrentState{}, jobspb.InvalidJobID, nil
	}
	stages := sc.StagesForCurrentPhase()
	for i := range stages {
		if err := executeStage(ctx, knobs, deps, sc, i, stages[i]); err != nil {
			return scpb.CurrentState{}, jobspb.InvalidJobID, err
		}
		after = stages[i].After
	}
	return scpb.CurrentState{TargetState: state.TargetState, Current: after}, sc.JobID, nil
}

// RunSchemaChangesInJob contains the business logic for the Resume method of a
// declarative schema change job, with the dependencies abstracted away.
func RunSchemaChangesInJob(
	ctx context.Context,
	knobs *TestingKnobs,
	settings *cluster.Settings,
	deps JobRunDependencies,
	jobID jobspb.JobID,
	descriptorIDs []descpb.ID,
	rollback bool,
) error {
	state, err := makeState(ctx, deps, descriptorIDs, rollback)
	if err != nil {
		return errors.Wrapf(err, "failed to construct state for job %d", jobID)
	}
	sc, err := scplan.MakePlan(state, scplan.Params{
		ExecutionPhase:             scop.PostCommitPhase,
		SchemaChangerJobIDSupplier: func() jobspb.JobID { return jobID },
	})
	if err != nil {
		return err
	}

	for i := range sc.Stages {
		// Execute each stage in its own transaction.
		if err := deps.WithTxnInJob(ctx, func(ctx context.Context, td scexec.Dependencies) error {
			if err := td.TransactionalJobRegistry().CheckPausepoint(
				pausepointName(state, i),
			); err != nil {
				return err
			}
			return executeStage(ctx, knobs, td, sc, i, sc.Stages[i])
		}); err != nil {
			if knobs.OnPostCommitError != nil {
				return knobs.OnPostCommitError(sc, i, err)
			}
			return err
		}
	}
	return nil
}

// pausepointName construct a name for the job execution phase pausepoint.
func pausepointName(state scpb.CurrentState, i int) string {
	return fmt.Sprintf(
		"schemachanger.%s.%s.%d",
		state.Authorization.UserName, state.Authorization.AppName, i,
	)
}

func executeStage(
	ctx context.Context,
	knobs *TestingKnobs,
	deps scexec.Dependencies,
	p scplan.Plan,
	stageIdx int,
	stage scplan.Stage,
) error {
	if knobs != nil && knobs.BeforeStage != nil {
		if err := knobs.BeforeStage(p, stageIdx); err != nil {
			return err
		}
	}

	log.Infof(ctx, "executing stage %d/%d in phase %v, %d ops of type %s (rollback=%v)",
		stage.Ordinal, stage.StagesInPhase, stage.Phase, len(stage.Ops()), stage.Ops()[0].Type(), p.InRollback)
	if err := scexec.ExecuteStage(ctx, deps, stage.Ops()); err != nil {
		return errors.Wrapf(p.DecorateErrorWithPlanDetails(err), "error executing %s", stage.String())
	}
	return nil
}

func makeState(
	ctx context.Context, deps JobRunDependencies, descriptorIDs []descpb.ID, rollback bool,
) (scpb.CurrentState, error) {
	var descriptorStates []*scpb.DescriptorState
	if err := deps.WithTxnInJob(ctx, func(ctx context.Context, txnDeps scexec.Dependencies) error {
		descriptorStates = nil
		// Reset for restarts.
		descs, err := txnDeps.Catalog().MustReadImmutableDescriptors(ctx, descriptorIDs...)
		if err != nil {
			// TODO(ajwerner): It seems possible that a descriptor could be deleted
			// and the schema change is in a happy place. Ideally we'd enforce that
			// descriptors may only be deleted on the very last step of the schema
			// change.
			return err
		}
		for _, desc := range descs {
			// TODO(ajwerner): Verify that the job ID matches on all of the
			// descriptors. Also verify that the Authorization matches.
			cs := desc.GetDeclarativeSchemaChangerState()
			if cs == nil {
				return errors.Errorf(
					"descriptor %q (%d) does not contain schema changer state", desc.GetName(), desc.GetID(),
				)
			}
			descriptorStates = append(descriptorStates, cs)
		}
		return nil
	}); err != nil {
		return scpb.CurrentState{}, err
	}
	state, err := scpb.MakeCurrentStateFromDescriptors(descriptorStates)
	if err != nil {
		return scpb.CurrentState{}, err
	}
	if !rollback && state.InRollback {
		return scpb.CurrentState{}, errors.Errorf(
			"job in running state but schema change in rollback, " +
				"returning an error to restart in the reverting state")
	}
	if rollback && !state.InRollback {
		state.Rollback()
	}
	return state, nil
}
