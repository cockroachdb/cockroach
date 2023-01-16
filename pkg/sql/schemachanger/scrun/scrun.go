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

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var enforcePlannerSanityCheck = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.schemachanger.strict_planning_sanity_check.enabled",
	"enforce strict sanity checks in the declarative schema changer planner",
	buildutil.CrdbTestBuild,
)

// RunStatementPhase executes in-transaction schema changes for the targeted
// state. These are the immediate changes which take place at DDL statement
// execution time (scop.StatementPhase).
func RunStatementPhase(
	ctx context.Context,
	knobs *scexec.TestingKnobs,
	deps scexec.Dependencies,
	state scpb.CurrentState,
) (scpb.CurrentState, jobspb.JobID, error) {
	return runTransactionPhase(ctx, knobs, deps, state, scop.StatementPhase)
}

// RunPreCommitPhase executes in-transaction schema changes for the targeted
// state. These are run when executing COMMIT (scop.PreCommitPhase), rather
// than the asynchronous changes which are done by the schema changer job
// after the transaction commits.
func RunPreCommitPhase(
	ctx context.Context,
	knobs *scexec.TestingKnobs,
	deps scexec.Dependencies,
	state scpb.CurrentState,
) (scpb.CurrentState, jobspb.JobID, error) {
	return runTransactionPhase(ctx, knobs, deps, state, scop.PreCommitPhase)
}

func runTransactionPhase(
	ctx context.Context,
	knobs *scexec.TestingKnobs,
	deps scexec.Dependencies,
	state scpb.CurrentState,
	phase scop.Phase,
) (scpb.CurrentState, jobspb.JobID, error) {
	if len(state.Current) == 0 {
		return scpb.CurrentState{}, jobspb.InvalidJobID, nil
	}
	sc, err := scplan.MakePlan(ctx, state, scplan.Params{
		ActiveVersion:              deps.ClusterSettings().Version.ActiveVersion(ctx),
		ExecutionPhase:             phase,
		SchemaChangerJobIDSupplier: deps.TransactionalJobRegistry().SchemaChangerJobID,
		EnforcePlannerSanityCheck:  enforcePlannerSanityCheck.Get(&deps.ClusterSettings().SV),
	})
	if err != nil {
		return scpb.CurrentState{}, jobspb.InvalidJobID, err
	}
	after := state.Current
	if len(after) == 0 {
		return scpb.CurrentState{}, jobspb.InvalidJobID, nil
	}
	stages := sc.StagesForCurrentPhase()
	if len(stages) == 0 {
		// Go through the pre-commit stage execution machinery anyway, catalog
		// change side effects are applied only in memory in the statement phase
		// and need to be applied in storage otherwise they will be lost.
		if err := scexec.ExecuteStage(ctx, deps, phase, nil /* ops */); err != nil {
			return scpb.CurrentState{}, jobspb.InvalidJobID, err
		}
	}
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
	knobs *scexec.TestingKnobs,
	deps JobRunDependencies,
	jobID jobspb.JobID,
	descriptorIDs []descpb.ID,
	rollback bool,
) error {
	state, err := makeState(ctx, jobID, descriptorIDs, rollback, func(
		ctx context.Context, f catalogFunc,
	) error {
		return deps.WithTxnInJob(ctx, func(
			ctx context.Context, txnDeps scexec.Dependencies,
		) error {
			return f(ctx, txnDeps.Catalog())
		})
	})
	if err != nil {
		if knobs != nil && knobs.OnPostCommitPlanError != nil {
			return knobs.OnPostCommitPlanError(nil, err)
		}
		return errors.Wrapf(err, "failed to construct state for job %d", jobID)
	}
	sc, err := scplan.MakePlan(ctx, state, scplan.Params{
		ActiveVersion:              deps.ClusterSettings().Version.ActiveVersion(ctx),
		ExecutionPhase:             scop.PostCommitPhase,
		SchemaChangerJobIDSupplier: func() jobspb.JobID { return jobID },
	})
	if err != nil {
		if knobs != nil && knobs.OnPostCommitPlanError != nil {
			return knobs.OnPostCommitPlanError(&state, err)
		}
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
			if knobs != nil && knobs.OnPostCommitError != nil {
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
	knobs *scexec.TestingKnobs,
	deps scexec.Dependencies,
	p scplan.Plan,
	stageIdx int,
	stage scplan.Stage,
) (err error) {
	defer scerrors.StartEventf(
		ctx,
		"executing declarative schema change %s (rollback=%v) for %s",
		redact.Safe(stage),
		redact.Safe(p.InRollback),
		redact.Safe(p.StatementTags()),
	).HandlePanicAndLogError(ctx, &err)
	if knobs != nil && knobs.BeforeStage != nil {
		if err := knobs.BeforeStage(p, stageIdx); err != nil {
			return err
		}
	}
	if err := scexec.ExecuteStage(ctx, deps, stage.Phase, stage.Ops()); err != nil {
		// Don't go through the effort to wrap the error if it's a retry or it's a
		// cancelation.
		if !errors.HasType(err, (*roachpb.TransactionRetryWithProtoRefreshError)(nil)) &&
			!errors.Is(err, context.Canceled) &&
			!scerrors.HasSchemaChangerUserError(err) {
			err = p.DecorateErrorWithPlanDetails(err)
		}
		// Certain errors are aimed to be user consumable and should never be
		// wrapped.
		if scerrors.HasSchemaChangerUserError(err) {
			return errors.Unwrap(err)
		}
		return errors.Wrapf(err, "error executing %s", stage)
	}
	if knobs != nil && knobs.AfterStage != nil {
		if err := knobs.AfterStage(p, stageIdx); err != nil {
			return err
		}
	}

	return nil
}

type (
	catalogFunc     = func(context.Context, scexec.Catalog) error
	withCatalogFunc = func(context.Context, catalogFunc) error
)

func makeState(
	ctx context.Context,
	jobID jobspb.JobID,
	descriptorIDs []descpb.ID,
	rollback bool,
	withCatalog withCatalogFunc,
) (state scpb.CurrentState, err error) {
	defer scerrors.StartEventf(
		ctx,
		"rebuilding declarative schema change state from descriptors %v",
		redact.Safe(descriptorIDs),
	).HandlePanicAndLogError(ctx, &err)
	descError := func(desc catalog.Descriptor, err error) error {
		return errors.Wrapf(err, "descriptor %q (%d)", desc.GetName(), desc.GetID())
	}
	validateJobID := func(fromDesc jobspb.JobID) error {
		switch {
		case fromDesc == jobspb.InvalidJobID:
			return errors.New("missing job ID in schema changer state")
		case fromDesc != jobID:
			return errors.Errorf("job ID mismatch: expected %d, got %d",
				jobID, fromDesc)
		default:
			return nil
		}
	}
	var authorization scpb.Authorization
	validateAuthorization := func(fromDesc scpb.Authorization) error {
		switch {
		case fromDesc == (scpb.Authorization{}):
			return errors.New("missing authorization in schema changer state")
		case authorization == (scpb.Authorization{}):
			authorization = fromDesc
		case authorization != fromDesc:
			return errors.Errorf("authorization mismatch: expected %v, got %v",
				authorization, fromDesc)
		}
		return nil
	}
	var descriptorStates []*scpb.DescriptorState
	addDescriptorState := func(desc catalog.Descriptor) error {
		cs := desc.GetDeclarativeSchemaChangerState()
		if cs == nil {
			return errors.New("missing schema changer state")
		}
		if err := validateJobID(cs.JobID); err != nil {
			return err
		}
		if err := validateAuthorization(cs.Authorization); err != nil {
			return err
		}
		descriptorStates = append(descriptorStates, cs)
		return nil
	}
	if err := withCatalog(ctx, func(
		ctx context.Context, cat scexec.Catalog,
	) error {
		descriptorStates = nil // reset for restarts
		descs, err := cat.MustReadImmutableDescriptors(ctx, descriptorIDs...)
		if err != nil {
			// TODO(ajwerner): It seems possible that a descriptor could be deleted
			// and the schema change is in a happy place. Ideally we'd enforce that
			// descriptors may only be deleted on the very last step of the schema
			// change.
			return err
		}
		for _, desc := range descs {
			if err := addDescriptorState(desc); err != nil {
				return descError(desc, err)
			}
		}
		return nil
	}); err != nil {
		return scpb.CurrentState{}, err
	}
	state, err = scpb.MakeCurrentStateFromDescriptors(descriptorStates)
	if err != nil {
		return scpb.CurrentState{}, err
	}
	if !rollback && state.InRollback {
		// If we do not mark the error as permanent, but we've configured the job to
		// be non-cancelable, we'll never make it to the reverting state.
		return scpb.CurrentState{}, jobs.MarkAsPermanentJobError(errors.Errorf(
			"job in running state but schema change in rollback, " +
				"returning an error to restart in the reverting state"))
	}
	if rollback && !state.InRollback {
		state.Rollback()
	}
	return state, nil
}
