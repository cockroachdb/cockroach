// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scrun

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var enforcePlannerSanityCheck = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
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
		SkipPlannerSanityChecks:    !enforcePlannerSanityCheck.Get(&deps.ClusterSettings().SV),
		MemAcc:                     mon.NewStandaloneUnlimitedAccount(),
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
	return state.WithCurrentStatuses(after), sc.JobID, nil
}

// RunSchemaChangesInJob contains the business logic for the Resume method of a
// declarative schema change job, with the dependencies abstracted away.
func RunSchemaChangesInJob(
	ctx context.Context,
	knobs *scexec.TestingKnobs,
	deps JobRunDependencies,
	jobID jobspb.JobID,
	jobStartTime time.Time,
	descriptorIDs []descpb.ID,
	rollbackCause error,
) error {
	if knobs != nil && knobs.RunBeforeMakingPostCommitPlan != nil {
		if err := knobs.RunBeforeMakingPostCommitPlan(rollbackCause != nil); err != nil {
			return err
		}
	}
	p, err := makePostCommitPlan(ctx, deps, jobID, jobStartTime, descriptorIDs, rollbackCause)
	if err != nil {
		if knobs != nil && knobs.OnPostCommitPlanError != nil {
			return knobs.OnPostCommitPlanError(err)
		}
		return err
	}
	deps.SetExplain(p.ExplainCompact())
	for i := range p.Stages {
		// Execute each stage in its own transaction.
		if err := deps.WithTxnInJob(ctx, func(ctx context.Context, td scexec.Dependencies, el EventLogger) error {
			if err := td.TransactionalJobRegistry().CheckPausepoint(pausepointName(p, i)); err != nil {
				return err
			}
			if err := executeStage(ctx, knobs, td, p, i, p.Stages[i]); err != nil {
				return err
			}
			// In the last stage, log that the schema change has finished.
			if i+1 == len(p.Stages) {
				var template eventpb.EventWithCommonSchemaChangePayload
				if p.CurrentState.InRollback {
					template = &eventpb.FinishSchemaChangeRollback{
						LatencyNanos: timeutil.Since(jobStartTime).Nanoseconds(),
					}
				} else {
					template = &eventpb.FinishSchemaChange{
						LatencyNanos: timeutil.Since(jobStartTime).Nanoseconds(),
					}
				}
				if err := logSchemaChangeEvents(ctx, el, p.CurrentState, template); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			if knobs != nil && knobs.OnPostCommitError != nil {
				return knobs.OnPostCommitError(p, i, err)
			}
			return err
		}
	}
	return nil
}

// pausepointName construct a name for the job execution phase pausepoint.
func pausepointName(p scplan.Plan, i int) string {
	return fmt.Sprintf(
		"schemachanger.%s.%s.%d",
		p.CurrentState.Authorization.UserName, p.CurrentState.Authorization.AppName, i,
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
		0, /* level */
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
		if !errors.HasType(err, (*kvpb.TransactionRetryWithProtoRefreshError)(nil)) &&
			!errors.Is(err, context.Canceled) &&
			!scerrors.HasSchemaChangerUserError(err) &&
			!pgerror.HasCandidateCode(err) {
			err = p.DecorateErrorWithPlanDetails(err)
		}
		// Certain errors are aimed to be user consumable and should never be
		// wrapped.
		if userError := scerrors.UnwrapSchemaChangerUserError(err); userError != nil {
			return userError
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

func makePostCommitPlan(
	ctx context.Context,
	deps JobRunDependencies,
	jobID jobspb.JobID,
	jobStartTime time.Time,
	descriptorIDs []descpb.ID,
	rollbackCause error,
) (scplan.Plan, error) {
	var state scpb.CurrentState
	do := func(ctx context.Context, txnDeps scexec.Dependencies, eventLogger EventLogger) error {
		// Read the descriptors which each contain a part of the declarative
		// schema change state.
		descriptors, err := txnDeps.Catalog().MustReadImmutableDescriptors(ctx, descriptorIDs...)
		if err != nil {
			// TODO(ajwerner): It seems possible that a descriptor could be deleted
			// and the schema change is in a happy place. Ideally we'd enforce that
			// descriptors may only be deleted on the very last step of the schema
			// change.
			return errors.Wrapf(err,
				"failed to read descriptors %v for the declarative schema change state",
				descriptorIDs)
		}
		// Rebuild the state from its constituent parts.
		activeVersion := deps.ClusterSettings().Version.ActiveVersion(ctx)
		state, err = makeState(ctx, jobID, descriptorIDs, descriptors, activeVersion)
		if err != nil {
			return err
		}
		if rollbackCause == nil && state.InRollback {
			// If we do not mark the error as permanent, but we've configured the job to
			// be non-cancelable, we'll never make it to the reverting state.
			return jobs.MarkAsPermanentJobError(errors.Errorf(
				"job in running state but schema change in rollback, " +
					"returning an error to restart in the reverting state"))
		}
		if rollbackCause != nil && !state.InRollback {
			// Revert the schema change and write about it in the event log.
			state.Rollback()
			return logSchemaChangeEvents(ctx, eventLogger, state, &eventpb.ReverseSchemaChange{
				Error:        redact.Sprintf("%+v", rollbackCause),
				SQLSTATE:     pgerror.GetPGCode(rollbackCause).String(),
				LatencyNanos: timeutil.Since(jobStartTime).Nanoseconds(),
			})
		}
		return nil
	}
	if err := deps.WithTxnInJob(ctx, do); err != nil {
		return scplan.Plan{}, err
	}
	// Plan the schema change.
	return scplan.MakePlan(ctx, state, scplan.Params{
		ActiveVersion:              deps.ClusterSettings().Version.ActiveVersion(ctx),
		ExecutionPhase:             scop.PostCommitPhase,
		SchemaChangerJobIDSupplier: func() jobspb.JobID { return jobID },
		SkipPlannerSanityChecks:    true,
		InRollback:                 state.InRollback,
		MemAcc:                     mon.NewStandaloneUnlimitedAccount(),
	})
}

func logSchemaChangeEvents(
	ctx context.Context,
	eventLogger EventLogger,
	state scpb.CurrentState,
	template eventpb.EventWithCommonSchemaChangePayload,
) error {
	var ids catalog.DescriptorIDSet
	for _, t := range state.TargetState.Targets {
		if t.Metadata.SourceElementID > 1 ||
			!t.IsLinkedToSchemaChange() { // Ignore empty metadata
			// Ignore targets which are the product of CASCADEs.
			continue
		}
		ids.Add(screl.GetDescID(t.Element()))
	}
	for _, id := range ids.Ordered() {
		template.CommonSchemaChangeDetails().DescriptorID = uint32(id)
		if err := eventLogger.LogEventForSchemaChange(ctx, template); err != nil {
			return err
		}
	}
	return nil
}

func makeState(
	ctx context.Context,
	jobID jobspb.JobID,
	descriptorIDs []descpb.ID,
	descriptors []catalog.Descriptor,
	version clusterversion.ClusterVersion,
) (state scpb.CurrentState, err error) {
	defer scerrors.StartEventf(
		ctx,
		0, /* level */
		"rebuilding declarative schema change state from descriptors %v",
		redact.Safe(descriptorIDs),
	).HandlePanicAndLogError(ctx, &err)
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
	addDescriptorState := func(desc catalog.Descriptor) (err error) {
		defer func() {
			err = errors.Wrapf(err, "descriptor %q (%d)", desc.GetName(), desc.GetID())
		}()
		cs := desc.GetDeclarativeSchemaChangerState()
		// Copy and apply migration for the state, this should normally do nothing,
		// but TXN_DROPPED is special and should be cleaned up in memory before
		// executing on a newer node.
		cs = protoutil.Clone(cs).(*scpb.DescriptorState)
		scpb.MigrateDescriptorState(version, desc.GetParentID(), cs)
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
	for _, desc := range descriptors {
		if err := addDescriptorState(desc); err != nil {
			return scpb.CurrentState{}, err
		}
	}
	return scpb.MakeCurrentStateFromDescriptors(descriptorStates)
}
