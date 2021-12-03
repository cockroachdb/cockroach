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

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraphviz"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/errors"
)

// RunStatementPhase executes in-transaction schema changes for the targeted
// state. These are the immediate changes which take place at DDL statement
// execution time (scop.StatementPhase).
func RunStatementPhase(
	ctx context.Context, knobs *TestingKnobs, deps scexec.Dependencies, state scpb.State,
) (scpb.State, error) {
	phase := scop.StatementPhase
	return runTransactionPhase(ctx, knobs, deps, state, phase, nil)
}

// RunPreCommitPhase executes in-transaction schema changes for the targeted
// state. These are run when executing COMMIT (scop.PreCommitPhase), rather
// than the asynchronous changes which are done by the schema changer job
// after the transaction commits.
func RunPreCommitPhase(
	ctx context.Context, knobs *TestingKnobs, deps scexec.Dependencies, state scpb.State,
) (scpb.State, jobspb.JobID, error) {
	var jobID jobspb.JobID
	after, err := runTransactionPhase(ctx, knobs, deps, state, scop.PreCommitPhase, func(
		stage *scplan.Stage,
	) error {
		var opsToAdd []scop.Op
		jobID, opsToAdd = createSchemaChangeJobAndAddDescriptorJobReferenceOps(deps, stage.After, stage.Revertible)
		newOps, err := scop.ExtendOps(stage.Ops, opsToAdd...)
		if err != nil {
			return err
		}
		stage.Ops = newOps
		return nil
	})
	if err != nil {
		return scpb.State{}, 0, err
	}
	return after, jobID, nil
}

func runTransactionPhase(
	ctx context.Context,
	knobs *TestingKnobs,
	deps scexec.Dependencies,
	state scpb.State,
	phase scop.Phase,
	augmentLastStage func(stage *scplan.Stage) error,
) (scpb.State, error) {
	if len(state.Nodes) == 0 {
		return scpb.State{}, nil
	}
	sc, err := scplan.MakePlan(state, scplan.Params{ExecutionPhase: phase})
	if err != nil {
		return scpb.State{}, scgraphviz.DecorateErrorWithPlanDetails(err, sc)
	}
	after := state
	stages := sc.StagesForCurrentPhase()
	for i := range stages {
		if i+1 == len(stages) && augmentLastStage != nil {
			if err := augmentLastStage(&stages[i]); err != nil {
				return scpb.State{}, errors.Wrap(err, "augmenting last stage")
			}
		}
		if err := executeStage(ctx, knobs, deps, sc, i, stages[i]); err != nil {
			return scpb.State{}, err
		}
		after = stages[i].After
	}
	if len(after.Nodes) == 0 {
		return scpb.State{}, nil
	}
	return after, nil
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
	sc, err := scplan.MakePlan(state, scplan.Params{ExecutionPhase: scop.PostCommitPhase})
	if err != nil {
		return scgraphviz.DecorateErrorWithPlanDetails(err, sc)
	}

	stages := sc.StagesForCurrentPhase()
	if len(stages) == 0 {
		// In the case where no stage exists, and therefore there's nothing to
		// execute, we still need to open a transaction to remove all references to
		// this schema change job from the descriptors.
		return deps.WithTxnInJob(ctx, func(ctx context.Context, td scexec.Dependencies) error {
			return scexec.ExecuteStage(ctx, td,
				scop.MakeOps(generateOpsToRemoveJobIDs(jobDescriptorIDs, jobID)...))
		})
	}

	for i := range stages {
		var opsToAdd []scop.Op
		if isLastStage := i+1 == len(stages); isLastStage {
			opsToAdd = generateOpsToRemoveJobIDs(jobDescriptorIDs, jobID)
		}
		opsToAdd = append(opsToAdd, generateUpdateJobProgressOp(jobID, stages[i].After))

		isMutationStage := stages[i].Ops.Type() == scop.MutationType
		if isMutationStage {
			if err := extendStageOps(&stages[i], opsToAdd); err != nil {
				return err
			}
		}
		// Execute each stage in its own transaction.
		if err := deps.WithTxnInJob(ctx, func(ctx context.Context, td scexec.Dependencies) error {
			return executeStage(ctx, knobs, td, sc, i, stages[i])
		}); err != nil {
			return err
		}
		if !isMutationStage {
			if err := deps.WithTxnInJob(ctx, func(ctx context.Context, td scexec.Dependencies) error {
				return scexec.ExecuteStage(ctx, td, scop.MakeOps(opsToAdd...))
			}); err != nil {
				return err
			}
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
	stage scplan.Stage,
) error {
	if knobs != nil && knobs.BeforeStage != nil {
		if err := knobs.BeforeStage(p, stageIdx); err != nil {
			return err
		}
	}
	err := scexec.ExecuteStage(ctx, deps, stage.Ops)
	if err != nil {
		err = errors.Wrapf(err, "error executing %s", stage.String())
		return scgraphviz.DecorateErrorWithPlanDetails(err, p)
	}
	return nil
}

func createSchemaChangeJobAndAddDescriptorJobReferenceOps(
	deps scexec.Dependencies, state scpb.State, revertible bool,
) (_ jobspb.JobID, opsToAdd []scop.Op) {
	targets := make([]*scpb.Target, len(state.Nodes))
	states := make([]scpb.Status, len(state.Nodes))
	// TODO(ajwerner): It may be better in the future to have the builder be
	// responsible for determining this set of descriptors. As of the time of
	// writing, the descriptors to be "locked," descriptors that need schema
	// change jobs, and descriptors with schema change mutations all coincide. But
	// there are future schema changes to be implemented in the new schema changer
	// (e.g., RENAME TABLE) for which this may no longer be true.
	descIDSet := catalog.MakeDescriptorIDSet()
	for i := range state.Nodes {
		targets[i] = state.Nodes[i].Target
		states[i] = state.Nodes[i].Status
		// Depending on the element type either a single descriptor ID
		// will exist or multiple (i.e. foreign keys).
		if id := screl.GetDescID(state.Nodes[i].Element()); id != descpb.InvalidID {
			descIDSet.Add(id)
		}
	}
	descIDs := descIDSet.Ordered()
	jobID := deps.TransactionalJobCreator().MakeJobID()
	record := jobs.Record{
		JobID:         jobID,
		Description:   "Schema change job", // TODO(ajwerner): use const
		Statements:    deps.Statements(),
		Username:      deps.User(),
		DescriptorIDs: descIDs,
		Details:       jobspb.NewSchemaChangeDetails{Targets: targets},
		Progress: jobspb.NewSchemaChangeProgress{
			States:        states,
			Authorization: &state.Authorization,
			Statements:    state.Statements,
		},
		RunningStatus: "",
		NonCancelable: !revertible,
	}
	opsToAdd = append(opsToAdd, scop.CreateDeclarativeSchemaChangerJob{
		Record: record,
	})
	opsToAdd = append(opsToAdd, generateOpsToAddJobIDs(descIDs, jobID)...)
	return jobID, opsToAdd
}

func extendStageOps(stage *scplan.Stage, opsToAdd []scop.Op) error {
	newOps, err := scop.ExtendOps(stage.Ops, opsToAdd...)
	if err != nil {
		return err
	}
	stage.Ops = newOps
	return nil
}

func generateUpdateJobProgressOp(id jobspb.JobID, after scpb.State) scop.Op {
	return scop.UpdateSchemaChangeJobProgress{
		JobID:    id,
		Statuses: after.Statuses(),
	}
}

func generateOpsToRemoveJobIDs(descIDs []descpb.ID, jobID jobspb.JobID) []scop.Op {
	return generateOpsForJobIDs(descIDs, jobID, func(descID descpb.ID, id jobspb.JobID) scop.Op {
		return scop.RemoveJobReference{DescriptorID: descID, JobID: jobID}
	})
}

func generateOpsToAddJobIDs(descIDs []descpb.ID, jobID jobspb.JobID) []scop.Op {
	return generateOpsForJobIDs(descIDs, jobID, func(descID descpb.ID, id jobspb.JobID) scop.Op {
		return scop.AddJobReference{DescriptorID: descID, JobID: jobID}
	})
}

func generateOpsForJobIDs(
	descIDs []descpb.ID, jobID jobspb.JobID, f func(descID descpb.ID, id jobspb.JobID) scop.Op,
) []scop.Op {
	ops := make([]scop.Op, len(descIDs))
	for i, descID := range descIDs {
		ops[i] = f(descID, jobID)
	}
	return ops
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
		Statements:    statements,
		Authorization: *authorization,
	}
	ts.Nodes = make([]*scpb.Node, len(protos))
	for i := range protos {
		ts.Nodes[i] = &scpb.Node{
			Target: protos[i],
			Status: states[i],
		}
		if rollback {
			switch ts.Nodes[i].Direction {
			case scpb.Target_ADD:
				ts.Nodes[i].Direction = scpb.Target_DROP
			case scpb.Target_DROP:
				ts.Nodes[i].Direction = scpb.Target_ADD
			}
		}
	}
	return ts
}
