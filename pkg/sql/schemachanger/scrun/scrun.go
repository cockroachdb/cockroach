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

// RunSchemaChangesInTxn executes in-transaction schema changes for the targeted
// state. These are the immediate changes which take place at DDL statement
// execution time (scop.StatementPhase) or when executing COMMIT
// (scop.PreCommitPhase), rather than the asynchronous changes which are done
// by the schema changer job after the transaction commits.
func RunSchemaChangesInTxn(
	ctx context.Context, deps TxnRunDependencies, state scpb.State,
) (scpb.State, error) {
	if len(state.Nodes) == 0 {
		return scpb.State{}, nil
	}
	sc, err := scplan.MakePlan(state, scplan.Params{ExecutionPhase: deps.Phase()})
	if err != nil {
		return scpb.State{}, scgraphviz.DecorateErrorWithPlanDetails(err, sc)
	}
	after := state
	for i, s := range sc.StagesForCurrentPhase() {
		if err := executeStage(ctx, deps, sc, i); err != nil {
			return scpb.State{}, err
		}
		after = s.After
	}
	if len(after.Nodes) == 0 {
		return scpb.State{}, nil
	}
	return after, nil
}

// CreateSchemaChangeJob builds and enqueues a schema change job for the target
// state at pre-COMMIT time. This also updates the affected descriptors with the
// id of the created job, effectively locking them to prevent any other schema
// changes concurrent to this job's execution.
func CreateSchemaChangeJob(
	ctx context.Context, deps SchemaChangeJobCreationDependencies, state scpb.State,
) (jobspb.JobID, error) {
	if len(state.Nodes) == 0 {
		return jobspb.InvalidJobID, nil
	}

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
	jobID, err := deps.TransactionalJobCreator().CreateJob(ctx, jobs.Record{
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
		NonCancelable: false,
	})
	if err != nil {
		return jobspb.InvalidJobID, err
	}
	// Write the job ID to the affected descriptors.
	if err := scexec.UpdateDescriptorJobIDs(
		ctx,
		deps.Catalog(),
		descIDs,
		jobspb.InvalidJobID,
		jobID,
	); err != nil {
		return jobID, err
	}
	return jobID, nil
}

// RunSchemaChangesInJob contains the business logic for the Resume method of a
// declarative schema change job, with the dependencies abstracted away.
func RunSchemaChangesInJob(
	ctx context.Context,
	deps JobRunDependencies,
	jobID jobspb.JobID,
	jobDescriptorIDs []descpb.ID,
	jobDetails jobspb.NewSchemaChangeDetails,
	jobProgress jobspb.NewSchemaChangeProgress,
	rollback bool,
) error {
	state := makeState(ctx,
		deps.ClusterSettings(),
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
		return deps.WithTxnInJob(ctx, func(ctx context.Context, td JobTxnRunDependencies) error {
			c := td.ExecutorDependencies().Catalog()
			return scexec.UpdateDescriptorJobIDs(ctx, c, jobDescriptorIDs, jobID, jobspb.InvalidJobID)
		})
	}

	for i, stage := range stages {
		isLastStage := i == len(stages)-1
		// Execute each stage in its own transaction.
		if err := deps.WithTxnInJob(ctx, func(ctx context.Context, td JobTxnRunDependencies) error {
			if err := executeStage(ctx, td, sc, i); err != nil {
				return err
			}
			if err := td.UpdateState(ctx, stage.After); err != nil {
				return err
			}
			if isLastStage {
				// Remove the reference to this schema change job from all affected
				// descriptors in the transaction executing the last stage.
				return scexec.UpdateDescriptorJobIDs(
					ctx, td.ExecutorDependencies().Catalog(), jobDescriptorIDs, jobID, jobspb.InvalidJobID,
				)
			}
			return nil
		}); err != nil {
			return err
		}
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

func executeStage(ctx context.Context, deps TxnRunDependencies, p scplan.Plan, stageIdx int) error {
	if knobs := deps.TestingKnobs(); knobs != nil && knobs.BeforeStage != nil {
		if err := knobs.BeforeStage(p, stageIdx); err != nil {
			return err
		}
	}
	stage := p.StagesForCurrentPhase()[stageIdx]
	err := scexec.ExecuteStage(ctx, deps.ExecutorDependencies(), stage.Ops)
	if err != nil {
		err = errors.Wrapf(err, "Error executing %s", stage.String())
		return scgraphviz.DecorateErrorWithPlanDetails(err, p)
	}
	return nil
}
