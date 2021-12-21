// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scstage

import (
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/errors"
)

// AugmentStagesForJob adds operations to the stages to deal with updating
// the job status and for updating the references from descriptors to jobs.
//
// TODO(ajwerner): Rather than adding this above the opgen layer, it'd be
// better to do it as part of graph generation. We could treat the job as
// and the job references as elements and track their relationships. The
// oddity here is that the job gets both added and removed. In practice, this
// may prove to be a somewhat common pattern in other cases: consider the
// intermediate index needed when adding and dropping columns as part of the
// same transaction.
func AugmentStagesForJob(
	stages []Stage, executionPhase scop.Phase, jobIDGenerator func() jobspb.JobID,
) (augmented []Stage, jobID jobspb.JobID) {
	var (
		toProcess   = stages
		addStage    = func(s Stage) { augmented = append(augmented, s) }
		addCurStage = func(opsToAdd ...scop.Op) {
			s := toProcess[0]
			if len(opsToAdd) > 0 {
				s.Ops = scop.MakeOps(append(
					append([]scop.Op(nil), s.Ops.Slice()...),
					opsToAdd...)...)
			}
			addStage(s)
			toProcess = toProcess[1:]
		}

		descIDs []descpb.ID
		setup   = func() (done bool) {
			switch executionPhase {
			case scop.StatementPhase:
				augmented = toProcess
				return true // done
			case scop.PreCommitPhase:
				// In the pre-commit phase (which should have exactly one stage), create the
				// job and have all of the descriptors updated to point to it.
				//
				// TODO(ajwerner): There are bugs (seemingly related to schema) which prevent
				// us from acting on an invariant that there is at most one stage in the
				// PreCommitPhase.
				var lastToProcess int
				for i := 0; i < len(toProcess) && toProcess[i].Phase == scop.PreCommitPhase; i++ {
					lastToProcess = i
				}
				for i := 0; i < lastToProcess; i++ {
					addCurStage()
				}
				if len(toProcess) == 1 {
					addCurStage()
					return true // done
				}
				var opsToAdd []scop.Op
				jobID, descIDs, opsToAdd = createSchemaChangeJobAndAddDescriptorJobReferenceOps(
					jobIDGenerator, toProcess[0].After, toProcess[0].Revertible,
				)
				addCurStage(opsToAdd...)
				return false // done
			case scop.PostCommitPhase:
				// Initialize the jobID and descriptors IDs to update for the
				// PostCommit stages.
				jobID, descIDs = jobIDGenerator(), descIDsFromState(toProcess[0].Before)
				return false // done

			default:
				panic(errors.AssertionFailedf("unknown phase %v", executionPhase))
			}
		}

		// Process the PostCommit phases stages by either updating the job status or
		// removing the references in the final stage.
		//
		// Three valid cases:
		// (1) This is a terminal mutation state, augment it to remove references.
		// (2) This is a non-terminal mutation stage, augment it to update progress.
		// (3) This is a non-terminal non-mutation phase, add it and add a mutation
		//    stage to update the job.
		processPostCommitStage = func() {
			if toProcess[0].Phase != scop.PostCommitPhase {
				panic(errors.AssertionFailedf(
					"expected to have PostCommitPhase, got %v",
					toProcess[0].Phase,
				))
			}

			// (1) Terminal mutation stage.
			if len(toProcess) == 1 {
				if toProcess[0].Ops.Type() != scop.MutationType {
					panic(errors.AssertionFailedf(
						"expected to have a mutation stage as the terminal stage, got %v",
						toProcess[0].Ops.Type(),
					))
				}
				addCurStage(generateOpsToRemoveJobIDs(descIDs, jobID)...)
				return
			}

			updateOp := generateUpdateJobProgressOp(jobID, toProcess[0].After)

			// (2) Non-terminal mutation stage.
			if toProcess[0].Ops.Type() == scop.MutationType {
				addCurStage(updateOp)
				return
			}

			// (3) Non-terminal non-mutation stage.
			s := toProcess[0]
			addCurStage()
			s.Before = s.After
			s.Ops = scop.MakeOps(updateOp)
			addStage(s)
		}
	)
	if done := setup(); !done {
		for len(toProcess) > 0 {
			processPostCommitStage()
		}
	}
	return decorateStages(augmented), jobID
}

func createSchemaChangeJobAndAddDescriptorJobReferenceOps(
	idGen func() jobspb.JobID, state scpb.State, revertible bool,
) (_ jobspb.JobID, descIDs []descpb.ID, opsToAdd []scop.Op) {
	targets := make([]*scpb.Target, len(state.Nodes))
	states := make([]scpb.Status, len(state.Nodes))
	// TODO(ajwerner): It may be better in the future to have the builder be
	// responsible for determining this set of descriptors. As of the time of
	// writing, the descriptors to be "locked," descriptors that need schema
	// change jobs, and descriptors with schema change mutations all coincide. But
	// there are future schema changes to be implemented in the new schema changer
	// (e.g., RENAME TABLE) for which this may no longer be true.
	for i := range state.Nodes {
		targets[i] = state.Nodes[i].Target
		states[i] = state.Nodes[i].Status
	}
	descIDs = descIDsFromState(state)
	jobID := idGen()
	record := jobs.Record{
		JobID:         jobID,
		Description:   "Schema change job", // TODO(ajwerner): use const
		Statements:    statementStrings(state.Statements),
		Username:      security.MakeSQLUsernameFromPreNormalizedString(state.Authorization.Username),
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
	opsToAdd = append(opsToAdd, &scop.CreateDeclarativeSchemaChangerJob{
		Record: record,
	})
	opsToAdd = append(opsToAdd, generateOpsToAddJobIDs(descIDs, jobID)...)
	return jobID, descIDs, opsToAdd
}

func statementStrings(statements []*scpb.Statement) (strs []string) {
	for _, stmt := range statements {
		strs = append(strs, stmt.Statement)
	}
	return strs
}

func generateUpdateJobProgressOp(id jobspb.JobID, after scpb.State) scop.Op {
	return &scop.UpdateSchemaChangeJobProgress{
		JobID:    id,
		Statuses: after.Statuses(),
	}
}

func generateOpsToRemoveJobIDs(descIDs []descpb.ID, jobID jobspb.JobID) []scop.Op {
	return generateOpsForJobIDs(descIDs, jobID, func(descID descpb.ID, id jobspb.JobID) scop.Op {
		return &scop.RemoveJobReference{DescriptorID: descID, JobID: jobID}
	})
}

func generateOpsToAddJobIDs(descIDs []descpb.ID, jobID jobspb.JobID) []scop.Op {
	return generateOpsForJobIDs(descIDs, jobID, func(descID descpb.ID, id jobspb.JobID) scop.Op {
		return &scop.AddJobReference{DescriptorID: descID, JobID: jobID}
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

// descIDsFromState extracts the set of descriptor IDs from state.
func descIDsFromState(state scpb.State) []descpb.ID {
	descIDSet := catalog.MakeDescriptorIDSet()
	for i := range state.Nodes {
		// Depending on the element type either a single descriptor ID
		// will exist or multiple (i.e. foreign keys).
		if id := screl.GetDescID(state.Nodes[i].Element()); id != descpb.InvalidID {
			descIDSet.Add(id)
		}
	}
	return descIDSet.Ordered()
}
