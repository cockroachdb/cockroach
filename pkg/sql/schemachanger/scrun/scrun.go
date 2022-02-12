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
	"sort"

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
	state, err := makeState(ctx, settings, deps, descriptorIDs, rollback)
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
	stage scplan.Stage,
) error {
	if knobs != nil && knobs.BeforeStage != nil {
		if err := knobs.BeforeStage(p, stageIdx); err != nil {
			return err
		}
	}

	log.Infof(ctx, "executing stage %d/%d in phase %v, %d ops of type %s", stage.Ordinal, stage.StagesInPhase, stage.Phase, len(stage.Ops()), stage.Ops()[0].Type())
	if err := scexec.ExecuteStage(ctx, deps, stage.Ops()); err != nil {
		return errors.Wrapf(p.DecorateErrorWithPlanDetails(err), "error executing %s", stage.String())
	}
	return nil
}

type stateAndRanks struct {
	*scpb.CurrentState
	ranks []uint32
}

var _ sort.Interface = (*stateAndRanks)(nil)

func (s *stateAndRanks) Len() int           { return len(s.Targets) }
func (s *stateAndRanks) Less(i, j int) bool { return s.ranks[i] < s.ranks[j] }
func (s *stateAndRanks) Swap(i, j int) {
	s.ranks[i], s.ranks[j] = s.ranks[j], s.ranks[i]
	s.Targets[i], s.Targets[j] = s.Targets[j], s.Targets[i]
	s.Current[i], s.Current[j] = s.Current[j], s.Current[i]
}

type stmtsAndRanks struct {
	stmts []scpb.Statement
	ranks []uint32
}

func (s *stmtsAndRanks) Len() int           { return len(s.stmts) }
func (s *stmtsAndRanks) Less(i, j int) bool { return s.ranks[i] < s.ranks[j] }
func (s stmtsAndRanks) Swap(i, j int) {
	s.ranks[i], s.ranks[j] = s.ranks[j], s.ranks[i]
	s.stmts[i], s.stmts[j] = s.stmts[j], s.stmts[i]
}

var _ sort.Interface = (*stmtsAndRanks)(nil)

func makeState(
	ctx context.Context,
	sv *cluster.Settings,
	deps JobRunDependencies,
	descriptorIDs []descpb.ID,
	rollback bool,
) (scpb.CurrentState, error) {
	var s scpb.CurrentState
	var targetRanks []uint32
	var stmts map[uint32]scpb.Statement
	if err := deps.WithTxnInJob(ctx, func(ctx context.Context, txnDeps scexec.Dependencies) error {

		// Reset for restarts.
		s = scpb.CurrentState{}
		targetRanks = nil
		stmts = make(map[uint32]scpb.Statement)

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
					"descriptor %d does not contain schema changer state", desc.GetID(),
				)
			}
			s.Current = append(s.Current, cs.CurrentStatuses...)
			s.Targets = append(s.Targets, cs.Targets...)
			targetRanks = append(targetRanks, cs.TargetRanks...)
			for _, stmt := range cs.RelevantStatements {
				if existing, ok := stmts[stmt.StatementRank]; ok {
					if existing.Statement != stmt.Statement.Statement {
						return errors.AssertionFailedf(
							"job %d: statement %q does not match %q for rank %d",
							cs.JobID,
							existing.Statement,
							stmt.Statement,
							stmt.StatementRank,
						)
					}
				}
				stmts[stmt.StatementRank] = stmt.Statement
			}
			s.Authorization = cs.Authorization
		}
		return nil
	}); err != nil {
		return scpb.CurrentState{}, err
	}
	sort.Sort(&stateAndRanks{
		CurrentState: &s,
		ranks:        targetRanks,
	})
	var sr stmtsAndRanks
	for rank, stmt := range stmts {
		sr.stmts = append(sr.stmts, stmt)
		sr.ranks = append(sr.ranks, rank)
	}
	sort.Sort(&sr)
	s.Statements = sr.stmts
	if rollback {
		for i := range s.Targets {
			t := &s.Targets[i]
			switch t.TargetStatus {
			case scpb.Status_PUBLIC:
				t.TargetStatus = scpb.Status_ABSENT
			case scpb.Status_ABSENT:
				t.TargetStatus = scpb.Status_PUBLIC
			}
		}
	}
	return s, nil
}
