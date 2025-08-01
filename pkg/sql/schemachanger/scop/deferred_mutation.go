// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scop

import (
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

//go:generate go run ./generate_visitor.go scop DeferredMutation deferred_mutation.go deferred_mutation_visitor_generated.go

type deferredMutationOp struct{ baseOp }

// Make sure baseOp is used for linter.
var _ = deferredMutationOp{baseOp: baseOp{}}

func (deferredMutationOp) Type() Type { return MutationType }

// CreateGCJobForDatabase creates a GC job for a given database.
type CreateGCJobForDatabase struct {
	deferredMutationOp
	DatabaseID descpb.ID
	StatementForDropJob
}

// CreateGCJobForTable creates a GC job for a given table, when necessary.
type CreateGCJobForTable struct {
	deferredMutationOp
	TableID    descpb.ID
	DatabaseID descpb.ID
	StatementForDropJob
}

// CreateGCJobForIndex creates a GC job for a given table index.
type CreateGCJobForIndex struct {
	deferredMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
	StatementForDropJob
}

// UpdateSchemaChangerJob may update the job's cancelable status.
type UpdateSchemaChangerJob struct {
	deferredMutationOp
	IsNonCancelable       bool
	JobID                 jobspb.JobID
	RunningStatus         string
	DescriptorIDsToRemove []descpb.ID
}

// CreateSchemaChangerJob constructs the job for the
// declarative schema changer post-commit phases.
type CreateSchemaChangerJob struct {
	deferredMutationOp
	JobID         jobspb.JobID
	Authorization scpb.Authorization
	Statements    []scpb.Statement
	DescriptorIDs []descpb.ID

	// NonCancelable maps to the job's property, but in the schema changer can
	// be thought of as !Revertible.
	NonCancelable bool
	RunningStatus string
}

// RemoveDatabaseRoleSettings is used to delete a role setting for a database.
type RemoveDatabaseRoleSettings struct {
	deferredMutationOp
	DatabaseID descpb.ID
}

// DeleteSchedule is used to delete a schedule ID from the database.
type DeleteSchedule struct {
	deferredMutationOp
	ScheduleID jobspb.ScheduleID
}

// RefreshStats is used to queue a table for stats refresh.
type RefreshStats struct {
	deferredMutationOp
	TableID descpb.ID
}

// MaybeAddSplitForIndex adds a admin split range temproarily on this index.
type MaybeAddSplitForIndex struct {
	deferredMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}
