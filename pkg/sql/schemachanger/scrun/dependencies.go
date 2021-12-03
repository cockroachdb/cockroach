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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

// SchemaChangeJobCreationDependencies contains the dependencies required for
// creating a schema change job for the declarative schema changer.
type SchemaChangeJobCreationDependencies interface {
	Catalog() scexec.Catalog
	TransactionalJobCreator() scexec.TransactionalJobCreator

	// User returns the user to be associated with the schema change job.
	User() security.SQLUsername

	// Statements returns the statements behind this schema change.
	Statements() []string
}

// TxnRunDependencies are dependencies used to plan and execute a stage of a
// schema change job.
type TxnRunDependencies interface {

	// ExecutorDependencies constructs the executor dependencies for use within
	// this transaction.
	ExecutorDependencies() scexec.Dependencies

	// Phase returns the phase in which operations are to be executed.
	Phase() scop.Phase

	// TestingKnobs returns the testing knobs for the new schema changer.
	TestingKnobs() *TestingKnobs
}

// JobTxnFunc is used to run a transactional stage of a schema change on
// behalf of a job. See JobRunDependencies.WithTxnInJob().
type JobTxnFunc = func(ctx context.Context, txnDeps JobTxnRunDependencies) error

// JobRunDependencies contains the dependencies required for
// executing the schema change job, i.e. for the logic in its Resume() method.
type JobRunDependencies interface {
	// WithTxnInJob is a wrapper for opening and committing a transaction around
	// the execution of the callback. After committing the transaction, the job
	// registry should be notified to adopt jobs.
	WithTxnInJob(ctx context.Context, fn JobTxnFunc) error

	// ClusterSettings returns the current cluster settings.
	ClusterSettings() *cluster.Settings
}

// JobTxnRunDependencies contains the dependencies required for
// executing a specific transaction in the schema change job execution.
type JobTxnRunDependencies interface {
	TxnRunDependencies

	// UpdateState triggers the update of the current schema change job state.
	UpdateState(ctx context.Context, state scpb.State) error
}

// JobProgressUpdater is for updating the progress of the schema change job.
// The internals of the jobs.JobUpdater are private, but this interface allows
// for test implementations.
type JobProgressUpdater interface {

	// UpdateProgress is implemented by jobs.JobUpdater.
	UpdateProgress(progress *jobspb.Progress)
}
