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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
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

// SchemaChangeJobExecutionDependencies contains the dependencies required for
// executing the schema change job, i.e. for the logic in its Resume() method.
type SchemaChangeJobExecutionDependencies interface {
	// WithTxnInJob is a wrapper for opening and committing a transaction around
	// the execution of the callback. After committing the transaction, the job
	// registry should be notified to adopt jobs.
	WithTxnInJob(ctx context.Context, fn func(ctx context.Context, txndeps SchemaChangeJobTxnDependencies) error) error

	// ClusterSettings returns the current cluster settings.
	ClusterSettings() *cluster.Settings
}

// SchemaChangeJobTxnDependencies contains the dependencies required for
// executing a specific transaction in the schema change job execution.
type SchemaChangeJobTxnDependencies interface {

	// UpdateSchemaChangeJob triggers the update of the current schema change job
	// via the supplied callback.
	UpdateSchemaChangeJob(ctx context.Context, fn func(md jobs.JobMetadata, ju JobProgressUpdater) error) error

	// ExecutorDependencies constructs the executor dependencies for use within
	// this transaction.
	ExecutorDependencies() scexec.Dependencies
}

// JobProgressUpdater is for updating the progress of the schema change job.
// The internals of the jobs.JobUpdater are private, but this interface allows
// for test implementations.
type JobProgressUpdater interface {

	// UpdateProgress is implemented by jobs.JobUpdater.
	UpdateProgress(progress *jobspb.Progress)
}
