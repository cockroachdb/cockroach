// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scdeps

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// NewJobCreationDependencies returns an
// scexec.SchemaChangeJobCreationDependencies implementation built from the
// given arguments.
func NewJobCreationDependencies(
	execDeps scexec.Dependencies, user security.SQLUsername,
) scrun.SchemaChangeJobCreationDependencies {
	return &jobCreationDeps{
		execDeps: execDeps,
		user:     user,
	}
}

type jobCreationDeps struct {
	execDeps scexec.Dependencies
	user     security.SQLUsername
}

var _ scrun.SchemaChangeJobCreationDependencies = (*jobCreationDeps)(nil)

// Catalog implements the scrun.SchemaChangeJobCreationDependencies interface.
func (d *jobCreationDeps) Catalog() scexec.Catalog {
	return d.execDeps.Catalog()
}

// TransactionalJobCreator implements the scrun.SchemaChangeJobCreationDependencies interface.
func (d *jobCreationDeps) TransactionalJobCreator() scexec.TransactionalJobCreator {
	return d.execDeps.TransactionalJobCreator()
}

// User implements the scrun.SchemaChangeJobCreationDependencies interface.
func (d *jobCreationDeps) User() security.SQLUsername {
	return d.user
}

// Statements implements the scrun.SchemaChangeJobCreationDependencies interface.
func (d *jobCreationDeps) Statements() []string {
	return d.execDeps.Statements()
}

// NewJobExecutionDependencies returns an
// scexec.SchemaChangeJobExecutionDependencies implementation built from the
// given arguments.
func NewJobExecutionDependencies(
	collectionFactory *descs.CollectionFactory,
	db *kv.DB,
	internalExecutor sqlutil.InternalExecutor,
	indexBackfiller scexec.IndexBackfiller,
	jobRegistry *jobs.Registry,
	job *jobs.Job,
	codec keys.SQLCodec,
	settings *cluster.Settings,
	indexValidator scexec.IndexValidator,
	cclCallbacks scexec.Partitioner,
	testingKnobs *scexec.NewSchemaChangerTestingKnobs,
	statements []string,
) scrun.SchemaChangeJobExecutionDependencies {
	return &jobExecutionDeps{
		collectionFactory: collectionFactory,
		db:                db,
		internalExecutor:  internalExecutor,
		indexBackfiller:   indexBackfiller,
		jobRegistry:       jobRegistry,
		job:               job,
		codec:             codec,
		settings:          settings,
		testingKnobs:      testingKnobs,
		statements:        statements,
		indexValidator:    indexValidator,
		partitioner:       cclCallbacks,
	}
}

type jobExecutionDeps struct {
	collectionFactory *descs.CollectionFactory
	db                *kv.DB
	internalExecutor  sqlutil.InternalExecutor
	indexBackfiller   scexec.IndexBackfiller
	jobRegistry       *jobs.Registry
	job               *jobs.Job

	indexValidator scexec.IndexValidator
	partitioner    scexec.Partitioner

	codec        keys.SQLCodec
	settings     *cluster.Settings
	testingKnobs *scexec.NewSchemaChangerTestingKnobs
	statements   []string
}

var _ scrun.SchemaChangeJobExecutionDependencies = (*jobExecutionDeps)(nil)

// ClusterSettings implements the scrun.SchemaChangeJobExecutionDependencies interface.
func (d *jobExecutionDeps) ClusterSettings() *cluster.Settings {
	return d.settings
}

// WithTxnInJob implements the scrun.SchemaChangeJobExecutionDependencies interface.
func (d *jobExecutionDeps) WithTxnInJob(
	ctx context.Context,
	fn func(ctx context.Context, txndeps scrun.SchemaChangeJobTxnDependencies) error,
) error {
	err := d.collectionFactory.Txn(ctx, d.internalExecutor, d.db, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
		return fn(ctx, &jobExecutionTxnDeps{
			jobExecutionDeps: *d,
			txnDeps: txnDeps{
				txn:             txn,
				codec:           d.codec,
				descsCollection: descriptors,
				jobRegistry:     d.jobRegistry,
				indexValidator:  d.indexValidator,
				partitioner:     d.partitioner,
			},
		})
	})
	if err != nil {
		return err
	}
	d.jobRegistry.NotifyToAdoptJobs(ctx)
	return nil
}

type jobExecutionTxnDeps struct {
	jobExecutionDeps
	txnDeps
}

var _ scrun.SchemaChangeJobTxnDependencies = (*jobExecutionTxnDeps)(nil)

// UpdateSchemaChangeJob implements the scrun.SchemaChangeJobTxnDependencies interface.
func (d *jobExecutionTxnDeps) UpdateSchemaChangeJob(
	ctx context.Context, fn func(md jobs.JobMetadata, ju scrun.JobProgressUpdater) error,
) error {
	return d.job.Update(ctx, d.txn, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		return fn(md, ju)
	})
}

// ExecutorDependencies implements the scrun.SchemaChangeJobTxnDependencies interface.
func (d *jobExecutionTxnDeps) ExecutorDependencies() scexec.Dependencies {
	return &execDeps{
		txnDeps:         d.txnDeps,
		indexBackfiller: d.indexBackfiller,
		testingKnobs:    d.testingKnobs,
		statements:      d.statements,

		phase: scop.PostCommitPhase,
	}
}
