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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// NewJobRunDependencies returns an scrun.JobRunDependencies implementation built from the
// given arguments.
func NewJobRunDependencies(
	collectionFactory *descs.CollectionFactory,
	db *kv.DB,
	internalExecutor sqlutil.InternalExecutor,
	indexBackfiller scexec.IndexBackfiller,
	eventLoggerFactory EventLoggerFactory,
	partitioner scmutationexec.Partitioner,
	jobRegistry *jobs.Registry,
	job *jobs.Job,
	codec keys.SQLCodec,
	settings *cluster.Settings,
	indexValidator scexec.IndexValidator,
	testingKnobs *scrun.TestingKnobs,
	statements []string,
) scrun.JobRunDependencies {
	return &jobExecutionDeps{
		collectionFactory:  collectionFactory,
		db:                 db,
		internalExecutor:   internalExecutor,
		indexBackfiller:    indexBackfiller,
		eventLoggerFactory: eventLoggerFactory,
		partitioner:        partitioner,
		jobRegistry:        jobRegistry,
		job:                job,
		codec:              codec,
		settings:           settings,
		testingKnobs:       testingKnobs,
		statements:         statements,
		indexValidator:     indexValidator,
	}
}

type jobExecutionDeps struct {
	collectionFactory  *descs.CollectionFactory
	db                 *kv.DB
	internalExecutor   sqlutil.InternalExecutor
	indexBackfiller    scexec.IndexBackfiller
	eventLoggerFactory func(txn *kv.Txn) scexec.EventLogger
	partitioner        scmutationexec.Partitioner
	jobRegistry        *jobs.Registry
	job                *jobs.Job

	indexValidator scexec.IndexValidator

	codec        keys.SQLCodec
	settings     *cluster.Settings
	testingKnobs *scrun.TestingKnobs
	statements   []string
}

var _ scrun.JobRunDependencies = (*jobExecutionDeps)(nil)

// ClusterSettings implements the scrun.JobRunDependencies interface.
func (d *jobExecutionDeps) ClusterSettings() *cluster.Settings {
	return d.settings
}

// WithTxnInJob implements the scrun.JobRunDependencies interface.
func (d *jobExecutionDeps) WithTxnInJob(ctx context.Context, fn scrun.JobTxnFunc) error {
	err := d.collectionFactory.Txn(ctx, d.internalExecutor, d.db, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
		return fn(ctx, &execDeps{
			txnDeps: txnDeps{
				txn:             txn,
				codec:           d.codec,
				descsCollection: descriptors,
				jobRegistry:     d.jobRegistry,
				indexValidator:  d.indexValidator,
				eventLogger:     d.eventLoggerFactory(txn),
			},
			indexBackfiller: d.indexBackfiller,
			statements:      d.statements,
			partitioner:     d.partitioner,
			user:            d.job.Payload().UsernameProto.Decode(),
		})
	})
	if err != nil {
		return err
	}
	d.jobRegistry.NotifyToAdoptJobs(ctx)
	return nil
}
