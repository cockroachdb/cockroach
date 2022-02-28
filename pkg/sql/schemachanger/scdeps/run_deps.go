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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// NewJobRunDependencies returns an scrun.JobRunDependencies implementation built from the
// given arguments.
func NewJobRunDependencies(
	collectionFactory *descs.CollectionFactory,
	db *kv.DB,
	internalExecutor sqlutil.InternalExecutor,
	backfiller scexec.Backfiller,
	rangeCounter RangeCounter,
	eventLoggerFactory EventLoggerFactory,
	jobRegistry *jobs.Registry,
	job *jobs.Job,
	codec keys.SQLCodec,
	settings *cluster.Settings,
	indexValidator scexec.IndexValidator,
	commentUpdaterFactory scexec.DescriptorMetadataUpdaterFactory,
	testingKnobs *scrun.TestingKnobs,
	statements []string,
	sessionData *sessiondata.SessionData,
	kvTrace bool,
) scrun.JobRunDependencies {
	return &jobExecutionDeps{
		collectionFactory:     collectionFactory,
		db:                    db,
		internalExecutor:      internalExecutor,
		backfiller:            backfiller,
		rangeCounter:          rangeCounter,
		eventLoggerFactory:    eventLoggerFactory,
		jobRegistry:           jobRegistry,
		job:                   job,
		codec:                 codec,
		settings:              settings,
		testingKnobs:          testingKnobs,
		statements:            statements,
		indexValidator:        indexValidator,
		commentUpdaterFactory: commentUpdaterFactory,
		sessionData:           sessionData,
		kvTrace:               kvTrace,
	}
}

type jobExecutionDeps struct {
	collectionFactory     *descs.CollectionFactory
	db                    *kv.DB
	internalExecutor      sqlutil.InternalExecutor
	eventLoggerFactory    func(txn *kv.Txn) scexec.EventLogger
	backfiller            scexec.Backfiller
	commentUpdaterFactory scexec.DescriptorMetadataUpdaterFactory
	rangeCounter          RangeCounter
	jobRegistry           *jobs.Registry
	job                   *jobs.Job
	kvTrace               bool

	indexValidator scexec.IndexValidator

	codec        keys.SQLCodec
	settings     *cluster.Settings
	testingKnobs *scrun.TestingKnobs
	statements   []string
	sessionData  *sessiondata.SessionData
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
		pl := d.job.Payload()
		return fn(ctx, &execDeps{
			txnDeps: txnDeps{
				txn:                txn,
				codec:              d.codec,
				descsCollection:    descriptors,
				jobRegistry:        d.jobRegistry,
				indexValidator:     d.indexValidator,
				eventLogger:        d.eventLoggerFactory(txn),
				schemaChangerJobID: d.job.ID(),
				kvTrace:            d.kvTrace,
			},
			backfiller: d.backfiller,
			backfillTracker: newBackfillTracker(d.codec,
				newBackfillTrackerConfig(ctx, d.codec, d.db, d.rangeCounter, d.job),
				convertFromJobBackfillProgress(
					d.codec, pl.GetNewSchemaChange().BackfillProgress,
				),
			),
			periodicProgressFlusher: newPeriodicProgressFlusherForIndexBackfill(d.settings),
			statements:              d.statements,
			user:                    pl.UsernameProto.Decode(),
			clock:                   NewConstantClock(timeutil.FromUnixMicros(pl.StartedMicros)),
			commentUpdaterFactory:   d.commentUpdaterFactory,
			sessionData:             d.sessionData,
		})
	})
	if err != nil {
		return err
	}
	// TODO(ajwerner): Rework the job registry dependency to capture the set of
	// jobs which were created to more efficiently notify and wait for jobs here.
	d.jobRegistry.NotifyToAdoptJobs()
	return nil
}
