// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scdeps

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/backfiller"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// NewJobRunDependencies returns an scrun.JobRunDependencies implementation built from the
// given arguments.
func NewJobRunDependencies(
	collectionFactory *descs.CollectionFactory,
	db descs.DB,
	backfiller scexec.Backfiller,
	spanSplitter scexec.IndexSpanSplitter,
	merger scexec.Merger,
	rangeCounter backfiller.RangeCounter,
	eventLoggerFactory func(isql.Txn) scrun.EventLogger,
	jobRegistry *jobs.Registry,
	job *jobs.Job,
	codec keys.SQLCodec,
	settings *cluster.Settings,
	indexValidator scexec.Validator,
	metadataUpdaterFactory MetadataUpdaterFactory,
	statsRefresher scexec.StatsRefresher,
	testingKnobs *scexec.TestingKnobs,
	statements []string,
	sessionData *sessiondata.SessionData,
	kvTrace bool,
) scrun.JobRunDependencies {
	return &jobExecutionDeps{
		collectionFactory:     collectionFactory,
		db:                    db,
		backfiller:            backfiller,
		spanSplitter:          spanSplitter,
		merger:                merger,
		rangeCounter:          rangeCounter,
		eventLoggerFactory:    eventLoggerFactory,
		jobRegistry:           jobRegistry,
		job:                   job,
		codec:                 codec,
		settings:              settings,
		testingKnobs:          testingKnobs,
		statements:            statements,
		indexValidator:        indexValidator,
		commentUpdaterFactory: metadataUpdaterFactory,
		sessionData:           sessionData,
		kvTrace:               kvTrace,
		statsRefresher:        statsRefresher,
	}
}

type jobExecutionDeps struct {
	collectionFactory     *descs.CollectionFactory
	db                    descs.DB
	statsRefresher        scexec.StatsRefresher
	backfiller            scexec.Backfiller
	spanSplitter          scexec.IndexSpanSplitter
	merger                scexec.Merger
	commentUpdaterFactory MetadataUpdaterFactory
	rangeCounter          backfiller.RangeCounter
	eventLoggerFactory    func(isql.Txn) scrun.EventLogger
	jobRegistry           *jobs.Registry
	job                   *jobs.Job
	kvTrace               bool

	indexValidator scexec.Validator

	codec        keys.SQLCodec
	settings     *cluster.Settings
	testingKnobs *scexec.TestingKnobs
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
	var createdJobs []jobspb.JobID
	var tableStatsToRefresh []descpb.ID
	err := d.db.DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) error {
		pl := d.job.Payload()
		ed := &execDeps{
			txnDeps: txnDeps{
				txn:                txn,
				codec:              d.codec,
				descsCollection:    txn.Descriptors(),
				jobRegistry:        d.jobRegistry,
				validator:          d.indexValidator,
				statsRefresher:     d.statsRefresher,
				schemaChangerJobID: d.job.ID(),
				schemaChangerJob:   d.job,
				kvTrace:            d.kvTrace,
				settings:           d.settings,
			},
			backfiller:   d.backfiller,
			merger:       d.merger,
			spanSplitter: d.spanSplitter,
			backfillerTracker: backfiller.NewTracker(
				d.codec,
				d.rangeCounter,
				d.job,
				pl.GetNewSchemaChange().BackfillProgress,
				pl.GetNewSchemaChange().MergeProgress,
			),
			periodicProgressFlusher: backfiller.NewPeriodicProgressFlusherForIndexBackfill(d.settings),
			statements:              d.statements,
			user:                    pl.UsernameProto.Decode(),
			clock:                   NewConstantClock(timeutil.FromUnixMicros(pl.StartedMicros)),
			metadataUpdater:         d.commentUpdaterFactory(ctx, txn.Descriptors(), txn),
			sessionData:             d.sessionData,
			testingKnobs:            d.testingKnobs,
		}
		if err := fn(ctx, ed, d.eventLoggerFactory(txn)); err != nil {
			return err
		}
		createdJobs = ed.CreatedJobs()
		tableStatsToRefresh = ed.tableStatsToRefresh
		return nil
	})
	if err != nil {
		return err
	}
	if len(createdJobs) > 0 {
		d.jobRegistry.NotifyToResume(ctx, createdJobs...)
	}
	if len(tableStatsToRefresh) > 0 {
		err := d.db.DescsTxn(ctx, func(
			ctx context.Context, txn descs.Txn,
		) error {
			for _, id := range tableStatsToRefresh {
				tbl, err := txn.Descriptors().ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, id)
				if err != nil {
					return err
				}
				d.statsRefresher.NotifyMutation(tbl, math.MaxInt32)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}
