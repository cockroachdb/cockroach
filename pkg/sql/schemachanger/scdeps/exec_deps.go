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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

// JobRegistry implements the methods the schema changer needs from the
// job registry. Outside of tests this should always be backed by *job.Registry.
type JobRegistry interface {
	MakeJobID() jobspb.JobID
	CreateJobWithTxn(ctx context.Context, record jobs.Record, jobID jobspb.JobID, txn isql.Txn) (*jobs.Job, error)
	UpdateJobWithTxn(
		ctx context.Context, jobID jobspb.JobID, txn isql.Txn, useReadLock bool, updateFunc jobs.UpdateFn,
	) error
	CheckPausepoint(name string) error
}

// NewExecutorDependencies returns an scexec.Dependencies implementation built
// from the given arguments.
func NewExecutorDependencies(
	settings *cluster.Settings,
	codec keys.SQLCodec,
	sessionData *sessiondata.SessionData,
	txn isql.Txn,
	user username.SQLUsername,
	descsCollection *descs.Collection,
	jobRegistry JobRegistry,
	backfiller scexec.Backfiller,
	spanSplitter scexec.IndexSpanSplitter,
	merger scexec.Merger,
	backfillTracker scexec.BackfillerTracker,
	backfillFlusher scexec.PeriodicProgressFlusher,
	validator scexec.Validator,
	clock scmutationexec.Clock,
	metadataUpdater scexec.DescriptorMetadataUpdater,
	statsRefresher scexec.StatsRefresher,
	testingKnobs *scexec.TestingKnobs,
	kvTrace bool,
	schemaChangerJobID jobspb.JobID,
	statements []string,
) scexec.Dependencies {
	return &execDeps{
		txnDeps: txnDeps{
			txn:                txn,
			codec:              codec,
			descsCollection:    descsCollection,
			jobRegistry:        jobRegistry,
			validator:          validator,
			statsRefresher:     statsRefresher,
			schemaChangerJobID: schemaChangerJobID,
			schemaChangerJob:   nil,
			kvTrace:            kvTrace,
			settings:           settings,
		},
		backfiller:              backfiller,
		spanSplitter:            spanSplitter,
		merger:                  merger,
		backfillerTracker:       backfillTracker,
		metadataUpdater:         metadataUpdater,
		periodicProgressFlusher: backfillFlusher,
		statements:              statements,
		user:                    user,
		sessionData:             sessionData,
		clock:                   clock,
		testingKnobs:            testingKnobs,
	}
}

type txnDeps struct {
	txn                 isql.Txn
	codec               keys.SQLCodec
	descsCollection     *descs.Collection
	jobRegistry         JobRegistry
	createdJobs         []jobspb.JobID
	validator           scexec.Validator
	statsRefresher      scexec.StatsRefresher
	tableStatsToRefresh []descpb.ID
	schemaChangerJobID  jobspb.JobID
	schemaChangerJob    *jobs.Job
	batch               *kv.Batch
	kvTrace             bool
	settings            *cluster.Settings
}

func (d *txnDeps) UpdateSchemaChangeJob(
	ctx context.Context, id jobspb.JobID, callback scexec.JobUpdateCallback,
) error {
	const useReadLock = false
	return d.jobRegistry.UpdateJobWithTxn(ctx, id, d.txn, useReadLock, func(
		txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
	) error {
		return callback(md, ju.UpdateProgress, ju.UpdatePayload)
	})
}

var _ scexec.Catalog = (*txnDeps)(nil)

// MustReadImmutableDescriptors implements the scexec.Catalog interface.
func (d *txnDeps) MustReadImmutableDescriptors(
	ctx context.Context, ids ...descpb.ID,
) ([]catalog.Descriptor, error) {
	return d.descsCollection.ByID(d.txn.KV()).WithoutSynthetic().Get().Descs(ctx, ids)
}

// GetFullyQualifiedName implements the scmutationexec.CatalogReader interface
func (d *txnDeps) GetFullyQualifiedName(ctx context.Context, id descpb.ID) (string, error) {
	g := d.descsCollection.ByID(d.txn.KV()).WithoutSynthetic().Get()
	objectDesc, err := g.Desc(ctx, id)
	if err != nil {
		return "", err
	}
	// For objects like tables, views, sequences, and types
	// we can fetch the fully qualified names.
	if objectDesc.DescriptorType() != catalog.Database &&
		objectDesc.DescriptorType() != catalog.Schema {
		databaseDesc, err := g.Database(ctx, objectDesc.GetParentID())
		if err != nil {
			return "", err
		}
		schemaDesc, err := g.Schema(ctx, objectDesc.GetParentSchemaID())
		if err != nil {
			return "", err
		}
		name := tree.MakeTableNameWithSchema(
			tree.Name(databaseDesc.GetName()),
			tree.Name(schemaDesc.GetName()),
			tree.Name(objectDesc.GetName()),
		)
		return name.FQString(), nil
	} else if objectDesc.DescriptorType() == catalog.Database {
		return objectDesc.GetName(), nil
	} else if objectDesc.DescriptorType() == catalog.Schema {
		databaseDesc, err := g.Database(ctx, objectDesc.GetParentID())
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s.%s", databaseDesc.GetName(), objectDesc.GetName()), nil
	}
	return "", errors.Newf("unknown descriptor type : %s\n", objectDesc.DescriptorType())
}

// MustReadMutableDescriptor implements the scexec.Catalog interface.
func (d *txnDeps) MustReadMutableDescriptor(
	ctx context.Context, id descpb.ID,
) (catalog.MutableDescriptor, error) {
	return d.descsCollection.MutableByID(d.txn.KV()).Desc(ctx, id)
}

// CreateOrUpdateDescriptor implements the scexec.Catalog interface.
func (d *txnDeps) CreateOrUpdateDescriptor(
	ctx context.Context, desc catalog.MutableDescriptor,
) error {
	return d.descsCollection.WriteDescToBatch(ctx, d.kvTrace, desc, d.getOrCreateBatch())
}

// DeleteName implements the scexec.Catalog interface.
func (d *txnDeps) DeleteName(ctx context.Context, nameInfo descpb.NameInfo, id descpb.ID) error {
	return d.descsCollection.DeleteNamespaceEntryToBatch(ctx, d.kvTrace, &nameInfo, d.getOrCreateBatch())
}

// DeleteDescriptor implements the scexec.Catalog interface.
func (d *txnDeps) DeleteDescriptor(ctx context.Context, id descpb.ID) error {
	return d.descsCollection.DeleteDescToBatch(ctx, d.kvTrace, id, d.getOrCreateBatch())
}

// DeleteZoneConfig implements the scexec.Catalog interface.
func (d *txnDeps) DeleteZoneConfig(ctx context.Context, id descpb.ID) error {
	return d.descsCollection.DeleteZoneConfigInBatch(ctx, d.kvTrace, d.getOrCreateBatch(), id)
}

// Validate implements the scexec.Catalog interface.
func (d *txnDeps) Validate(ctx context.Context) error {
	return d.descsCollection.ValidateUncommittedDescriptors(ctx, d.txn.KV())
}

// Run implements the scexec.Catalog interface.
func (d *txnDeps) Run(ctx context.Context) error {
	if d.batch == nil {
		return nil
	}
	if err := d.txn.KV().Run(ctx, d.batch); err != nil {
		return errors.Wrap(err, "persisting catalog mutations")
	}
	d.batch = nil
	return nil
}

// Reset implements the scexec.Catalog interface.
func (d *txnDeps) Reset(ctx context.Context) error {
	d.descsCollection.ResetUncommitted(ctx)
	d.batch = nil
	return nil
}

func (d *txnDeps) getOrCreateBatch() *kv.Batch {
	if d.batch == nil {
		d.batch = d.txn.KV().NewBatch()
	}
	return d.batch
}

// UpdateComment implements the scexec.Catalog interface.
func (d *txnDeps) UpdateComment(ctx context.Context, key catalogkeys.CommentKey, cmt string) error {
	return d.descsCollection.WriteCommentToBatch(ctx, d.kvTrace, d.getOrCreateBatch(), key, cmt)
}

// DeleteComment implements the scexec.Catalog interface.
func (d *txnDeps) DeleteComment(ctx context.Context, key catalogkeys.CommentKey) error {
	return d.descsCollection.DeleteCommentInBatch(ctx, d.kvTrace, d.getOrCreateBatch(), key)
}

var _ scexec.TransactionalJobRegistry = (*txnDeps)(nil)

func (d *txnDeps) MakeJobID() jobspb.JobID {
	return d.jobRegistry.MakeJobID()
}

func (d *txnDeps) CheckPausepoint(name string) error {
	return d.jobRegistry.CheckPausepoint(name)
}

func (d *txnDeps) UseLegacyGCJob(ctx context.Context) bool {
	return !d.settings.Version.IsActive(ctx, clusterversion.V22_2UseDelRangeInGCJob)
}

func (d *txnDeps) SchemaChangerJobID() jobspb.JobID {
	if d.schemaChangerJobID == 0 {
		d.schemaChangerJobID = d.jobRegistry.MakeJobID()
	}
	return d.schemaChangerJobID
}

func (d *txnDeps) CurrentJob() *jobs.Job {
	return d.schemaChangerJob
}

// CreateJob implements the scexec.TransactionalJobRegistry interface.
func (d *txnDeps) CreateJob(ctx context.Context, record jobs.Record) error {
	if _, err := d.jobRegistry.CreateJobWithTxn(ctx, record, record.JobID, d.txn); err != nil {
		return err
	}
	d.createdJobs = append(d.createdJobs, record.JobID)
	return nil
}

// CreatedJobs implements the scexec.TransactionalJobRegistry interface.
func (d *txnDeps) CreatedJobs() []jobspb.JobID {
	return d.createdJobs
}

// GetResumeSpans implements the scexec.BackfillerTracker interface.
func (d *txnDeps) GetResumeSpans(
	ctx context.Context, tableID descpb.ID, indexID descpb.IndexID,
) ([]roachpb.Span, error) {
	table, err := d.descsCollection.ByID(d.txn.KV()).WithoutNonPublic().WithoutSynthetic().Get().Table(ctx, tableID)
	if err != nil {
		return nil, err
	}
	return []roachpb.Span{table.IndexSpan(d.codec, indexID)}, nil
}

// SetResumeSpans implements the scexec.BackfillerTracker interface.
func (d *txnDeps) SetResumeSpans(
	ctx context.Context, tableID descpb.ID, indexID descpb.IndexID, total, done []roachpb.Span,
) error {
	panic("implement me")
}

type execDeps struct {
	txnDeps
	clock                   scmutationexec.Clock
	metadataUpdater         scexec.DescriptorMetadataUpdater
	backfiller              scexec.Backfiller
	spanSplitter            scexec.IndexSpanSplitter
	merger                  scexec.Merger
	backfillerTracker       scexec.BackfillerTracker
	periodicProgressFlusher scexec.PeriodicProgressFlusher
	statements              []string
	user                    username.SQLUsername
	sessionData             *sessiondata.SessionData
	testingKnobs            *scexec.TestingKnobs
}

func (d *execDeps) Clock() scmutationexec.Clock {
	return d.clock
}

var _ scexec.Dependencies = (*execDeps)(nil)

// Catalog implements the scexec.Dependencies interface.
func (d *execDeps) Catalog() scexec.Catalog {
	return d
}

// IndexBackfiller implements the scexec.Dependencies interface.
func (d *execDeps) IndexBackfiller() scexec.Backfiller {
	return d.backfiller
}

// IndexMerger implements the scexec.Dependencies interface.
func (d *execDeps) IndexMerger() scexec.Merger {
	return d.merger
}

// BackfillProgressTracker implements the scexec.Dependencies interface.
func (d *execDeps) BackfillProgressTracker() scexec.BackfillerTracker {
	return d.backfillerTracker
}

// PeriodicProgressFlusher implements the scexec.Dependencies interface.
func (d *execDeps) PeriodicProgressFlusher() scexec.PeriodicProgressFlusher {
	return d.periodicProgressFlusher
}

func (d *execDeps) Validator() scexec.Validator {
	return d.validator
}

// IndexSpanSplitter implements the scexec.Dependencies interface.
func (d *execDeps) IndexSpanSplitter() scexec.IndexSpanSplitter {
	return d.spanSplitter
}

// TransactionalJobRegistry implements the scexec.Dependencies interface.
func (d *execDeps) TransactionalJobRegistry() scexec.TransactionalJobRegistry {
	return d
}

// Statements implements the scexec.Dependencies interface.
func (d *execDeps) Statements() []string {
	return d.statements
}

// User implements the scexec.Dependencies interface.
func (d *execDeps) User() username.SQLUsername {
	return d.user
}

// ClusterSettings implements the scexec.Dependencies interface.
func (d *execDeps) ClusterSettings() *cluster.Settings {
	return d.settings
}

// DescriptorMetadataUpdater implements the scexec.Dependencies interface.
func (d *execDeps) DescriptorMetadataUpdater(ctx context.Context) scexec.DescriptorMetadataUpdater {
	return d.metadataUpdater
}

// MetadataUpdaterFactory constructs a new metadata updater with a txn.
type MetadataUpdaterFactory = func(ctx context.Context, descriptors *descs.Collection, txn isql.Txn) scexec.DescriptorMetadataUpdater

// GetTestingKnobs implements scexec.Dependencies
func (d *execDeps) GetTestingKnobs() *scexec.TestingKnobs {
	return d.testingKnobs
}

// AddTableForStatsRefresh adds a table for stats refresh once we are finished
// executing the current transaction.
func (d *execDeps) AddTableForStatsRefresh(id descpb.ID) {
	d.tableStatsToRefresh = append(d.tableStatsToRefresh, id)
}

// StatsRefresher implements scexec.Dependencies
func (d *execDeps) StatsRefresher() scexec.StatsRefreshQueue {
	return d
}

// Telemetry implements the scexec.Dependencies interface.
func (d *execDeps) Telemetry() scexec.Telemetry {
	return d
}

// IncrementSchemaChangeErrorType implemented the scexec.Telemetry interface.
func (d *execDeps) IncrementSchemaChangeErrorType(typ string) {
	telemetry.Inc(sqltelemetry.SchemaChangeErrorCounter(typ))
}

// NewNoOpBackfillerTracker constructs a backfill tracker which does not do
// anything. It will always return progress for a given backfill which
// contains a full set of CompletedSpans corresponding to the source index
// span and an empty MinimumWriteTimestamp. Similarly for merges.
func NewNoOpBackfillerTracker(codec keys.SQLCodec) scexec.BackfillerTracker {
	return noopBackfillProgress{codec: codec}
}

type noopBackfillProgress struct {
	codec keys.SQLCodec
}

func (n noopBackfillProgress) FlushCheckpoint(ctx context.Context) error {
	return nil
}

func (n noopBackfillProgress) FlushFractionCompleted(ctx context.Context) error {
	return nil
}

func (n noopBackfillProgress) GetBackfillProgress(
	ctx context.Context, b scexec.Backfill,
) (scexec.BackfillProgress, error) {
	key := n.codec.IndexPrefix(uint32(b.TableID), uint32(b.SourceIndexID))
	return scexec.BackfillProgress{
		Backfill: b,
		CompletedSpans: []roachpb.Span{
			{Key: key, EndKey: key.PrefixEnd()},
		},
	}, nil
}

func (n noopBackfillProgress) GetMergeProgress(
	ctx context.Context, m scexec.Merge,
) (scexec.MergeProgress, error) {
	p := scexec.MergeProgress{
		Merge:          m,
		CompletedSpans: make([][]roachpb.Span, len(m.SourceIndexIDs)),
	}
	for i, sourceID := range m.SourceIndexIDs {
		prefix := n.codec.IndexPrefix(uint32(m.TableID), uint32(sourceID))
		p.CompletedSpans[i] = []roachpb.Span{{Key: prefix, EndKey: prefix.PrefixEnd()}}
	}
	return p, nil
}

func (n noopBackfillProgress) SetBackfillProgress(
	ctx context.Context, progress scexec.BackfillProgress,
) error {
	return nil
}

func (n noopBackfillProgress) SetMergeProgress(
	ctx context.Context, progress scexec.MergeProgress,
) error {
	return nil
}

type noopPeriodicProgressFlusher struct {
}

// NewNoopPeriodicProgressFlusher constructs a new
// PeriodicProgressFlusher which never does anything.
func NewNoopPeriodicProgressFlusher() scexec.PeriodicProgressFlusher {
	return noopPeriodicProgressFlusher{}
}

func (n noopPeriodicProgressFlusher) StartPeriodicUpdates(
	ctx context.Context, tracker scexec.BackfillerProgressFlusher,
) (stop func() error) {
	return func() error { return nil }
}

type constantClock struct {
	ts time.Time
}

// NewConstantClock constructs a new clock for use in execution.
func NewConstantClock(ts time.Time) scmutationexec.Clock {
	return constantClock{ts: ts}
}

func (c constantClock) ApproximateTime() time.Time {
	return c.ts
}
