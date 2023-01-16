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
	CreateJobWithTxn(ctx context.Context, record jobs.Record, jobID jobspb.JobID, txn *kv.Txn) (*jobs.Job, error)
	UpdateJobWithTxn(
		ctx context.Context, jobID jobspb.JobID, txn *kv.Txn, useReadLock bool, updateFunc jobs.UpdateFn,
	) error
	CheckPausepoint(name string) error
}

// NewExecutorDependencies returns an scexec.Dependencies implementation built
// from the given arguments.
func NewExecutorDependencies(
	settings *cluster.Settings,
	codec keys.SQLCodec,
	sessionData *sessiondata.SessionData,
	txn *kv.Txn,
	user username.SQLUsername,
	descsCollection *descs.Collection,
	jobRegistry JobRegistry,
	backfiller scexec.Backfiller,
	merger scexec.Merger,
	backfillTracker scexec.BackfillerTracker,
	backfillFlusher scexec.PeriodicProgressFlusher,
	validator scexec.Validator,
	clock scmutationexec.Clock,
	metadataUpdater scexec.DescriptorMetadataUpdater,
	eventLogger scexec.EventLogger,
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
			eventLogger:        eventLogger,
			statsRefresher:     statsRefresher,
			schemaChangerJobID: schemaChangerJobID,
			schemaChangerJob:   nil,
			kvTrace:            kvTrace,
			settings:           settings,
		},
		backfiller:              backfiller,
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
	txn                 *kv.Txn
	codec               keys.SQLCodec
	descsCollection     *descs.Collection
	jobRegistry         JobRegistry
	createdJobs         []jobspb.JobID
	validator           scexec.Validator
	statsRefresher      scexec.StatsRefresher
	tableStatsToRefresh []descpb.ID
	eventLogger         scexec.EventLogger
	schemaChangerJobID  jobspb.JobID
	schemaChangerJob    *jobs.Job
	kvTrace             bool
	settings            *cluster.Settings
}

func (d *txnDeps) UpdateSchemaChangeJob(
	ctx context.Context, id jobspb.JobID, callback scexec.JobUpdateCallback,
) error {
	const useReadLock = false
	return d.jobRegistry.UpdateJobWithTxn(ctx, id, d.txn, useReadLock, func(
		txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
	) error {
		return callback(md, ju.UpdateProgress, ju.UpdatePayload)
	})
}

var _ scexec.Catalog = (*txnDeps)(nil)

// MustReadImmutableDescriptors implements the scmutationexec.CatalogReader interface.
func (d *txnDeps) MustReadImmutableDescriptors(
	ctx context.Context, ids ...descpb.ID,
) ([]catalog.Descriptor, error) {
	return d.descsCollection.ByID(d.txn).WithoutSynthetic().Get().Descs(ctx, ids)
}

// GetFullyQualifiedName implements the scmutationexec.CatalogReader interface
func (d *txnDeps) GetFullyQualifiedName(ctx context.Context, id descpb.ID) (string, error) {
	g := d.descsCollection.ByID(d.txn).WithoutSynthetic().Get()
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
	return d.descsCollection.MutableByID(d.txn).Desc(ctx, id)
}

// AddSyntheticDescriptor is part of the
// scmutationexec.SyntheticDescriptorStateUpdater interface.
func (d *txnDeps) AddSyntheticDescriptor(desc catalog.Descriptor) {
	d.descsCollection.AddSyntheticDescriptor(desc)
}

// NewCatalogChangeBatcher implements the scexec.Catalog interface.
func (d *txnDeps) NewCatalogChangeBatcher() scexec.CatalogChangeBatcher {
	return &catalogChangeBatcher{
		txnDeps: d,
		batch:   d.txn.NewBatch(),
	}
}

type catalogChangeBatcher struct {
	*txnDeps
	batch *kv.Batch
}

var _ scexec.CatalogChangeBatcher = (*catalogChangeBatcher)(nil)

// CreateOrUpdateDescriptor implements the scexec.CatalogWriter interface.
func (b *catalogChangeBatcher) CreateOrUpdateDescriptor(
	ctx context.Context, desc catalog.MutableDescriptor,
) error {
	return b.descsCollection.WriteDescToBatch(ctx, b.kvTrace, desc, b.batch)
}

// DeleteName implements the scexec.CatalogWriter interface.
func (b *catalogChangeBatcher) DeleteName(
	ctx context.Context, nameInfo descpb.NameInfo, id descpb.ID,
) error {
	return b.descsCollection.DeleteNamespaceEntryToBatch(ctx, b.kvTrace, &nameInfo, b.batch)
}

// DeleteDescriptor implements the scexec.CatalogChangeBatcher interface.
func (b *catalogChangeBatcher) DeleteDescriptor(ctx context.Context, id descpb.ID) error {
	return b.descsCollection.DeleteDescToBatch(ctx, b.kvTrace, id, b.batch)
}

// DeleteZoneConfig implements the scexec.CatalogChangeBatcher interface.
func (b *catalogChangeBatcher) DeleteZoneConfig(ctx context.Context, id descpb.ID) error {
	return b.descsCollection.DeleteZoneConfigInBatch(ctx, b.kvTrace, b.batch, id)
}

// ValidateAndRun implements the scexec.CatalogChangeBatcher interface.
func (b *catalogChangeBatcher) ValidateAndRun(ctx context.Context) error {
	if err := b.descsCollection.ValidateUncommittedDescriptors(ctx, b.txn); err != nil {
		return err
	}
	if err := b.txn.Run(ctx, b.batch); err != nil {
		return errors.Wrap(err, "writing descriptors")
	}
	return nil
}

// UpdateComment implements the scexec.CatalogChangeBatcher interface.
func (b *catalogChangeBatcher) UpdateComment(
	ctx context.Context, key catalogkeys.CommentKey, cmt string,
) error {
	return b.descsCollection.WriteCommentToBatch(ctx, b.kvTrace, b.batch, key, cmt)
}

// DeleteComment implements the scexec.CatalogChangeBatcher interface.
func (b *catalogChangeBatcher) DeleteComment(
	ctx context.Context, key catalogkeys.CommentKey,
) error {
	return b.descsCollection.DeleteCommentInBatch(ctx, b.kvTrace, b.batch, key)
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

var _ scexec.IndexSpanSplitter = (*txnDeps)(nil)

// MaybeSplitIndexSpans implements the scexec.IndexSpanSplitter interface.
func (d *txnDeps) MaybeSplitIndexSpans(
	ctx context.Context, table catalog.TableDescriptor, indexToBackfill catalog.Index,
) error {
	// Only perform splits on the system tenant.
	if !d.codec.ForSystemTenant() {
		return nil
	}

	span := table.IndexSpan(d.codec, indexToBackfill.GetID())
	const backfillSplitExpiration = time.Hour
	expirationTime := d.txn.DB().Clock().Now().Add(backfillSplitExpiration.Nanoseconds(), 0)
	return d.txn.DB().AdminSplit(ctx, span.Key, expirationTime)
}

// GetResumeSpans implements the scexec.BackfillerTracker interface.
func (d *txnDeps) GetResumeSpans(
	ctx context.Context, tableID descpb.ID, indexID descpb.IndexID,
) ([]roachpb.Span, error) {
	table, err := d.descsCollection.ByID(d.txn).WithoutNonPublic().WithoutSynthetic().Get().Table(ctx, tableID)
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
	return d
}

// TransactionalJobCreator implements the scexec.Dependencies interface.
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

// EventLoggerFactory constructs a new event logger with a txn.
type EventLoggerFactory = func(*kv.Txn) scexec.EventLogger

// MetadataUpdaterFactory constructs a new metadata updater with a txn.
type MetadataUpdaterFactory = func(ctx context.Context, descriptors *descs.Collection, txn *kv.Txn) scexec.DescriptorMetadataUpdater

// EventLogger implements scexec.Dependencies
func (d *execDeps) EventLogger() scexec.EventLogger {
	return d.eventLogger
}

// GetTestingKnobs implements scexec.Dependencies
func (d *execDeps) GetTestingKnobs() *scexec.TestingKnobs {
	return d.testingKnobs
}

// AddTableForStatsRefresh adds a table for stats refresh once we are finished
// executing the current transaction.
func (d *execDeps) AddTableForStatsRefresh(id descpb.ID) {
	d.tableStatsToRefresh = append(d.tableStatsToRefresh, id)
}

// getTablesForStatsRefresh gets tables that need refresh for stats.
func (d *execDeps) getTablesForStatsRefresh() []descpb.ID {
	return d.tableStatsToRefresh
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
