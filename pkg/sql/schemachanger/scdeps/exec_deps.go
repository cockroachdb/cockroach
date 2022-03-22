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

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	codec keys.SQLCodec,
	sessionData *sessiondata.SessionData,
	txn *kv.Txn,
	user security.SQLUsername,
	descsCollection *descs.Collection,
	jobRegistry JobRegistry,
	backfiller scexec.Backfiller,
	backfillTracker scexec.BackfillTracker,
	backfillFlusher scexec.PeriodicProgressFlusher,
	indexValidator scexec.IndexValidator,
	clock scmutationexec.Clock,
	commentUpdaterFactory scexec.DescriptorMetadataUpdaterFactory,
	eventLogger scexec.EventLogger,
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
			indexValidator:     indexValidator,
			eventLogger:        eventLogger,
			schemaChangerJobID: schemaChangerJobID,
			kvTrace:            kvTrace,
		},
		backfiller:              backfiller,
		backfillTracker:         backfillTracker,
		commentUpdaterFactory:   commentUpdaterFactory,
		periodicProgressFlusher: backfillFlusher,
		statements:              statements,
		user:                    user,
		sessionData:             sessionData,
		clock:                   clock,
	}
}

type txnDeps struct {
	txn                *kv.Txn
	codec              keys.SQLCodec
	descsCollection    *descs.Collection
	jobRegistry        JobRegistry
	indexValidator     scexec.IndexValidator
	eventLogger        scexec.EventLogger
	deletedDescriptors catalog.DescriptorIDSet
	schemaChangerJobID jobspb.JobID
	kvTrace            bool
}

func (d *txnDeps) UpdateSchemaChangeJob(
	ctx context.Context, id jobspb.JobID, callback scexec.JobUpdateCallback,
) error {
	const useReadLock = false
	return d.jobRegistry.UpdateJobWithTxn(ctx, id, d.txn, useReadLock, func(
		txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
	) error {
		setNonCancelable := func() {
			payload := *md.Payload
			if !payload.Noncancelable {
				payload.Noncancelable = true
				ju.UpdatePayload(&payload)
			}
		}
		return callback(md, ju.UpdateProgress, setNonCancelable)
	})
}

var _ scexec.Catalog = (*txnDeps)(nil)

// MustReadImmutableDescriptors implements the scmutationexec.CatalogReader interface.
func (d *txnDeps) MustReadImmutableDescriptors(
	ctx context.Context, ids ...descpb.ID,
) ([]catalog.Descriptor, error) {
	flags := tree.CommonLookupFlags{
		Required:       true,
		RequireMutable: false,
		AvoidLeased:    true,
		IncludeOffline: true,
		IncludeDropped: true,
	}
	return d.descsCollection.GetImmutableDescriptorsByID(ctx, d.txn, flags, ids...)
}

// GetFullyQualifiedName implements the scmutationexec.CatalogReader interface
func (d *txnDeps) GetFullyQualifiedName(ctx context.Context, id descpb.ID) (string, error) {
	objectDesc, err := d.descsCollection.GetImmutableDescriptorByID(ctx,
		d.txn,
		id,
		tree.CommonLookupFlags{
			Required:       true,
			IncludeDropped: true,
			AvoidLeased:    true,
		})
	if err != nil {
		return "", err
	}
	// For objects like tables, views, sequences, and types
	// we can fetch the fully qualified names.
	if objectDesc.DescriptorType() != catalog.Database &&
		objectDesc.DescriptorType() != catalog.Schema {
		_, databaseDesc, err := d.descsCollection.GetImmutableDatabaseByID(ctx,
			d.txn,
			objectDesc.GetParentID(),
			tree.CommonLookupFlags{
				IncludeDropped: true,
				Required:       true,
				AvoidLeased:    true,
			})
		if err != nil {
			return "", err
		}
		schemaDesc, err := d.descsCollection.GetImmutableSchemaByID(ctx, d.txn, objectDesc.GetParentSchemaID(),
			tree.SchemaLookupFlags{
				Required:       true,
				IncludeDropped: true,
				AvoidLeased:    true,
			})
		if err != nil {
			return "", err
		}
		name := tree.MakeTableNameWithSchema(tree.Name(databaseDesc.GetName()),
			tree.Name(schemaDesc.GetName()),
			tree.Name(objectDesc.GetName()))
		return name.FQString(), nil
	} else if objectDesc.DescriptorType() == catalog.Database {
		return objectDesc.GetName(), nil
	} else if objectDesc.DescriptorType() == catalog.Schema {
		_, databaseDesc, err := d.descsCollection.GetImmutableDatabaseByID(ctx,
			d.txn,
			objectDesc.GetParentID(),
			tree.CommonLookupFlags{
				IncludeDropped: true,
				Required:       true,
				AvoidLeased:    true,
			})
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s.%s", databaseDesc.GetName(), objectDesc.GetName()), nil
	}
	return "", errors.Newf("unknown descriptor type : %s\n", objectDesc.DescriptorType())
}

// AddSyntheticDescriptor implements the scmutationexec.CatalogReader interface.
func (d *txnDeps) AddSyntheticDescriptor(desc catalog.Descriptor) {
	d.descsCollection.AddSyntheticDescriptor(desc)
}

// RemoveSyntheticDescriptor implements the scmutationexec.CatalogReader interface.
func (d *txnDeps) RemoveSyntheticDescriptor(id descpb.ID) {
	d.descsCollection.RemoveSyntheticDescriptor(id)
}

// MustReadMutableDescriptor implements the scexec.Catalog interface.
func (d *txnDeps) MustReadMutableDescriptor(
	ctx context.Context, id descpb.ID,
) (catalog.MutableDescriptor, error) {
	return d.descsCollection.GetMutableDescriptorByID(ctx, d.txn, id)
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
	marshalledKey := catalogkeys.EncodeNameKey(b.codec, nameInfo)
	if b.kvTrace {
		log.VEventf(ctx, 2, "Del %s", marshalledKey)
	}
	b.batch.Del(marshalledKey)
	return nil
}

// DeleteDescriptor implements the scexec.CatalogChangeBatcher interface.
func (b *catalogChangeBatcher) DeleteDescriptor(ctx context.Context, id descpb.ID) error {
	marshalledKey := catalogkeys.MakeDescMetadataKey(b.codec, id)
	b.batch.Del(marshalledKey)
	if b.kvTrace {
		log.VEventf(ctx, 2, "Del %s", marshalledKey)
	}
	b.deletedDescriptors.Add(id)
	b.descsCollection.AddDeletedDescriptor(id)
	return nil
}

// DeleteZoneConfig implements the scexec.CatalogChangeBatcher interface.
func (b *catalogChangeBatcher) DeleteZoneConfig(ctx context.Context, id descpb.ID) error {
	zoneKeyPrefix := config.MakeZoneKeyPrefix(b.codec, id)
	if b.kvTrace {
		log.VEventf(ctx, 2, "DelRange %s", zoneKeyPrefix)
	}
	b.batch.DelRange(zoneKeyPrefix, zoneKeyPrefix.PrefixEnd(), false /* returnKeys */)
	return nil
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

var _ scexec.TransactionalJobRegistry = (*txnDeps)(nil)

func (d *txnDeps) MakeJobID() jobspb.JobID {
	return d.jobRegistry.MakeJobID()
}

func (d *txnDeps) CheckPausepoint(name string) error {
	return d.jobRegistry.CheckPausepoint(name)
}

func (d *txnDeps) SchemaChangerJobID() jobspb.JobID {
	if d.schemaChangerJobID == 0 {
		d.schemaChangerJobID = d.jobRegistry.MakeJobID()
	}
	return d.schemaChangerJobID
}

// CreateJob implements the scexec.TransactionalJobRegistry interface.
func (d *txnDeps) CreateJob(ctx context.Context, record jobs.Record) error {
	_, err := d.jobRegistry.CreateJobWithTxn(ctx, record, record.JobID, d.txn)
	return err
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

// GetResumeSpans implements the scexec.BackfillTracker interface.
func (d *txnDeps) GetResumeSpans(
	ctx context.Context, tableID descpb.ID, indexID descpb.IndexID,
) ([]roachpb.Span, error) {
	table, err := d.descsCollection.GetImmutableTableByID(ctx, d.txn, tableID, tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{
			Required:    true,
			AvoidLeased: true,
		},
	})
	if err != nil {
		return nil, err
	}
	return []roachpb.Span{table.IndexSpan(d.codec, indexID)}, nil
}

// SetResumeSpans implements the scexec.BackfillTracker interface.
func (d *txnDeps) SetResumeSpans(
	ctx context.Context, tableID descpb.ID, indexID descpb.IndexID, total, done []roachpb.Span,
) error {
	panic("implement me")
}

type execDeps struct {
	txnDeps
	clock                   scmutationexec.Clock
	commentUpdaterFactory   scexec.DescriptorMetadataUpdaterFactory
	backfiller              scexec.Backfiller
	backfillTracker         scexec.BackfillTracker
	periodicProgressFlusher scexec.PeriodicProgressFlusher
	statements              []string
	user                    security.SQLUsername
	sessionData             *sessiondata.SessionData
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

// BackfillProgressTracker implements the scexec.Dependencies interface.
func (d *execDeps) BackfillProgressTracker() scexec.BackfillTracker {
	return d.backfillTracker
}

// PeriodicProgressFlusher implements the scexec.Dependencies interface.
func (d *execDeps) PeriodicProgressFlusher() scexec.PeriodicProgressFlusher {
	return d.periodicProgressFlusher
}

func (d *execDeps) IndexValidator() scexec.IndexValidator {
	return d.indexValidator
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
func (d *execDeps) User() security.SQLUsername {
	return d.user
}

// CommentUpdater implements the scexec.Dependencies interface.
func (d *execDeps) DescriptorMetadataUpdater(ctx context.Context) scexec.DescriptorMetadataUpdater {
	return d.commentUpdaterFactory.NewMetadataUpdater(ctx, d.txn, d.sessionData)
}

// EventLoggerFactory constructs a new event logger with a txn.
type EventLoggerFactory = func(*kv.Txn) scexec.EventLogger

// EventLogger implements scexec.Dependencies
func (d *execDeps) EventLogger() scexec.EventLogger {
	return d.eventLogger
}

// NewNoOpBackfillTracker constructs a backfill tracker which does not do
// anything. It will always return progress for a given backfill which
// contains a full set of CompletedSpans corresponding to the source index
// span and an empty MinimumWriteTimestamp.
func NewNoOpBackfillTracker(codec keys.SQLCodec) scexec.BackfillTracker {
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

func (n noopBackfillProgress) SetBackfillProgress(
	ctx context.Context, progress scexec.BackfillProgress,
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
	ctx context.Context, tracker scexec.BackfillProgressFlusher,
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
