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
}

// NewExecutorDependencies returns an scexec.Dependencies implementation built
// from the given arguments.
func NewExecutorDependencies(
	codec keys.SQLCodec,
	txn *kv.Txn,
	user security.SQLUsername,
	descsCollection *descs.Collection,
	jobRegistry JobRegistry,
	indexBackfiller scexec.IndexBackfiller,
	indexValidator scexec.IndexValidator,
	partitioner scmutationexec.Partitioner,
	eventLogger scexec.EventLogger,
	statements []string,
) scexec.Dependencies {
	return &execDeps{
		txnDeps: txnDeps{
			txn:             txn,
			codec:           codec,
			descsCollection: descsCollection,
			jobRegistry:     jobRegistry,
			indexValidator:  indexValidator,
			eventLogger:     eventLogger,
		},
		indexBackfiller: indexBackfiller,
		statements:      statements,
		partitioner:     partitioner,
		user:            user,
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
}

func (d *txnDeps) UpdateSchemaChangeJob(
	ctx context.Context, id jobspb.JobID, fn scexec.JobProgressUpdateFunc,
) error {
	const useReadLock = false
	return d.jobRegistry.UpdateJobWithTxn(ctx, id, d.txn, useReadLock, func(
		txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
	) error {
		return fn(md, ju.UpdateProgress)
	})
}

var _ scexec.Catalog = (*txnDeps)(nil)

// MustReadImmutableDescriptor implements the scmutationexec.CatalogReader interface.
func (d *txnDeps) MustReadImmutableDescriptor(
	ctx context.Context, id descpb.ID,
) (catalog.Descriptor, error) {
	flags := tree.CommonLookupFlags{
		Required:       true,
		RequireMutable: false,
		AvoidLeased:    true,
		IncludeOffline: true,
		IncludeDropped: true,
	}
	return d.descsCollection.GetImmutableDescriptorByID(ctx, d.txn, id, flags)
}

// GetFullyQualifiedName implements the scmutationexec.CatalogReader interface
func (d *txnDeps) GetFullyQualifiedName(ctx context.Context, id descpb.ID) (string, error) {
	objectDesc, err := d.descsCollection.GetImmutableDescriptorByID(ctx,
		d.txn,
		id,
		tree.CommonLookupFlags{
			Required:       true,
			IncludeDropped: true,
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
			})
		if err != nil {
			return "", err
		}
		schemaDesc, err := d.descsCollection.GetImmutableSchemaByID(ctx, d.txn, objectDesc.GetParentSchemaID(),
			tree.SchemaLookupFlags{
				Required:       true,
				IncludeDropped: true})
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
	return d.descsCollection.GetMutableDescriptorByID(ctx, id, d.txn)
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

// CreateOrUpdateDescriptor implements the scexec.CatalogChangeBatcher interface.
func (b *catalogChangeBatcher) CreateOrUpdateDescriptor(
	ctx context.Context, desc catalog.MutableDescriptor,
) error {
	return b.descsCollection.WriteDescToBatch(ctx, false /* kvTrace */, desc, b.batch)
}

// DeleteName implements the scexec.CatalogChangeBatcher interface.
func (b *catalogChangeBatcher) DeleteName(
	ctx context.Context, nameInfo descpb.NameInfo, id descpb.ID,
) error {
	b.batch.Del(catalogkeys.EncodeNameKey(b.codec, nameInfo))
	return nil
}

// DeleteDescriptor implements the scexec.CatalogChangeBatcher interface.
func (b *catalogChangeBatcher) DeleteDescriptor(ctx context.Context, id descpb.ID) error {
	b.batch.Del(catalogkeys.MakeDescMetadataKey(b.codec, id))
	b.deletedDescriptors.Add(id)
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

var _ scexec.TransactionalJobCreator = (*txnDeps)(nil)

func (d *txnDeps) MakeJobID() jobspb.JobID {
	return d.jobRegistry.MakeJobID()
}

// CreateJob implements the scexec.TransactionalJobCreator interface.
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

var _ scexec.JobProgressTracker = (*execDeps)(nil)

// GetResumeSpans implements the scexec.JobProgressTracker interface.
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

// SetResumeSpans implements the scexec.JobProgressTracker interface.
func (d *txnDeps) SetResumeSpans(
	ctx context.Context, tableID descpb.ID, indexID descpb.IndexID, total, done []roachpb.Span,
) error {
	panic("implement me")
}

type execDeps struct {
	txnDeps
	indexBackfiller scexec.IndexBackfiller
	partitioner     scmutationexec.Partitioner
	statements      []string
	user            security.SQLUsername
}

var _ scexec.Dependencies = (*execDeps)(nil)

// Catalog implements the scexec.Dependencies interface.
func (d *execDeps) Catalog() scexec.Catalog {
	return d
}

// Partitioner implements the scexec.Dependencies interface.
func (d *execDeps) Partitioner() scmutationexec.Partitioner {
	return d.partitioner
}

// IndexBackfiller implements the scexec.Dependencies interface.
func (d *execDeps) IndexBackfiller() scexec.IndexBackfiller {
	return d.indexBackfiller
}

func (d *execDeps) IndexValidator() scexec.IndexValidator {
	return d.indexValidator
}

// IndexSpanSplitter implements the scexec.Dependencies interface.
func (d *execDeps) IndexSpanSplitter() scexec.IndexSpanSplitter {
	return d
}

// JobProgressTracker implements the scexec.Dependencies interface.
func (d *execDeps) JobProgressTracker() scexec.JobProgressTracker {
	return d
}

// TransactionalJobCreator implements the scexec.Dependencies interface.
func (d *execDeps) TransactionalJobCreator() scexec.TransactionalJobCreator {
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

// EventLogger implements scexec.Dependencies
func (d *execDeps) EventLogger() scexec.EventLogger {
	return d.eventLogger
}
