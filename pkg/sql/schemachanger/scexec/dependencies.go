// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// Dependencies contains all the dependencies required by the executor.
type Dependencies interface {
	Catalog() Catalog
	TransactionalJobCreator() TransactionalJobCreator
	IndexBackfiller() IndexBackfiller
	IndexValidator() IndexValidator
	IndexSpanSplitter() IndexSpanSplitter
	JobProgressTracker() JobProgressTracker
	EventLogger() EventLogger

	// Statements returns the statements behind this schema change.
	Statements() []string
}

// Catalog encapsulates the catalog-related dependencies for the executor.
// This involves reading descriptors, as well as preparing batches of catalog
// changes.
type Catalog interface {
	scmutationexec.CatalogReader

	// MustReadMutableDescriptor the mutable equivalent to
	// MustReadImmutableDescriptor in scmutationexec.CatalogReader.
	// This method should be used carefully.
	MustReadMutableDescriptor(ctx context.Context, id descpb.ID) (catalog.MutableDescriptor, error)

	// NewCatalogChangeBatcher is equivalent to creating a new kv.Batch for the
	// current kv.Txn.
	NewCatalogChangeBatcher() CatalogChangeBatcher
}

// EventLogger encapsulates the operations for collecting
// and emitting event log entries.
type EventLogger interface {
	scmutationexec.EventLogWriter
	ProcessAndSubmitEvents(ctx context.Context) error
}

// CatalogChangeBatcher encapsulates batched updates to the catalog: descriptor
// updates, namespace operations, etc.
type CatalogChangeBatcher interface {

	// CreateOrUpdateDescriptor upserts a descriptor.
	CreateOrUpdateDescriptor(ctx context.Context, desc catalog.MutableDescriptor) error

	// DeleteName deletes a namespace entry.
	DeleteName(ctx context.Context, nameInfo descpb.NameInfo, id descpb.ID) error

	// DeleteDescriptor deletes a descriptor entry.
	DeleteDescriptor(ctx context.Context, id descpb.ID) error

	// ValidateAndRun executes the updates after validating them using
	// catalog.Validate.
	ValidateAndRun(ctx context.Context) error
}

// TransactionalJobCreator creates a job in the current transaction.
type TransactionalJobCreator interface {

	// CreateJob creates a job in the current transaction and returns the
	// id which was assigned to that job, or an error otherwise.
	CreateJob(ctx context.Context, record jobs.Record) (jobspb.JobID, error)
}

// IndexBackfiller is an abstract index backfiller that performs index backfills
// when provided with a specification of tables and indexes and a way to track
// job progress.
type IndexBackfiller interface {
	BackfillIndex(
		ctx context.Context,
		_ JobProgressTracker,
		_ catalog.TableDescriptor,
		source descpb.IndexID,
		destinations ...descpb.IndexID,
	) error
}

// Partitioner provides an interface that implements CCL exclusive
// callbacks.
type Partitioner interface {
	AddPartitioning(
		ctx context.Context,
		tableDesc *tabledesc.Mutable,
		indexDesc *descpb.IndexDescriptor,
		partitionFields []string,
		listPartition []*scpb.ListPartition,
		rangePartition []*scpb.RangePartitions,
		allowedNewColumnNames []tree.Name,
		allowImplicitPartitioning bool,
	) (err error)
}

// IndexValidator provides interfaces that allow indexes to be validated.
type IndexValidator interface {
	ValidateForwardIndexes(
		ctx context.Context,
		tableDesc catalog.TableDescriptor,
		indexes []catalog.Index,
		withFirstMutationPublic bool,
		gatherAllInvalid bool,
		override sessiondata.InternalExecutorOverride,
	) error

	ValidateInvertedIndexes(
		ctx context.Context,
		tableDesc catalog.TableDescriptor,
		indexes []catalog.Index,
		gatherAllInvalid bool,
		override sessiondata.InternalExecutorOverride,
	) error
}

// IndexSpanSplitter can try to split an index span in the current transaction
// prior to backfilling.
type IndexSpanSplitter interface {

	// MaybeSplitIndexSpans will attempt to split the backfilled index span.
	MaybeSplitIndexSpans(ctx context.Context, table catalog.TableDescriptor, indexToBackfill catalog.Index) error
}

// JobProgressTracker abstracts the infrastructure to read and write backfill
// progress to job state.
type JobProgressTracker interface {

	// This interface is implicitly implying that there is only one stage of
	// index backfills for a given table in a schema change. It implies that
	// because it assumes that it's safe and reasonable to just store one set of
	// resume spans per table on the job.
	//
	// Potentially something close to interface could still work if there were
	// multiple stages of backfills for a table if we tracked which stage this
	// were somehow. Maybe we could do something like increment a stage counter
	// per table after finishing the backfills.
	//
	// It definitely is possible that there are multiple index backfills on a
	// table in the context of a single schema change that changes the set of
	// columns (primary index) and adds secondary indexes.
	//
	// Really this complexity arises in the computation of the fraction completed.
	// We'll want to know whether there are more index backfills to come.
	//
	// One idea is to index secondarily on the source index.

	GetResumeSpans(ctx context.Context, tableID descpb.ID, indexID descpb.IndexID) ([]roachpb.Span, error)
	SetResumeSpans(ctx context.Context, tableID descpb.ID, indexID descpb.IndexID, total, done []roachpb.Span) error
}
