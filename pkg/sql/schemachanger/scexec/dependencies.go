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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

// Dependencies contains all the dependencies required by the executor.
type Dependencies interface {
	Catalog() Catalog
	Partitioner() scmutationexec.Partitioner
	TransactionalJobRegistry() TransactionalJobRegistry
	IndexBackfiller() Backfiller
	BackfillProgressTracker() BackfillTracker
	PeriodicProgressFlusher() PeriodicProgressFlusher
	IndexValidator() IndexValidator
	IndexSpanSplitter() IndexSpanSplitter
	EventLogger() EventLogger

	// Statements returns the statements behind this schema change.
	Statements() []string
	User() security.SQLUsername
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

// EventLogger encapsulates the operations for emitting event log entries.
type EventLogger interface {
	// LogEvent writes to the eventlog.
	LogEvent(ctx context.Context, descID descpb.ID, metadata scpb.ElementMetadata, event eventpb.EventPayload) error
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

// TransactionalJobRegistry creates and updates jobs in the current transaction.
type TransactionalJobRegistry interface {

	// UpdateSchemaChangeJob triggers the update of the current schema change job
	// via the supplied callback.
	UpdateSchemaChangeJob(ctx context.Context, id jobspb.JobID, fn JobUpdateCallback) error

	// MakeJobID is used to make a JobID.
	MakeJobID() jobspb.JobID

	// SchemaChangerJobID returns the schema changer job ID, creating one if it
	// doesn't yet exist.
	SchemaChangerJobID() jobspb.JobID

	// CreateJob creates a job in the current transaction and returns the
	// id which was assigned to that job, or an error otherwise.
	CreateJob(ctx context.Context, record jobs.Record) error

	// TODO(ajwerner): Deal with setting the running status to indicate
	// validating, backfilling, or generally performing metadata changes
	// and waiting for lease draining.
}

// JobUpdateCallback is for updating a job.
type JobUpdateCallback = func(
	md jobs.JobMetadata,
	updateProgress func(*jobspb.Progress),
	setNonCancelable func(),
) error

// Backfiller is an abstract index backfiller that performs index backfills
// when provided with a specification of tables and indexes and a way to track
// job progress.
type Backfiller interface {

	// MaybePrepareDestIndexesForBackfill will choose a MinimumWriteTimestamp
	// for the backfill if one does not exist. It will scan all destination
	// indexes at that timestamp to ensure that no subsequent writes from
	// other transactions will occur below that timestamp. If it populates
	// the timestamp, it will initialize the set of SpansToDo to the entire
	// index span corresponding to the source index.
	MaybePrepareDestIndexesForBackfill(
		context.Context, BackfillProgress, catalog.TableDescriptor,
	) (BackfillProgress, error)

	// BackfillIndex will backfill the specified indexes on in the table with
	// the specified source and destination indexes. Note that the
	// MinimumWriteTimestamp on the progress must be non-zero. Use
	// MaybePrepareDestIndexesForBackfill to construct a properly initialized
	// progress.
	BackfillIndex(
		context.Context,
		BackfillProgress,
		BackfillProgressWriter,
		catalog.TableDescriptor,
	) error
}

// IndexValidator provides interfaces that allow indexes to be validated.
type IndexValidator interface {
	ValidateForwardIndexes(
		ctx context.Context,
		tbl catalog.TableDescriptor,
		indexes []catalog.Index,
		override sessiondata.InternalExecutorOverride,
	) error

	ValidateInvertedIndexes(
		ctx context.Context,
		tbl catalog.TableDescriptor,
		indexes []catalog.Index,
		override sessiondata.InternalExecutorOverride,
	) error
}

// IndexSpanSplitter can try to split an index span in the current transaction
// prior to backfilling.
type IndexSpanSplitter interface {

	// MaybeSplitIndexSpans will attempt to split the backfilled index span.
	MaybeSplitIndexSpans(ctx context.Context, table catalog.TableDescriptor, indexToBackfill catalog.Index) error
}

// BackfillProgress tracks the progress for a Backfill.
type BackfillProgress struct {
	Backfill

	// MinimumWriteTimestamp is a timestamp above which all
	// reads and writes of the backfill should be performed.
	// The timestamp corresponds to a timestamp at or before
	// which the destination indexes have been scanned and thus
	// all subsequent writes due to other transactions will occur.
	MinimumWriteTimestamp hlc.Timestamp

	// SpansToDo contains the spans of the source index which still need to
	// be backfilled. The spans are expected to contain any tenant prefix.
	SpansToDo []roachpb.Span
}

// Backfill corresponds to a definition of a backfill from a source
// index into multiple destination indexes.
type Backfill struct {
	TableID       descpb.ID
	SourceIndexID descpb.IndexID
	DestIndexIDs  []descpb.IndexID
}

// BackfillTracker abstracts the infrastructure to read and write backfill
// progress to job state. Implementations should support multiple concurrent
// writers.
type BackfillTracker interface {
	BackfillProgressReader
	BackfillProgressWriter
	BackfillProgressFlusher
}

// PeriodicProgressFlusher is used to write updates to backfill progress
// periodically.
type PeriodicProgressFlusher interface {
	StartPeriodicUpdates(ctx context.Context, tracker BackfillProgressFlusher) (stop func() error)
}

// BackfillProgressReader is used by the backfill execution layer to read
// backfill progress.
type BackfillProgressReader interface {
	// GetBackfillProgress reads the backfill progress for the specified backfill.
	// If no such backfill has been stored previously, this call will return a
	// new BackfillProgress without
	GetBackfillProgress(ctx context.Context, b Backfill) (BackfillProgress, error)
}

// BackfillProgressWriter is used by the backfiller to write out progress
// updates.
type BackfillProgressWriter interface {
	// SetBackfillProgress updates the progress for a single backfill. Multiple
	// backfills may be concurrently tracked. Setting the progress may not make
	// that progress durable; the concrete implementation of the backfill tracker
	// may defer writing until later.
	SetBackfillProgress(ctx context.Context, progress BackfillProgress) error
}

// BackfillProgressFlusher is used to flush backfill progress state to
// the underlying store.
type BackfillProgressFlusher interface {

	// FlushCheckpoint writes out a checkpoint containing any data which has
	// been previously set via SetBackfillProgress.
	FlushCheckpoint(ctx context.Context) error

	// FlushFractionCompleted writes out the fraction completed.
	FlushFractionCompleted(ctx context.Context) error
}
