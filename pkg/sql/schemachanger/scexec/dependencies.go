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

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
)

// Dependencies contains all the dependencies required by the executor.
type Dependencies interface {
	Catalog() Catalog
	Clock() scmutationexec.Clock
	TransactionalJobRegistry() TransactionalJobRegistry
	IndexBackfiller() Backfiller
	IndexMerger() Merger
	BackfillProgressTracker() BackfillerTracker
	PeriodicProgressFlusher() PeriodicProgressFlusher
	IndexValidator() IndexValidator
	IndexSpanSplitter() IndexSpanSplitter
	EventLogger() EventLogger
	DescriptorMetadataUpdater(ctx context.Context) DescriptorMetadataUpdater
	StatsRefresher() StatsRefreshQueue
	GetTestingKnobs() *TestingKnobs
	Telemetry() Telemetry

	// Statements returns the statements behind this schema change.
	Statements() []string
	User() username.SQLUsername
}

// Catalog encapsulates the catalog-related dependencies for the executor.
// This involves reading descriptors, as well as preparing batches of catalog
// changes.
type Catalog interface {
	scmutationexec.NameResolver

	// MustReadImmutableDescriptors reads descriptors from the catalog by ID.
	MustReadImmutableDescriptors(ctx context.Context, ids ...descpb.ID) ([]catalog.Descriptor, error)

	// MustReadMutableDescriptor the mutable equivalent to
	// MustReadImmutableDescriptors.
	MustReadMutableDescriptor(ctx context.Context, id descpb.ID) (catalog.MutableDescriptor, error)

	// NewCatalogChangeBatcher is equivalent to creating a new kv.Batch for the
	// current kv.Txn.
	NewCatalogChangeBatcher() CatalogChangeBatcher
}

// EventLogger encapsulates the operations for emitting event log entries.
type EventLogger interface {
	// LogEvent writes to the event log.
	LogEvent(
		ctx context.Context,
		details eventpb.CommonSQLEventDetails,
		event logpb.EventPayload,
	) error

	// LogEventForSchemaChange write a schema change event entry into the event log.
	LogEventForSchemaChange(
		ctx context.Context, event logpb.EventPayload,
	) error
}

// Telemetry encapsulates metrics gather for the declarative schema changer.
type Telemetry interface {
	// IncrementSchemaChangeErrorType increments the number of errors of a given
	// type observed by the schema changer.
	IncrementSchemaChangeErrorType(typ string)
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

	// ValidateAndRun executes the updates after validating the catalog changes.
	ValidateAndRun(ctx context.Context) error

	// DeleteZoneConfig deletes the zone config for a descriptor.
	DeleteZoneConfig(ctx context.Context, id descpb.ID) error
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

	// CreatedJobs is the set of jobs created thus far in the current
	// transaction.
	CreatedJobs() []jobspb.JobID

	// CheckPausepoint returns a PauseRequestError if the named pause-point is
	// set.
	//
	// See (*jobs.Registry).CheckPausepoint
	CheckPausepoint(name string) error

	// UseLegacyGCJob indicate whether the legacy GC job should be used.
	// This only matters for setting the initial RunningStatus.
	UseLegacyGCJob(ctx context.Context) bool

	// TODO(ajwerner): Deal with setting the running status to indicate
	// validating, backfilling, or generally performing metadata changes
	// and waiting for lease draining.
}

// JobUpdateCallback is for updating a job.
type JobUpdateCallback = func(
	md jobs.JobMetadata,
	updateProgress func(*jobspb.Progress),
	updatePayload func(*jobspb.Payload),
) error

// Backfiller is an abstract index backfiller that performs index backfills
// when provided with a specification of tables and indexes and a way to track
// job progress.
type Backfiller interface {

	// MaybePrepareDestIndexesForBackfill will choose a MinimumWriteTimestamp
	// for the backfill if one does not exist. It will scan all destination
	// indexes at that timestamp to ensure that no subsequent writes from
	// other transactions will occur below that timestamp.
	MaybePrepareDestIndexesForBackfill(
		context.Context, BackfillProgress, catalog.TableDescriptor,
	) (BackfillProgress, error)

	// BackfillIndexes will backfill the specified indexes in the table with
	// the specified source and destination indexes. Note that the
	// MinimumWriteTimestamp on the progress must be non-zero. Use
	// MaybePrepareDestIndexesForBackfill to construct a properly initialized
	// progress.
	BackfillIndexes(
		context.Context,
		BackfillProgress,
		BackfillerProgressWriter,
		catalog.TableDescriptor,
	) error
}

// Merger is an abstract index merger that performs index merges
// when provided with a specification of tables and indexes and a way to track
// job progress.
type Merger interface {

	// MergeIndexes will merge the specified indexes in the table, from each
	// temporary index into each adding index.
	MergeIndexes(
		context.Context,
		MergeProgress,
		BackfillerProgressWriter,
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

	// CompletedSpans contains the spans of the source index which have been
	// backfilled into the destination indexes. The spans are expected to
	// contain any tenant prefix.
	CompletedSpans []roachpb.Span
}

// Backfill corresponds to a definition of a backfill from a source
// index into multiple destination indexes.
type Backfill struct {
	TableID       descpb.ID
	SourceIndexID descpb.IndexID
	DestIndexIDs  []descpb.IndexID
}

// MergeProgress tracks the progress for a Merge.
type MergeProgress struct {
	Merge

	// CompletedSpans contains the spans of the source indexes which have been
	// merged into the destination indexes. The spans are expected to
	// contain any tenant prefix. The outer slice is parallel to the
	// SourceIndexIDs slice in the embedded Merge struct.
	CompletedSpans [][]roachpb.Span
}

// MakeMergeProgress constructs a new MergeProgress for a merge with an empty
// set of CompletedSpans.
func MakeMergeProgress(m Merge) MergeProgress {
	return MergeProgress{
		Merge:          m,
		CompletedSpans: make([][]roachpb.Span, len(m.SourceIndexIDs)),
	}
}

// Merge corresponds to a definition of a merge from multiple temporary indexes
// into adding indexes.
type Merge struct {
	TableID        descpb.ID
	SourceIndexIDs []descpb.IndexID
	DestIndexIDs   []descpb.IndexID
}

// BackfillerTracker abstracts the infrastructure to read and write backfill
// and merge progress to job state. Implementations should support multiple
// concurrent writers.
type BackfillerTracker interface {
	BackfillerProgressReader
	BackfillerProgressWriter
	BackfillerProgressFlusher
}

// PeriodicProgressFlusher is used to write updates to backfill progress
// periodically.
type PeriodicProgressFlusher interface {
	StartPeriodicUpdates(ctx context.Context, tracker BackfillerProgressFlusher) (stop func() error)
}

// BackfillerProgressReader is used by the backfill execution layer to read
// backfill and merge progress.
type BackfillerProgressReader interface {
	// GetBackfillProgress reads the backfill progress for the specified backfill.
	// If no such backfill has been stored previously, this call will return a
	// new BackfillProgress without the CompletedSpans or MinimumWriteTimestamp
	// populated.
	GetBackfillProgress(ctx context.Context, b Backfill) (BackfillProgress, error)
	// GetMergeProgress reads the merge progress for the specified merge.
	// If no such merge has been stored previously, this call will return a
	// new MergeProgress without the CompletedSpans populated
	GetMergeProgress(ctx context.Context, b Merge) (MergeProgress, error)
}

// BackfillerProgressWriter is used by the backfiller to write out progress
// updates.
type BackfillerProgressWriter interface {
	// SetBackfillProgress updates the progress for a single backfill. Multiple
	// backfills may be concurrently tracked. Setting the progress may not make
	// that progress durable; the concrete implementation of the backfill tracker
	// may defer writing until later.
	SetBackfillProgress(ctx context.Context, progress BackfillProgress) error
	// SetMergeProgress updates the progress for a single merge. Multiple
	// merges may be concurrently tracked. Setting the progress may not make
	// that progress durable; the concrete implementation of the merge tracker
	// may defer writing until later.
	SetMergeProgress(ctx context.Context, progress MergeProgress) error
}

// BackfillerProgressFlusher is used to flush backfill and merge progress state
// to the underlying store.
type BackfillerProgressFlusher interface {

	// FlushCheckpoint writes out a checkpoint containing any data which has
	// been previously set via SetBackfillProgress or SetMergeProgress.
	FlushCheckpoint(ctx context.Context) error

	// FlushFractionCompleted writes out the fraction completed.
	FlushFractionCompleted(ctx context.Context) error
}

// DescriptorMetadataUpdater is used to update metadata associated with schema objects,
// for example comments associated with a schema.
type DescriptorMetadataUpdater interface {
	// UpsertDescriptorComment updates a comment associated with a schema object.
	UpsertDescriptorComment(id int64, subID int64, commentType keys.CommentType, comment string) error

	// DeleteDescriptorComment deletes a comment for schema object.
	DeleteDescriptorComment(id int64, subID int64, commentType keys.CommentType) error

	//UpsertConstraintComment upserts a comment associated with a constraint.
	UpsertConstraintComment(tableID descpb.ID, constraintID descpb.ConstraintID, comment string) error

	//DeleteConstraintComment deletes a comment associated with a constraint.
	DeleteConstraintComment(tableID descpb.ID, constraintID descpb.ConstraintID) error

	// DeleteDatabaseRoleSettings deletes role settings associated with a database.
	DeleteDatabaseRoleSettings(ctx context.Context, dbID descpb.ID) error

	// SwapDescriptorSubComment moves a comment from one sub ID to another.
	SwapDescriptorSubComment(id int64, oldSubID int64, newSubID int64, commentType keys.CommentType) error

	// DeleteAllCommentsForTables deletes all table-bound comments for the tables
	// with the specified IDs.
	DeleteAllCommentsForTables(ids catalog.DescriptorIDSet) error

	// DeleteSchedule deletes the given schedule.
	DeleteSchedule(ctx context.Context, id int64) error

	// UpsertZoneConfig sets the zone config for a given descriptor. If necessary,
	// the subzone spans will be recomputed as part of this call.
	UpsertZoneConfig(
		ctx context.Context, id descpb.ID, zone *zonepb.ZoneConfig,
	) (numAffected int, err error)

	// DeleteZoneConfig deletes a zone config for a given descriptor.
	DeleteZoneConfig(ctx context.Context, id descpb.ID) (numAffected int, err error)
}

// StatsRefreshQueue queues table for stats refreshes.
type StatsRefreshQueue interface {
	// AddTableForStatsRefresh adds a table for a stats refresh.
	AddTableForStatsRefresh(id descpb.ID)
}

// StatsRefresher responsible for refreshing table stats.
type StatsRefresher interface {
	// NotifyMutation notifies the stats refresher that a table needs its
	// statistics updated.
	NotifyMutation(table catalog.TableDescriptor, rowsAffected int)
}
