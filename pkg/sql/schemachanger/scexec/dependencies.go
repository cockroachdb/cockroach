// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	Validator() Validator
	IndexSpanSplitter() IndexSpanSplitter
	DescriptorMetadataUpdater(ctx context.Context) DescriptorMetadataUpdater
	StatsRefresher() StatsRefreshQueue
	GetTestingKnobs() *TestingKnobs
	Telemetry() Telemetry

	// Statements returns the statements behind this schema change.
	Statements() []string
	User() username.SQLUsername
	ClusterSettings() *cluster.Settings
}

// Catalog encapsulates the catalog-related dependencies for the executor.
// This involves reading descriptors, as well as preparing batches of catalog
// changes.
type Catalog interface {
	scmutationexec.NameResolver
	scmutationexec.DescriptorReader
	TemporarySchemaCreator

	// CreateOrUpdateDescriptor upserts a descriptor.
	CreateOrUpdateDescriptor(ctx context.Context, desc catalog.MutableDescriptor) error

	// DeleteName deletes a namespace entry.
	DeleteName(ctx context.Context, nameInfo descpb.NameInfo, id descpb.ID) error

	// AddName adds a namespace entry.
	AddName(ctx context.Context, nameInfo descpb.NameInfo, id descpb.ID) error

	// DeleteDescriptor deletes a descriptor entry.
	DeleteDescriptor(ctx context.Context, id descpb.ID) error

	// WriteZoneConfigToBatch adds the new zoneconfig to uncommitted layer and
	// writes to the kv batch.
	WriteZoneConfigToBatch(ctx context.Context, id descpb.ID, zc catalog.ZoneConfig) error

	// GetZoneConfig gets the zone config for a descriptor ID.
	GetZoneConfig(ctx context.Context, id descpb.ID) (catalog.ZoneConfig, error)

	// UpdateZoneConfig upserts a zone config for a descriptor ID.
	UpdateZoneConfig(ctx context.Context, id descpb.ID, zc *zonepb.ZoneConfig) error

	// UpdateSubzoneConfig upserts a subzone config into the given zone config
	// for a descriptor ID.
	UpdateSubzoneConfig(
		ctx context.Context,
		parentZone catalog.ZoneConfig,
		subzone zonepb.Subzone,
		subzoneSpans []zonepb.SubzoneSpan,
		idxRefToDelete int32,
	) (catalog.ZoneConfig, error)

	// DeleteZoneConfig deletes the zone config for a descriptor.
	DeleteZoneConfig(ctx context.Context, id descpb.ID) error

	// DeleteSubzoneConfig deletes a subzone config from the zone config for a
	// table.
	DeleteSubzoneConfig(
		ctx context.Context,
		tableID descpb.ID,
		subzone zonepb.Subzone,
		subzoneSpans []zonepb.SubzoneSpan,
	) error

	// UpdateComment upserts a comment for the (objID, subID, cmtType) key.
	UpdateComment(ctx context.Context, key catalogkeys.CommentKey, cmt string) error

	// DeleteComment deletes a comment with (objID, subID, cmtType) key.
	DeleteComment(ctx context.Context, key catalogkeys.CommentKey) error

	// Validate validates all the uncommitted catalog changes performed
	// in this transaction so far.
	Validate(ctx context.Context) error

	// Run persists all the uncommitted catalog changes performed in this
	// transaction so far. Reset cannot be called after this method.
	Run(ctx context.Context) error

	// Reset undoes all the uncommitted catalog changes performed in this
	// transaction so far, assuming that they haven't been persisted yet
	// by calling Run.
	Reset(ctx context.Context) error

	// InitializeSequence initializes the initial value for a sequence.
	InitializeSequence(id descpb.ID, startVal int64)
}

// Telemetry encapsulates metrics gather for the declarative schema changer.
type Telemetry interface {
	// IncrementSchemaChangeErrorType increments the number of errors of a given
	// type observed by the schema changer.
	IncrementSchemaChangeErrorType(typ string)
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

	// CurrentJob returns the schema changer job that is currently, running,
	// if we are executing within a job.
	CurrentJob() *jobs.Job

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
		*jobs.Job,
		catalog.TableDescriptor,
	) error
}

// Merger is an abstract index merger that performs index merges
// when provided with a specification of tables and indexes and a way to track
// job progress.
type Merger interface {

	// MergeIndexes will merge the specified indexes in the table, from each
	// temporary index into each adding index.
	MergeIndexes(context.Context, *jobs.Job, MergeProgress, BackfillerProgressWriter, catalog.TableDescriptor) error
}

// Validator provides interfaces that allow indexes and check constraints to be validated.
type Validator interface {
	ValidateForwardIndexes(
		ctx context.Context,
		job *jobs.Job,
		tbl catalog.TableDescriptor,
		indexes []catalog.Index,
		override sessiondata.InternalExecutorOverride,
	) error

	ValidateInvertedIndexes(
		ctx context.Context,
		job *jobs.Job,
		tbl catalog.TableDescriptor,
		indexes []catalog.Index,
		override sessiondata.InternalExecutorOverride,
	) error

	ValidateConstraint(
		ctx context.Context,
		tbl catalog.TableDescriptor,
		constraint catalog.Constraint,
		indexIDForValidation descpb.IndexID,
		override sessiondata.InternalExecutorOverride,
	) error
}

// IndexSpanSplitter can try to split an index span in the current transaction
// prior to backfilling.
type IndexSpanSplitter interface {

	// MaybeSplitIndexSpans will attempt to split the backfilled index span, if
	// the index is in the system tenant or is partitioned.
	MaybeSplitIndexSpans(ctx context.Context, table catalog.TableDescriptor, indexToBackfill catalog.Index) error

	// MaybeSplitIndexSpansForPartitioning will split backfilled index spans
	// across hash-sharded index boundaries if applicable.
	MaybeSplitIndexSpansForPartitioning(ctx context.Context, table catalog.TableDescriptor, indexToBackfill catalog.Index) error
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
	StartPeriodicUpdates(ctx context.Context, tracker BackfillerProgressFlusher) (stop func())
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
	// DeleteDatabaseRoleSettings deletes role settings associated with a database.
	DeleteDatabaseRoleSettings(ctx context.Context, dbID descpb.ID) error

	// DeleteSchedule deletes the given schedule.
	DeleteSchedule(ctx context.Context, id jobspb.ScheduleID) error

	// UpdateTTLScheduleLabel updates the schedule_name for the TTL Scheduled Job
	// of the given table.
	UpdateTTLScheduleLabel(ctx context.Context, tbl *tabledesc.Mutable) error
}

type TemporarySchemaCreator interface {
	// InsertTemporarySchema inserts a temporary schema into the current session
	// data.
	InsertTemporarySchema(
		tempSchemaName string, databaseID descpb.ID, schemaID descpb.ID,
	)
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

// ProtectedTimestampManager used to install a protected timestamp before
// the GC interval is encountered.
type ProtectedTimestampManager interface {
	// TryToProtectBeforeGC adds a protected timestamp record for a historical
	// transaction for a specific table, once a certain percentage of the GC time
	// has elapsed. This is done on a best effort bases using a timer relative to
	// the GC TTL, and should be done fairy early in the transaction. Note, the
	// function assumes the in-memory job is up to date with the persisted job
	// record.
	TryToProtectBeforeGC(
		ctx context.Context, job *jobs.Job, tableDesc catalog.TableDescriptor, readAsOf hlc.Timestamp,
	) jobsprotectedts.Cleaner

	// Protect adds a protected timestamp record for a historical transaction for
	// a specific target immediately. If an existing record is found, it will be
	// updated with a new timestamp. Returns a Cleaner function to remove the
	// protected timestamp, if one was installed. Note, the function assumes the
	// in-memory job is up to date with the persisted job record.
	Protect(
		ctx context.Context,
		job *jobs.Job,
		target *ptpb.Target,
		readAsOf hlc.Timestamp,
	) (jobsprotectedts.Cleaner, error)

	// Unprotect unprotects the spans associated with the job, mainly for last
	// resort cleanup. The function assumes the in-memory job is up to date with
	// the persisted job record. Note: This should only be used for job cleanup if
	// its not currently, executing.
	Unprotect(ctx context.Context, job *jobs.Job) error
}
