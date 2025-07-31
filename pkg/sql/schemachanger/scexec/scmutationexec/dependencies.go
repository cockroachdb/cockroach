// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
)

// Clock is used to provide a timestamp to track loosely when something
// happened. It can be used for things like observability and telemetry and
// not for anything involving correctness.
type Clock interface {

	// ApproximateTime provides a present timestamp.
	ApproximateTime() time.Time
}

// NameResolver is used to retrieve fully qualified names from the catalog.
type NameResolver interface {

	// GetFullyQualifiedName gets the fully qualified name from a descriptor ID.
	GetFullyQualifiedName(ctx context.Context, id descpb.ID) (string, error)
}

// DescriptorReader is used to retrieve descriptors from the catalog.
type DescriptorReader interface {

	// MustReadImmutableDescriptors reads descriptors from the catalog by ID.
	MustReadImmutableDescriptors(ctx context.Context, ids ...descpb.ID) ([]catalog.Descriptor, error)

	// MustReadMutableDescriptor the mutable equivalent to
	// MustReadImmutableDescriptors.
	MustReadMutableDescriptor(ctx context.Context, id descpb.ID) (catalog.MutableDescriptor, error)
}

// ImmediateMutationStateUpdater contains the methods used to update the
// set of changes to bring about from executing immediate mutations.
type ImmediateMutationStateUpdater interface {

	// AddToCheckedOutDescriptors adds a mutable descriptor to the set of
	// checked-out descriptors.
	AddToCheckedOutDescriptors(mut catalog.MutableDescriptor)

	// MaybeGetCheckedOutDescriptor looks up a checked-out descriptor by ID.
	MaybeGetCheckedOutDescriptor(id descpb.ID) catalog.MutableDescriptor

	// DeleteName marks a namespace entry as being drained.
	DeleteName(id descpb.ID, nameInfo descpb.NameInfo)

	// AddName adds a namespace entry.
	AddName(id descpb.ID, nameInfo descpb.NameInfo)

	// CreateDescriptor adds a descriptor for creation.
	CreateDescriptor(desc catalog.MutableDescriptor)

	// DeleteDescriptor adds a descriptor for deletion.
	DeleteDescriptor(id descpb.ID)

	// DeleteComment removes comments for a descriptor.
	DeleteComment(id descpb.ID, subID int, commentType catalogkeys.CommentType)

	// AddComment adds comments for a descriptor.
	AddComment(id descpb.ID, subID int, commentType catalogkeys.CommentType, comment string)

	// InitSequence initializes a sequence.
	InitSequence(id descpb.ID, startVal int64)

	// UpdateZoneConfig upserts a zone config.
	UpdateZoneConfig(id descpb.ID, zc *zonepb.ZoneConfig)

	// UpdateSubzoneConfig upserts a subzone config.
	UpdateSubzoneConfig(tableID descpb.ID, subzone zonepb.Subzone, subzoneSpans []zonepb.SubzoneSpan, idxRefToDelete int32)

	// DeleteZoneConfig deletes the zone config for the given ID.
	DeleteZoneConfig(id descpb.ID)

	// DeleteSubzoneConfig deletes the given subzone config for the given table
	// ID.
	DeleteSubzoneConfig(tableID descpb.ID, subzone zonepb.Subzone, subzoneSpans []zonepb.SubzoneSpan)

	// Reset schedules a reset of the in-txn catalog state
	// to undo the modifications from earlier stages.
	Reset()

	// AddTemporarySchema adds the temporary schema in the current session.
	AddTemporarySchema(id descpb.ID)

	// AddTemporarySchemaParent registers the parent database for an added
	// temporary schema.
	AddTemporarySchemaParent(id descpb.ID, databaseID descpb.ID)
}

type DeferredMutationStateUpdater interface {

	// DeleteDatabaseRoleSettings removes a database role setting
	DeleteDatabaseRoleSettings(ctx context.Context, dbID descpb.ID) error

	// AddNewGCJobForTable enqueues a GC job for the given table.
	AddNewGCJobForTable(stmt scop.StatementForDropJob, dbID, tableID descpb.ID)

	// AddNewGCJobForDatabase enqueues a GC job for the given database.
	AddNewGCJobForDatabase(stmt scop.StatementForDropJob, dbID descpb.ID)

	// AddNewGCJobForIndex enqueues a GC job for the given table index.
	AddNewGCJobForIndex(stmt scop.StatementForDropJob, tableID descpb.ID, indexID descpb.IndexID)

	// AddNewSchemaChangerJob adds a schema changer job.
	AddNewSchemaChangerJob(
		jobID jobspb.JobID,
		stmts []scpb.Statement,
		isNonCancelable bool,
		auth scpb.Authorization,
		descriptorIDs catalog.DescriptorIDSet,
		runningStatus string,
	) error

	// UpdateSchemaChangerJob will update the progress and payload of the
	// schema changer job.
	UpdateSchemaChangerJob(
		jobID jobspb.JobID,
		isNonCancelable bool,
		runningStatus string,
		descriptorIDsToRemove catalog.DescriptorIDSet,
	) error

	// DeleteSchedule deletes a scheduled job.
	DeleteSchedule(scheduleID jobspb.ScheduleID)

	// RefreshStats refresh stats for a given descriptor.
	RefreshStats(id descpb.ID)

	// AddIndexForMaybeSplitAndScatter splits and scatters rows for a given index,
	// if it's either hash sharded or under the system tenant.
	AddIndexForMaybeSplitAndScatter(
		tableID catid.DescID, indexID catid.IndexID,
	)
}
