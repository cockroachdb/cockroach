// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scmutationexec

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
)

// NameResolver is used to retrieve fully qualified names from the catalog.
type NameResolver interface {

	// GetFullyQualifiedName gets the fully qualified name from a descriptor ID.
	GetFullyQualifiedName(ctx context.Context, id descpb.ID) (string, error)
}

// Clock is used to provide a timestamp to track loosely when something
// happened. It can be used for things like observability and telemetry and
// not for anything involving correctness.
type Clock interface {

	// ApproximateTime provides a present timestamp.
	ApproximateTime() time.Time
}

// SyntheticDescriptorStateUpdater is used to update the synthetic descriptor
// state. This state is not visible to the operations in the declarative schema
// changer execution layer. Its only purpose is to ensure that subsequent
// queries in the same transaction as a schema change statement behave as if the
// schema change had already completed.
type SyntheticDescriptorStateUpdater interface {

	// AddSyntheticDescriptor sets a synthetic descriptor to shadow any existing
	// descriptor with the same name or the same ID for the remainder of the
	// current transaction.
	AddSyntheticDescriptor(desc catalog.Descriptor)
}

// MutationVisitorStateUpdater is the interface for updating the visitor state.
type MutationVisitorStateUpdater interface {

	// GetDescriptor returns a checked-out descriptor, or reads a descriptor from
	// the catalog by ID if it hasn't been checked out yet.
	GetDescriptor(ctx context.Context, id descpb.ID) (catalog.Descriptor, error)

	// CheckOutDescriptor reads a descriptor from the catalog by ID and marks it
	// as undergoing a change.
	CheckOutDescriptor(ctx context.Context, id descpb.ID) (catalog.MutableDescriptor, error)

	// AddDrainedName marks a namespace entry as being drained.
	AddDrainedName(id descpb.ID, nameInfo descpb.NameInfo)

	// DeleteDescriptor adds a descriptor for deletion.
	DeleteDescriptor(id descpb.ID)

	// DeleteComment removes comments for a descriptor
	DeleteComment(id descpb.ID, subID int, commentType catalogkeys.CommentType)

	// AddComment adds comments for a descriptor
	AddComment(id descpb.ID, subID int, commentType catalogkeys.CommentType, comment string)

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

	// EnqueueEvent will enqueue an event to be written to the event log.
	EnqueueEvent(
		id descpb.ID,
		metadata scpb.TargetMetadata,
		details eventpb.CommonSQLEventDetails,
		event logpb.EventPayload,
	) error

	// DeleteSchedule deletes a scheduled job.
	DeleteSchedule(scheduleID int64)

	// RefreshStats refresh stats for a given descriptor.
	RefreshStats(id descpb.ID)
}
