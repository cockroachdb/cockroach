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
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func executeDescriptorMutationOps(ctx context.Context, deps Dependencies, ops []scop.Op) error {
	mvs := newMutationVisitorState(deps.Catalog())
	v := scmutationexec.NewMutationVisitor(mvs, deps.Catalog(), deps.Catalog(), deps.Clock())
	for _, op := range ops {
		if err := op.(scop.MutationOp).Visit(ctx, v); err != nil {
			return err
		}
	}
	b := deps.Catalog().NewCatalogChangeBatcher()
	mvs.descriptorsToDelete.ForEach(func(id descpb.ID) {
		mvs.checkedOutDescriptors.Remove(id)
	})
	err := mvs.checkedOutDescriptors.IterateByID(func(entry catalog.NameEntry) error {
		return b.CreateOrUpdateDescriptor(ctx, entry.(catalog.MutableDescriptor))
	})
	if err != nil {
		return err
	}
	for id, drainedNames := range mvs.drainedNames {
		for _, name := range drainedNames {
			if err := b.DeleteName(ctx, name, id); err != nil {
				return err
			}
		}
	}
	// Any databases being GCed should have an entry even if none of its tables
	// are being dropped. This entry will be used to generate the GC jobs below.
	for _, dbID := range mvs.dbGCJobs.Ordered() {
		if _, ok := mvs.descriptorGCJobs[dbID]; !ok {
			// Zone config should now be safe to remove versus waiting for the GC job.
			if err := b.DeleteZoneConfig(ctx, dbID); err != nil {
				return err
			}
		}
	}
	var dbIDs catalog.DescriptorIDSet
	for dbID := range mvs.descriptorGCJobs {
		dbIDs.Add(dbID)
	}
	for _, dbID := range dbIDs.Ordered() {
		job := jobspb.SchemaChangeGCDetails{
			Tables: mvs.descriptorGCJobs[dbID],
		}
		// Check if the database is also being cleaned up at the same time.
		if mvs.dbGCJobs.Contains(dbID) {
			job.ParentID = dbID
		}
		jobName := func() string {
			var ids catalog.DescriptorIDSet
			if job.ParentID != descpb.InvalidID {
				ids.Add(job.ParentID)
			}
			for _, table := range mvs.descriptorGCJobs[dbID] {
				ids.Add(table.ID)
			}
			var sb strings.Builder
			if ids.Len() == 1 {
				sb.WriteString("dropping descriptor")
			} else {
				sb.WriteString("dropping descriptors")
			}
			ids.ForEach(func(id descpb.ID) {
				sb.WriteString(fmt.Sprintf(" %d", id))
			})
			return sb.String()
		}

		record := createGCJobRecord(jobName(), security.NodeUserName(), job)
		record.JobID = deps.TransactionalJobRegistry().MakeJobID()
		if err := deps.TransactionalJobRegistry().CreateJob(ctx, record); err != nil {
			return err
		}
	}
	for tableID, indexes := range mvs.indexGCJobs {
		job := jobspb.SchemaChangeGCDetails{
			ParentID: tableID,
			Indexes:  indexes,
		}
		jobName := func() string {
			if len(indexes) == 1 {
				return fmt.Sprintf("dropping table %d index %d", tableID, indexes[0].IndexID)
			}
			var sb strings.Builder
			sb.WriteString(fmt.Sprintf("dropping table %d indexes", tableID))
			for _, index := range indexes {
				sb.WriteString(fmt.Sprintf(" %d", index.IndexID))
			}
			return sb.String()
		}

		record := createGCJobRecord(jobName(), security.NodeUserName(), job)
		record.JobID = deps.TransactionalJobRegistry().MakeJobID()
		if err := deps.TransactionalJobRegistry().CreateJob(ctx, record); err != nil {
			return err
		}
	}
	if mvs.schemaChangerJob != nil {
		if err := deps.TransactionalJobRegistry().CreateJob(ctx, *mvs.schemaChangerJob); err != nil {
			return err
		}
	}

	statementIDs := make([]uint32, 0, len(mvs.eventsByStatement))
	for statementID := range mvs.eventsByStatement {
		statementIDs = append(statementIDs, statementID)
	}
	sort.Slice(statementIDs, func(i, j int) bool {
		return statementIDs[i] < statementIDs[j]
	})
	for _, statementID := range statementIDs {
		entries := eventLogEntriesForStatement(mvs.eventsByStatement[statementID])
		for _, e := range entries {
			// TODO(postamar): batch these
			if err := deps.EventLogger().LogEvent(ctx, e.id, e.details, e.event); err != nil {
				return err
			}
		}
	}
	metadataUpdater := deps.DescriptorMetadataUpdater(ctx)
	for _, comment := range mvs.commentsToUpdate {
		if len(comment.comment) > 0 {
			if err := metadataUpdater.UpsertDescriptorComment(
				comment.id, comment.subID, comment.commentType, comment.comment); err != nil {
				return err
			}
		} else {
			if err := metadataUpdater.DeleteDescriptorComment(
				comment.id, comment.subID, comment.commentType); err != nil {
				return err
			}
		}
	}
	for _, comment := range mvs.constraintCommentsToUpdate {
		if len(comment.comment) > 0 {
			if err := metadataUpdater.UpsertConstraintComment(
				comment.tblID, comment.constraintID, comment.comment); err != nil {
				return err
			}
		} else {
			if err := metadataUpdater.DeleteConstraintComment(
				comment.tblID, comment.constraintID); err != nil {
				return err
			}
		}
	}
	for _, dbRoleSetting := range mvs.databaseRoleSettingsToDelete {
		err := metadataUpdater.DeleteDatabaseRoleSettings(ctx, dbRoleSetting.dbID)
		if err != nil {
			return err
		}
	}
	for _, id := range mvs.descriptorsToDelete.Ordered() {
		if err := b.DeleteDescriptor(ctx, id); err != nil {
			return err
		}
	}
	for id, update := range mvs.schemaChangerJobUpdates {
		if err := deps.TransactionalJobRegistry().UpdateSchemaChangeJob(ctx, id, func(
			md jobs.JobMetadata, updateProgress func(*jobspb.Progress), setNonCancelable func(),
		) error {
			progress := *md.Progress
			updateProgress(&progress)
			if !md.Payload.Noncancelable && update.isNonCancelable {
				setNonCancelable()
			}
			return nil
		}); err != nil {
			return err
		}
	}
	for _, scheduleID := range mvs.scheduleIDsToDelete {
		if err := metadataUpdater.DeleteSchedule(ctx, scheduleID); err != nil {
			return err
		}
	}
	return b.ValidateAndRun(ctx)
}

func eventLogEntriesForStatement(statementEvents []eventPayload) (logEntries []eventPayload) {
	// A dependent event is one which is generated because of a
	// dependency getting modified from the source object. An example
	// of this is a DROP TABLE will be the source event, which will track
	// any dependent views dropped.
	var dependentEvents = make(map[uint32][]eventPayload)
	var sourceEvents = make(map[uint32]eventPayload)
	// First separate out events, where the first event generated will always
	// be the source and everything else before will be dependencies if they have
	// the same subtask ID.
	for _, event := range statementEvents {
		dependentEvents[event.SubWorkID] = append(dependentEvents[event.SubWorkID], event)
	}
	// Split of the source events.
	orderedSubWorkID := make([]uint32, 0, len(dependentEvents))
	for subWorkID := range dependentEvents {
		elems := dependentEvents[subWorkID]
		sort.SliceStable(elems, func(i, j int) bool {
			return elems[i].SourceElementID < elems[j].SourceElementID
		})
		sourceEvents[subWorkID] = elems[0]
		dependentEvents[subWorkID] = elems[1:]
		orderedSubWorkID = append(orderedSubWorkID, subWorkID)
	}
	// Store an ordered list of sub-work IDs for deterministic
	// event order.
	sort.SliceStable(orderedSubWorkID, func(i, j int) bool {
		return orderedSubWorkID[i] < orderedSubWorkID[j]
	})
	// Collect the dependent objects for each
	// source event, and generate an event log entry.
	for _, subWorkID := range orderedSubWorkID {
		// Determine which objects we should collect.
		collectDependentViewNames := false
		collectDependentTables := false
		collectDependentSequences := false
		sourceEvent := sourceEvents[subWorkID]
		switch sourceEvent.event.(type) {
		case *eventpb.DropDatabase:
			// Log each of the objects that are dropped.
			collectDependentViewNames = true
			collectDependentTables = true
			collectDependentSequences = true
		case *eventpb.DropView, *eventpb.DropTable:
			// Drop view and drop tables only cares about
			// dependent views
			collectDependentViewNames = true
		}
		var dependentObjects []string
		for _, dependentEvent := range dependentEvents[subWorkID] {
			switch ev := dependentEvent.event.(type) {
			case *eventpb.DropSequence:
				if collectDependentSequences {
					dependentObjects = append(dependentObjects, ev.SequenceName)
				}
			case *eventpb.DropTable:
				if collectDependentTables {
					dependentObjects = append(dependentObjects, ev.TableName)
				}
			case *eventpb.DropView:
				if collectDependentViewNames {
					dependentObjects = append(dependentObjects, ev.ViewName)
				}
			}
		}
		// Add anything that we determined based
		// on the dependencies.
		switch ev := sourceEvent.event.(type) {
		case *eventpb.DropTable:
			ev.CascadeDroppedViews = dependentObjects
		case *eventpb.DropView:
			ev.CascadeDroppedViews = dependentObjects
		case *eventpb.DropDatabase:
			ev.DroppedSchemaObjects = dependentObjects
		}
		// Generate event log entries for the source event only. The dependent
		// events will be ignored.
		logEntries = append(logEntries, sourceEvent)
	}
	return logEntries
}

type mutationVisitorState struct {
	c                            Catalog
	checkedOutDescriptors        nstree.Map
	drainedNames                 map[descpb.ID][]descpb.NameInfo
	descriptorsToDelete          catalog.DescriptorIDSet
	commentsToUpdate             []commentToUpdate
	constraintCommentsToUpdate   []constraintCommentToUpdate
	databaseRoleSettingsToDelete []databaseRoleSettingToDelete
	dbGCJobs                     catalog.DescriptorIDSet
	descriptorGCJobs             map[descpb.ID][]jobspb.SchemaChangeGCDetails_DroppedID
	indexGCJobs                  map[descpb.ID][]jobspb.SchemaChangeGCDetails_DroppedIndex
	schemaChangerJob             *jobs.Record
	schemaChangerJobUpdates      map[jobspb.JobID]schemaChangerJobUpdate
	eventsByStatement            map[uint32][]eventPayload
	scheduleIDsToDelete          []int64
}

type constraintCommentToUpdate struct {
	tblID        catid.DescID
	constraintID descpb.ConstraintID
	comment      string
}

type commentToUpdate struct {
	id          int64
	subID       int64
	commentType keys.CommentType
	comment     string
}

type databaseRoleSettingToDelete struct {
	dbID catid.DescID
}

type eventPayload struct {
	id descpb.ID
	scpb.TargetMetadata

	details eventpb.CommonSQLEventDetails
	event   eventpb.EventPayload
}

type schemaChangerJobUpdate struct {
	isNonCancelable bool
}

func (mvs *mutationVisitorState) UpdateSchemaChangerJob(
	jobID jobspb.JobID, isNonCancelable bool,
) error {
	if mvs.schemaChangerJobUpdates == nil {
		mvs.schemaChangerJobUpdates = make(map[jobspb.JobID]schemaChangerJobUpdate)
	} else if _, exists := mvs.schemaChangerJobUpdates[jobID]; exists {
		return errors.AssertionFailedf("cannot update job %d more than once", jobID)
	}
	mvs.schemaChangerJobUpdates[jobID] = schemaChangerJobUpdate{
		isNonCancelable: isNonCancelable,
	}
	return nil
}

func newMutationVisitorState(c Catalog) *mutationVisitorState {
	return &mutationVisitorState{
		c:                 c,
		drainedNames:      make(map[descpb.ID][]descpb.NameInfo),
		indexGCJobs:       make(map[descpb.ID][]jobspb.SchemaChangeGCDetails_DroppedIndex),
		descriptorGCJobs:  make(map[descpb.ID][]jobspb.SchemaChangeGCDetails_DroppedID),
		eventsByStatement: make(map[uint32][]eventPayload),
	}
}

var _ scmutationexec.MutationVisitorStateUpdater = (*mutationVisitorState)(nil)

func (mvs *mutationVisitorState) GetDescriptor(
	ctx context.Context, id descpb.ID,
) (catalog.Descriptor, error) {
	if entry := mvs.checkedOutDescriptors.GetByID(id); entry != nil {
		return entry.(catalog.Descriptor), nil
	}
	descs, err := mvs.c.MustReadImmutableDescriptors(ctx, id)
	if err != nil {
		return nil, err
	}
	return descs[0], nil
}

func (mvs *mutationVisitorState) CheckOutDescriptor(
	ctx context.Context, id descpb.ID,
) (catalog.MutableDescriptor, error) {
	entry := mvs.checkedOutDescriptors.GetByID(id)
	if entry != nil {
		return entry.(catalog.MutableDescriptor), nil
	}
	mut, err := mvs.c.MustReadMutableDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	mut.MaybeIncrementVersion()
	mvs.checkedOutDescriptors.Upsert(mut)
	return mut, nil
}

func (mvs *mutationVisitorState) MaybeCheckedOutDescriptor(id descpb.ID) catalog.Descriptor {
	entry := mvs.checkedOutDescriptors.GetByID(id)
	if entry == nil {
		return nil
	}
	return entry.(catalog.Descriptor)
}

func (mvs *mutationVisitorState) DeleteDescriptor(id descpb.ID) {
	mvs.descriptorsToDelete.Add(id)
}

func (mvs *mutationVisitorState) DeleteComment(
	id descpb.ID, subID int, commentType keys.CommentType,
) {
	mvs.commentsToUpdate = append(mvs.commentsToUpdate,
		commentToUpdate{
			id:          int64(id),
			subID:       int64(subID),
			commentType: commentType,
		})
}

func (mvs *mutationVisitorState) DeleteConstraintComment(
	ctx context.Context, tblID descpb.ID, constraintID descpb.ConstraintID,
) error {
	mvs.constraintCommentsToUpdate = append(mvs.constraintCommentsToUpdate,
		constraintCommentToUpdate{
			tblID:        tblID,
			constraintID: constraintID,
		})
	return nil
}

func (mvs *mutationVisitorState) DeleteDatabaseRoleSettings(
	ctx context.Context, dbID descpb.ID,
) error {
	mvs.databaseRoleSettingsToDelete = append(mvs.databaseRoleSettingsToDelete,
		databaseRoleSettingToDelete{
			dbID: dbID,
		})
	return nil
}

func (mvs *mutationVisitorState) DeleteSchedule(scheduleID int64) {
	mvs.scheduleIDsToDelete = append(mvs.scheduleIDsToDelete, scheduleID)
}

func (mvs *mutationVisitorState) AddDrainedName(id descpb.ID, nameInfo descpb.NameInfo) {
	mvs.drainedNames[id] = append(mvs.drainedNames[id], nameInfo)
}

func (mvs *mutationVisitorState) AddNewGCJobForTable(table catalog.TableDescriptor) {
	mvs.descriptorGCJobs[table.GetParentID()] = append(mvs.descriptorGCJobs[table.GetParentID()],
		jobspb.SchemaChangeGCDetails_DroppedID{
			ID:       table.GetID(),
			DropTime: timeutil.Now().UnixNano(),
		})
}

func (mvs *mutationVisitorState) AddNewGCJobForDatabase(db catalog.DatabaseDescriptor) {
	mvs.dbGCJobs.Add(db.GetID())
}

func (mvs *mutationVisitorState) AddNewGCJobForIndex(
	tbl catalog.TableDescriptor, index catalog.Index,
) {
	mvs.indexGCJobs[tbl.GetID()] = append(
		mvs.indexGCJobs[tbl.GetID()],
		jobspb.SchemaChangeGCDetails_DroppedIndex{
			IndexID:  index.GetID(),
			DropTime: timeutil.Now().UnixNano(),
		})
}

func (mvs *mutationVisitorState) AddNewSchemaChangerJob(
	jobID jobspb.JobID, stmts []scpb.Statement, auth scpb.Authorization, descriptorIDs descpb.IDs,
) error {
	if mvs.schemaChangerJob != nil {
		return errors.AssertionFailedf("cannot create more than one new schema change job")
	}
	mvs.schemaChangerJob = MakeDeclarativeSchemaChangeJobRecord(jobID, stmts, auth, descriptorIDs)
	return nil
}

// MakeDeclarativeSchemaChangeJobRecord is used to construct a declarative
// schema change job. The state of the schema change is stored in the descriptors
// themselves rather than the job state. During execution, the only state which
// is stored in the job itself pertains to backfill progress.
//
// Note that there's no way to construct a job in the reverting state. If the
// state of the schema change according to the descriptors is InRollback, then
// at the outset of the job, an error will be returned to move the job into
// the reverting state.
func MakeDeclarativeSchemaChangeJobRecord(
	jobID jobspb.JobID, stmts []scpb.Statement, auth scpb.Authorization, descriptorIDs descpb.IDs,
) *jobs.Record {
	stmtStrs := make([]string, len(stmts))
	for i, stmt := range stmts {
		stmtStrs[i] = stmt.Statement
	}
	rec := &jobs.Record{
		JobID:       jobID,
		Description: "schema change job", // TODO(ajwerner): use const
		Statements:  stmtStrs,
		Username:    security.MakeSQLUsernameFromPreNormalizedString(auth.UserName),
		// TODO(ajwerner): It may be better in the future to have the builder be
		// responsible for determining this set of descriptorIDs. As of the time of
		// writing, the descriptorIDs to be "locked," descriptorIDs that need schema
		// change jobs, and descriptorIDs with schema change mutations all coincide.
		// But there are future schema changes to be implemented in the new schema
		// changer (e.g., RENAME TABLE) for which this may no longer be true.
		DescriptorIDs: descriptorIDs,
		Details:       jobspb.NewSchemaChangeDetails{},
		Progress:      jobspb.NewSchemaChangeProgress{},

		// TODO(ajwerner): It'd be good to populate the RunningStatus at all times.
		RunningStatus: "",
		NonCancelable: false, // TODO(ajwerner): Set this appropriately
	}
	return rec
}

// createGCJobRecord creates the job record for a GC job, setting some
// properties which are common for all GC jobs.
func createGCJobRecord(
	originalDescription string, username security.SQLUsername, details jobspb.SchemaChangeGCDetails,
) jobs.Record {
	descriptorIDs := make([]descpb.ID, 0)
	if len(details.Indexes) > 0 {
		if len(descriptorIDs) == 0 {
			descriptorIDs = []descpb.ID{details.ParentID}
		}
	} else {
		for _, table := range details.Tables {
			descriptorIDs = append(descriptorIDs, table.ID)
		}
	}
	return jobs.Record{
		Description:   fmt.Sprintf("GC for %s", originalDescription),
		Username:      username,
		DescriptorIDs: descriptorIDs,
		Details:       details,
		Progress:      jobspb.SchemaChangeGCProgress{},
		RunningStatus: "waiting for GC TTL",
		NonCancelable: true,
	}
}

// EnqueueEvent implements the scmutationexec.MutationVisitorStateUpdater
// interface.
func (mvs *mutationVisitorState) EnqueueEvent(
	id descpb.ID,
	metadata scpb.TargetMetadata,
	details eventpb.CommonSQLEventDetails,
	event eventpb.EventPayload,
) error {
	mvs.eventsByStatement[metadata.StatementID] = append(
		mvs.eventsByStatement[metadata.StatementID],
		eventPayload{
			id:             id,
			event:          event,
			TargetMetadata: metadata,
			details:        details,
		},
	)
	return nil
}
