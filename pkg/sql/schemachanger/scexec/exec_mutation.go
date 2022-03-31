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
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// executeDescriptorMutationOps will visit each operation, accumulating
// side effects into a mutationVisitorState object, and then writing out
// those side effects using the provided deps.
func executeDescriptorMutationOps(ctx context.Context, deps Dependencies, ops []scop.Op) error {

	mvs := newMutationVisitorState(deps.Catalog())
	v := scmutationexec.NewMutationVisitor(mvs, deps.Catalog(), deps.Catalog(), deps.Clock())
	for _, op := range ops {
		if err := op.(scop.MutationOp).Visit(ctx, v); err != nil {
			return err
		}
	}

	// Note that we perform the catalog writes first in order to acquire locks
	// on the descriptors in question as early as possible. If a restart is
	// encountered, these locks will be retained in subsequent epochs (assuming
	// that the transaction is not aborted due to, say, a deadlock). If we were
	// to lock the eventlog or jobs tables first, they would not provide any
	// liveness benefit because their entries are non-deterministic. The jobs
	// writes are particularly bad because that table is constantly being
	// scanned.
	dbZoneConfigsToDelete, gcJobRecords := mvs.gcJobs.makeRecords(
		deps.TransactionalJobRegistry().MakeJobID,
	)
	if err := performBatchedCatalogWrites(
		ctx,
		mvs.descriptorsToDelete,
		dbZoneConfigsToDelete,
		mvs.checkedOutDescriptors,
		mvs.drainedNames,
		deps.Catalog(),
	); err != nil {
		return err
	}
	if err := logEvents(ctx, mvs, deps.EventLogger()); err != nil {
		return err
	}
	if err := updateDescriptorMetadata(
		ctx, mvs, deps.DescriptorMetadataUpdater(ctx),
	); err != nil {
		return err
	}
	return manageJobs(
		ctx,
		gcJobRecords,
		mvs.schemaChangerJob,
		mvs.schemaChangerJobUpdates,
		deps.TransactionalJobRegistry(),
	)
}

func performBatchedCatalogWrites(
	ctx context.Context,
	descriptorsToDelete catalog.DescriptorIDSet,
	dbZoneConfigsToDelete catalog.DescriptorIDSet,
	checkedOutDescriptors nstree.Map,
	drainedNames map[descpb.ID][]descpb.NameInfo,
	cat Catalog,
) error {
	b := cat.NewCatalogChangeBatcher()
	descriptorsToDelete.ForEach(func(id descpb.ID) {
		checkedOutDescriptors.Remove(id)
	})
	err := checkedOutDescriptors.IterateByID(func(entry catalog.NameEntry) error {
		return b.CreateOrUpdateDescriptor(ctx, entry.(catalog.MutableDescriptor))
	})
	if err != nil {
		return err
	}
	for _, id := range descriptorsToDelete.Ordered() {
		if err := b.DeleteDescriptor(ctx, id); err != nil {
			return err
		}
	}
	for id, drainedNames := range drainedNames {
		for _, name := range drainedNames {
			if err := b.DeleteName(ctx, name, id); err != nil {
				return err
			}
		}
	}
	// Any databases being GCed should have an entry even if none of its tables
	// are being dropped. This entry will be used to generate the GC jobs below.
	{
		var err error
		dbZoneConfigsToDelete.ForEach(func(id descpb.ID) {
			if err == nil {
				err = b.DeleteZoneConfig(ctx, id)
			}
		})
		if err != nil {
			return err
		}
	}

	return b.ValidateAndRun(ctx)
}

func logEvents(ctx context.Context, mvs *mutationVisitorState, el EventLogger) error {
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
			if err := el.LogEvent(ctx, e.id, e.details, e.event); err != nil {
				return err
			}
		}
	}
	return nil
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

// updateDescriptorMetadata performs the portions of the side effects of the
// operations delegated to the DescriptorMetadataUpdater.
func updateDescriptorMetadata(
	ctx context.Context, mvs *mutationVisitorState, m DescriptorMetadataUpdater,
) error {
	for _, comment := range mvs.commentsToUpdate {
		if len(comment.comment) > 0 {
			if err := m.UpsertDescriptorComment(
				comment.id, comment.subID, comment.commentType, comment.comment); err != nil {
				return err
			}
		} else {
			if err := m.DeleteDescriptorComment(
				comment.id, comment.subID, comment.commentType); err != nil {
				return err
			}
		}
	}
	for _, comment := range mvs.constraintCommentsToUpdate {
		if len(comment.comment) > 0 {
			if err := m.UpsertConstraintComment(
				comment.tblID, comment.constraintID, comment.comment); err != nil {
				return err
			}
		} else {
			if err := m.DeleteConstraintComment(
				comment.tblID, comment.constraintID); err != nil {
				return err
			}
		}
	}
	if !mvs.tableCommentsToDelete.Empty() {
		if err := m.DeleteAllCommentsForTables(mvs.tableCommentsToDelete); err != nil {
			return err
		}
	}
	for _, dbRoleSetting := range mvs.databaseRoleSettingsToDelete {
		err := m.DeleteDatabaseRoleSettings(ctx, dbRoleSetting.dbID)
		if err != nil {
			return err
		}
	}
	for _, scheduleID := range mvs.scheduleIDsToDelete {
		if err := m.DeleteSchedule(ctx, scheduleID); err != nil {
			return err
		}
	}
	return nil
}

func manageJobs(
	ctx context.Context,
	gcJobs []jobs.Record,
	scJob *jobs.Record,
	scJobUpdates map[jobspb.JobID]schemaChangerJobUpdate,
	jr TransactionalJobRegistry,
) error {
	// TODO(ajwerner): Batch job creation. Should be easy, the registry has
	// the needed API.
	for _, j := range gcJobs {
		if err := jr.CreateJob(ctx, j); err != nil {
			return err
		}
	}
	if scJob != nil {
		if err := jr.CreateJob(ctx, *scJob); err != nil {
			return err
		}
	}
	for id, update := range scJobUpdates {
		if err := jr.UpdateSchemaChangeJob(ctx, id, func(
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
	return nil
}

type mutationVisitorState struct {
	c                            Catalog
	checkedOutDescriptors        nstree.Map
	drainedNames                 map[descpb.ID][]descpb.NameInfo
	descriptorsToDelete          catalog.DescriptorIDSet
	commentsToUpdate             []commentToUpdate
	tableCommentsToDelete        catalog.DescriptorIDSet
	constraintCommentsToUpdate   []constraintCommentToUpdate
	databaseRoleSettingsToDelete []databaseRoleSettingToDelete
	schemaChangerJob             *jobs.Record
	schemaChangerJobUpdates      map[jobspb.JobID]schemaChangerJobUpdate
	eventsByStatement            map[uint32][]eventPayload
	scheduleIDsToDelete          []int64

	gcJobs
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

func (mvs *mutationVisitorState) DeleteAllTableComments(id descpb.ID) {
	mvs.tableCommentsToDelete.Add(id)
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

func (mvs *mutationVisitorState) AddNewSchemaChangerJob(
	jobID jobspb.JobID,
	stmts []scpb.Statement,
	isNonCancelable bool,
	auth scpb.Authorization,
	descriptorIDs descpb.IDs,
) error {
	if mvs.schemaChangerJob != nil {
		return errors.AssertionFailedf("cannot create more than one new schema change job")
	}
	mvs.schemaChangerJob = MakeDeclarativeSchemaChangeJobRecord(jobID, stmts, isNonCancelable, auth, descriptorIDs)
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
	jobID jobspb.JobID,
	stmts []scpb.Statement,
	isNonCancelable bool,
	auth scpb.Authorization,
	descriptorIDs descpb.IDs,
) *jobs.Record {
	stmtStrs := make([]string, len(stmts))
	for i, stmt := range stmts {
		// Use the redactable string because it's been normalized and
		// fully-qualified. The regular statement is exactly the user input
		// but that's a possibly ambiguous value and not what the old
		// schema changer used. It's probably that the right thing to use
		// is the redactable string with the redaction markers.
		stmtStrs[i] = redact.RedactableString(stmt.RedactedStatement).StripMarkers()
	}
	// The description being all the statements might seem a bit suspect, but
	// it's what the old schema changer does, so it's what we'll do.
	description := strings.Join(stmtStrs, "; ")
	rec := &jobs.Record{
		JobID:         jobID,
		Description:   description,
		Statements:    stmtStrs,
		Username:      security.MakeSQLUsernameFromPreNormalizedString(auth.UserName),
		DescriptorIDs: descriptorIDs,
		Details:       jobspb.NewSchemaChangeDetails{},
		Progress:      jobspb.NewSchemaChangeProgress{},
		// TODO(ajwerner): It'd be good to populate the RunningStatus at all times.
		RunningStatus: "",
		NonCancelable: isNonCancelable,
	}
	return rec
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
