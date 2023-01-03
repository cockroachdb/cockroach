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
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// executeDescriptorMutationOps will visit each operation, accumulating
// side effects into a mutationVisitorState object, and then writing out
// those side effects using the provided deps.
func executeDescriptorMutationOps(ctx context.Context, deps Dependencies, ops []scop.Op) error {

	mvs := newMutationVisitorState(deps.Catalog())
	v := scmutationexec.NewMutationVisitor(mvs, deps.Catalog(), deps.Clock(), deps.Catalog())
	for _, op := range ops {
		if err := op.(scop.MutationOp).Visit(ctx, v); err != nil {
			return errors.Wrapf(err, "%T: %v", op, op)
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
		!deps.TransactionalJobRegistry().UseLegacyGCJob(ctx),
	)
	if err := performBatchedCatalogWrites(
		ctx,
		mvs.descriptorsToDelete,
		dbZoneConfigsToDelete,
		mvs.modifiedDescriptors,
		mvs.drainedNames,
		mvs.commentsToUpdate,
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
	if err := refreshStatsForDescriptors(
		ctx,
		mvs,
		deps.StatsRefresher()); err != nil {
		return err
	}
	if err := maybeSplitAndScatterIndexes(ctx,
		mvs,
		deps.IndexSpanSplitter()); err != nil {
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
	modifiedDescriptors nstree.IDMap,
	drainedNames map[descpb.ID][]descpb.NameInfo,
	commentsToUpdate []commentToUpdate,
	cat Catalog,
) error {
	b := cat.NewCatalogChangeBatcher()
	descriptorsToDelete.ForEach(func(id descpb.ID) {
		modifiedDescriptors.Remove(id)
	})
	err := modifiedDescriptors.Iterate(func(entry catalog.NameEntry) error {
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

	{
		for _, u := range commentsToUpdate {
			if len(u.comment) > 0 {
				if err := b.UpdateComment(ctx, catalogkeys.MakeCommentKey(uint32(u.id), uint32(u.subID), u.commentType), u.comment); err != nil {
					return err
				}
			} else {
				if err := b.DeleteComment(ctx, catalogkeys.MakeCommentKey(uint32(u.id), uint32(u.subID), u.commentType)); err != nil {
					return err
				}
			}
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
			switch e.event.(type) {
			case eventpb.EventWithCommonSQLPayload:
				details := e.details
				details.DescriptorID = uint32(e.id)
				if err := el.LogEvent(ctx, details, e.event); err != nil {
					return err
				}
			case eventpb.EventWithCommonSchemaChangePayload:
				if err := el.LogEventForSchemaChange(ctx, e.event); err != nil {
					return err
				}
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
		case *eventpb.DropView, *eventpb.DropTable, *eventpb.DropIndex:
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
		case *eventpb.DropIndex:
			ev.CascadeDroppedViews = dependentObjects
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

func refreshStatsForDescriptors(
	_ context.Context, mvs *mutationVisitorState, statsRefresher StatsRefreshQueue,
) error {
	for descriptorID := range mvs.statsToRefresh {
		statsRefresher.AddTableForStatsRefresh(descriptorID)
	}
	return nil
}

func maybeSplitAndScatterIndexes(
	ctx context.Context, mvs *mutationVisitorState, splitter IndexSpanSplitter,
) error {
	for _, idx := range mvs.indexesToSplitAndScatter {
		desc, err := mvs.GetDescriptor(ctx, idx.tableID)
		if err != nil {
			return err
		}
		tableDesc := desc.(catalog.TableDescriptor)
		idxDesc, err := catalog.MustFindIndexByID(tableDesc, idx.indexID)
		if err != nil {
			return err
		}
		if err := splitter.MaybeSplitIndexSpans(ctx, tableDesc, idxDesc); err != nil {
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
			md jobs.JobMetadata, updateProgress func(*jobspb.Progress), updatePayload func(*jobspb.Payload),
		) error {
			s := schemaChangeJobUpdateState{md: md}
			defer s.doUpdate(updateProgress, updatePayload)
			s.updatedProgress().RunningStatus = update.runningStatus
			if !md.Payload.Noncancelable && update.isNonCancelable {
				s.updatedPayload().Noncancelable = true
			}
			oldIDs := catalog.MakeDescriptorIDSet(md.Payload.DescriptorIDs...)
			newIDs := oldIDs.Difference(update.descriptorIDsToRemove)
			if newIDs.Len() < oldIDs.Len() {
				s.updatedPayload().DescriptorIDs = newIDs.Ordered()
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// schemaChangeJobUpdateState is a helper struct for managing the state in the
// callback passed to TransactionalJobRegistry.UpdateSchemaChangeJob in
// manageJobs.
type schemaChangeJobUpdateState struct {
	md                   jobs.JobMetadata
	maybeUpdatedPayload  *jobspb.Payload
	maybeUpdatedProgress *jobspb.Progress
}

func (s *schemaChangeJobUpdateState) updatedProgress() *jobspb.Progress {
	if s.maybeUpdatedProgress == nil {
		clone := *s.md.Progress
		s.maybeUpdatedProgress = &clone
	}
	return s.maybeUpdatedProgress
}

func (s *schemaChangeJobUpdateState) updatedPayload() *jobspb.Payload {
	if s.maybeUpdatedPayload == nil {
		clone := *s.md.Payload
		s.maybeUpdatedPayload = &clone
	}
	return s.maybeUpdatedPayload
}

func (s *schemaChangeJobUpdateState) doUpdate(
	updateProgress func(*jobspb.Progress), updatePayload func(*jobspb.Payload),
) {
	if s.maybeUpdatedProgress != nil {
		updateProgress(s.maybeUpdatedProgress)
	}
	if s.maybeUpdatedPayload != nil {
		updatePayload(s.maybeUpdatedPayload)
	}
}

type mutationVisitorState struct {
	c                            Catalog
	modifiedDescriptors          nstree.IDMap
	drainedNames                 map[descpb.ID][]descpb.NameInfo
	descriptorsToDelete          catalog.DescriptorIDSet
	commentsToUpdate             []commentToUpdate
	databaseRoleSettingsToDelete []databaseRoleSettingToDelete
	schemaChangerJob             *jobs.Record
	schemaChangerJobUpdates      map[jobspb.JobID]schemaChangerJobUpdate
	eventsByStatement            map[uint32][]eventPayload
	scheduleIDsToDelete          []int64
	statsToRefresh               map[descpb.ID]struct{}
	indexesToSplitAndScatter     []indexToSplitAndScatter
	gcJobs
}

type indexToSplitAndScatter struct {
	tableID catid.DescID
	indexID catid.IndexID
}

type commentToUpdate struct {
	id          int64
	subID       int64
	commentType catalogkeys.CommentType
	comment     string
}

type databaseRoleSettingToDelete struct {
	dbID catid.DescID
}

type eventPayload struct {
	id descpb.ID
	scpb.TargetMetadata

	details eventpb.CommonSQLEventDetails
	event   logpb.EventPayload
}

type schemaChangerJobUpdate struct {
	isNonCancelable       bool
	runningStatus         string
	descriptorIDsToRemove catalog.DescriptorIDSet
}

func (mvs *mutationVisitorState) UpdateSchemaChangerJob(
	jobID jobspb.JobID,
	isNonCancelable bool,
	runningStatus string,
	descriptorIDsToRemove catalog.DescriptorIDSet,
) error {
	if mvs.schemaChangerJobUpdates == nil {
		mvs.schemaChangerJobUpdates = make(map[jobspb.JobID]schemaChangerJobUpdate)
	} else if _, exists := mvs.schemaChangerJobUpdates[jobID]; exists {
		return errors.AssertionFailedf("cannot update job %d more than once", jobID)
	}
	mvs.schemaChangerJobUpdates[jobID] = schemaChangerJobUpdate{
		isNonCancelable:       isNonCancelable,
		runningStatus:         runningStatus,
		descriptorIDsToRemove: descriptorIDsToRemove,
	}
	return nil
}

func newMutationVisitorState(c Catalog) *mutationVisitorState {
	return &mutationVisitorState{
		c:                 c,
		drainedNames:      make(map[descpb.ID][]descpb.NameInfo),
		eventsByStatement: make(map[uint32][]eventPayload),
		statsToRefresh:    make(map[descpb.ID]struct{}),
	}
}

var _ scmutationexec.MutationVisitorStateUpdater = (*mutationVisitorState)(nil)

func (mvs *mutationVisitorState) GetDescriptor(
	ctx context.Context, id descpb.ID,
) (catalog.Descriptor, error) {
	if entry := mvs.modifiedDescriptors.Get(id); entry != nil {
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
	entry := mvs.modifiedDescriptors.Get(id)
	if entry != nil {
		return entry.(catalog.MutableDescriptor), nil
	}
	mut, err := mvs.c.MustReadMutableDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	mut.MaybeIncrementVersion()
	mvs.modifiedDescriptors.Upsert(mut)
	return mut, nil
}

func (mvs *mutationVisitorState) DeleteDescriptor(id descpb.ID) {
	mvs.descriptorsToDelete.Add(id)
}

func (mvs *mutationVisitorState) AddComment(
	id descpb.ID, subID int, commentType catalogkeys.CommentType, comment string,
) {
	mvs.commentsToUpdate = append(mvs.commentsToUpdate,
		commentToUpdate{
			id:          int64(id),
			subID:       int64(subID),
			commentType: commentType,
			comment:     comment,
		})
}

func (mvs *mutationVisitorState) DeleteComment(
	id descpb.ID, subID int, commentType catalogkeys.CommentType,
) {
	mvs.commentsToUpdate = append(mvs.commentsToUpdate,
		commentToUpdate{
			id:          int64(id),
			subID:       int64(subID),
			commentType: commentType,
		})
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

func (mvs *mutationVisitorState) RefreshStats(descriptorID descpb.ID) {
	mvs.statsToRefresh[descriptorID] = struct{}{}
}

func (mvs *mutationVisitorState) AddDrainedName(id descpb.ID, nameInfo descpb.NameInfo) {
	mvs.drainedNames[id] = append(mvs.drainedNames[id], nameInfo)
}

func (mvs *mutationVisitorState) AddNewSchemaChangerJob(
	jobID jobspb.JobID,
	stmts []scpb.Statement,
	isNonCancelable bool,
	auth scpb.Authorization,
	descriptorIDs catalog.DescriptorIDSet,
	runningStatus string,
) error {
	if mvs.schemaChangerJob != nil {
		return errors.AssertionFailedf("cannot create more than one new schema change job")
	}
	mvs.schemaChangerJob = MakeDeclarativeSchemaChangeJobRecord(
		jobID,
		stmts,
		isNonCancelable,
		auth,
		descriptorIDs,
		runningStatus,
	)
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
	descriptorIDs catalog.DescriptorIDSet,
	runningStatus string,
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
		Username:      username.MakeSQLUsernameFromPreNormalizedString(auth.UserName),
		DescriptorIDs: descriptorIDs.Ordered(),
		Details:       jobspb.NewSchemaChangeDetails{},
		Progress:      jobspb.NewSchemaChangeProgress{},
		RunningStatus: jobs.RunningStatus(runningStatus),
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
	event logpb.EventPayload,
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

// AddIndexForMaybeSplitAndScatter implements the scmutationexec.MutationVisitorStateUpdater
// // interface.
func (mvs *mutationVisitorState) AddIndexForMaybeSplitAndScatter(
	tableID catid.DescID, indexID catid.IndexID,
) {
	mvs.indexesToSplitAndScatter = append(mvs.indexesToSplitAndScatter,
		indexToSplitAndScatter{
			tableID: tableID,
			indexID: indexID,
		})
}
