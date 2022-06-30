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

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/regionutils"
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

	mvs := newMutationVisitorState(deps.Catalog(), deps.ZoneConfigReaderForExec())
	v := scmutationexec.NewMutationVisitor(mvs, deps.Catalog(), deps.Clock())
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
	)
	if err := performBatchedCatalogWrites(
		ctx,
		mvs.descriptorsToDelete,
		dbZoneConfigsToDelete,
		mvs.modifiedDescriptors,
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
	if err := refreshStatsForDescriptors(
		ctx,
		mvs,
		deps.StatsRefresher()); err != nil {
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
	modifiedDescriptors nstree.Map,
	drainedNames map[descpb.ID][]descpb.NameInfo,
	cat Catalog,
) error {
	b := cat.NewCatalogChangeBatcher()
	descriptorsToDelete.ForEach(func(id descpb.ID) {
		modifiedDescriptors.Remove(id)
	})
	err := modifiedDescriptors.IterateByID(func(entry catalog.NameEntry) error {
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
			switch e.event.(type) {
			case eventpb.EventWithCommonSQLPayload:
				if err := el.LogEvent(ctx, e.id, e.details, e.event); err != nil {
					return err
				}
			case eventpb.EventWithCommonSchemaChangePayload:
				if err := el.LogEventForSchemaChange(ctx, e.id, e.event); err != nil {
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
	for id, zoneCfg := range mvs.zoneConfigsToUpdate {
		if zoneCfg != nil {
			if _, err := m.UpsertZoneConfig(ctx, id, zoneCfg, true /*regenrateSpans*/); err != nil {
				return err
			}
		} else {
			if _, err := m.DeleteZoneConfig(ctx, id); err != nil {
				return err
			}
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
			progress.RunningStatus = update.runningStatus
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
	zoneConfigReader             scmutationexec.ZoneConfigReader
	modifiedDescriptors          nstree.Map
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
	statsToRefresh               map[descpb.ID]struct{}
	zoneConfigsToUpdate          map[descpb.ID]*zonepb.ZoneConfig
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
	runningStatus   string
}

func (mvs *mutationVisitorState) UpdateSchemaChangerJob(
	jobID jobspb.JobID, isNonCancelable bool, runningStatus string,
) error {
	if mvs.schemaChangerJobUpdates == nil {
		mvs.schemaChangerJobUpdates = make(map[jobspb.JobID]schemaChangerJobUpdate)
	} else if _, exists := mvs.schemaChangerJobUpdates[jobID]; exists {
		return errors.AssertionFailedf("cannot update job %d more than once", jobID)
	}
	mvs.schemaChangerJobUpdates[jobID] = schemaChangerJobUpdate{
		isNonCancelable: isNonCancelable,
		runningStatus:   runningStatus,
	}
	return nil
}

func newMutationVisitorState(
	c Catalog, zoneConfigReader scmutationexec.ZoneConfigReader,
) *mutationVisitorState {
	return &mutationVisitorState{
		c:                   c,
		zoneConfigReader:    zoneConfigReader,
		drainedNames:        make(map[descpb.ID][]descpb.NameInfo),
		eventsByStatement:   make(map[uint32][]eventPayload),
		statsToRefresh:      make(map[descpb.ID]struct{}),
		zoneConfigsToUpdate: make(map[descpb.ID]*zonepb.ZoneConfig),
	}
}

var _ scmutationexec.MutationVisitorStateUpdater = (*mutationVisitorState)(nil)

func (mvs *mutationVisitorState) GetDescriptor(
	ctx context.Context, id descpb.ID,
) (catalog.Descriptor, error) {
	if entry := mvs.modifiedDescriptors.GetByID(id); entry != nil {
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
	entry := mvs.modifiedDescriptors.GetByID(id)
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

func (mvs *mutationVisitorState) DeleteAllTableComments(id descpb.ID) {
	mvs.tableCommentsToDelete.Add(id)
}

func (mvs *mutationVisitorState) AddComment(
	id descpb.ID, subID int, commentType keys.CommentType, comment string,
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
	descriptorIDs descpb.IDs,
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
	descriptorIDs descpb.IDs,
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
		DescriptorIDs: descriptorIDs,
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

// getOrAddZoneConfig reads the existing zone config for a given descriptor,
// ID if one exists, otherwise a nil one is stored.
func (mvs *mutationVisitorState) getOrAddZoneConfig(
	ctx context.Context, id descpb.ID,
) (*zonepb.ZoneConfig, error) {
	if zc := mvs.zoneConfigsToUpdate[id]; zc != nil {
		return zc, nil
	}
	zoneConfig, err := mvs.zoneConfigReader.GetZoneConfig(ctx, id)
	if err != nil {
		return nil, err
	}
	if zoneConfig == nil {
		zoneConfig = zonepb.NewZoneConfig()
	}
	mvs.zoneConfigsToUpdate[id] = zoneConfig
	return mvs.zoneConfigsToUpdate[id], nil
}

// synthesizeMultiRegionConfig synthesizes the multi region config for a given
// database descriptor.
func (mvs *mutationVisitorState) synthesizeMultiRegionConfig(
	ctx context.Context, dbID descpb.ID,
) (multiregion.RegionConfig, error) {
	regionConfig := multiregion.RegionConfig{}
	desc, err := mvs.GetDescriptor(ctx, dbID)
	if err != nil {
		return multiregion.RegionConfig{}, err
	}
	dbDesc := desc.(catalog.DatabaseDescriptor)
	dbRegionConfig := dbDesc.GetRegionConfig()
	desc, err = mvs.GetDescriptor(ctx, dbRegionConfig.RegionEnumID)
	if err != nil {
		return multiregion.RegionConfig{}, err
	}
	regionEnum := desc.(catalog.TypeDescriptor)
	regionNames, err := regionEnum.RegionNames()
	if err != nil {
		return multiregion.RegionConfig{}, err
	}
	superRegions, err := regionEnum.SuperRegions()
	if err != nil {
		return multiregion.RegionConfig{}, err
	}
	zoneCfgExtensions, err := regionEnum.ZoneConfigExtensions()
	if err != nil {
		return regionConfig, err
	}

	regionConfig = multiregion.MakeRegionConfig(
		regionNames,
		dbRegionConfig.PrimaryRegion,
		dbRegionConfig.SurvivalGoal,
		dbRegionConfig.RegionEnumID,
		dbRegionConfig.Placement,
		superRegions,
		zoneCfgExtensions,
	)

	if err := multiregion.ValidateRegionConfig(regionConfig); err != nil {
		panic(err)
	}
	return regionConfig, nil
}

// At the table/partition level, the only attributes that are set are
// `num_voters`, `voter_constraints`, and `lease_preferences`. We expect that
// the attributes `num_replicas` and `constraints` will be inherited from the
// database level zone config.
func zoneConfigForMultiRegionPartition(
	partitionRegion catpb.RegionName, regionConfig multiregion.RegionConfig,
) (zonepb.ZoneConfig, error) {
	zc := *zonepb.NewZoneConfig()

	numVoters, numReplicas := regionutils.GetNumVotersAndNumReplicas(regionConfig)
	zc.NumVoters = &numVoters

	if regionConfig.IsMemberOfExplicitSuperRegion(partitionRegion) {
		err := regionutils.AddConstraintsForSuperRegion(&zc, regionConfig, partitionRegion)
		if err != nil {
			return zonepb.ZoneConfig{}, err
		}
	} else if !regionConfig.RegionalInTablesInheritDatabaseConstraints(partitionRegion) {
		// If the database constraints can't be inherited to serve as the
		// constraints for this partition, define the constraints ourselves.
		zc.NumReplicas = &numReplicas

		constraints, err := regionutils.SynthesizeReplicaConstraints(regionConfig.Regions(), regionConfig.Placement())
		if err != nil {
			return zonepb.ZoneConfig{}, err
		}
		zc.Constraints = constraints
		zc.InheritedConstraints = false
	}

	voterConstraints, err := regionutils.SynthesizeVoterConstraints(partitionRegion, regionConfig)
	if err != nil {
		return zonepb.ZoneConfig{}, err
	}
	zc.VoterConstraints = voterConstraints
	zc.NullVoterConstraintsIsEmpty = true

	leasePreferences := regionutils.SynthesizeLeasePreferences(partitionRegion)
	zc.LeasePreferences = leasePreferences
	zc.InheritedLeasePreferences = false

	zc = regionConfig.ApplyZoneConfigExtensionForRegionalIn(zc, partitionRegion)
	return zc, err
}

// AddSubZoneConfig implements the scmutationexec.MutationVisitorStateUpdater
// interface.
func (mvs *mutationVisitorState) AddSubZoneConfig(
	ctx context.Context, tbl catalog.TableDescriptor, config *zonepb.Subzone,
) error {
	zc, err := mvs.getOrAddZoneConfig(ctx, tbl.GetID())
	if err != nil {
		return err
	}
	// Compute the zone config for the multiregion config.
	multiRegionCfg, err := mvs.synthesizeMultiRegionConfig(ctx, tbl.GetParentID())
	if err != nil {
		return err
	}
	config.Config, err = zoneConfigForMultiRegionPartition(catpb.RegionName(config.PartitionName), multiRegionCfg)
	if err != nil {
		return err
	}
	zc.Subzones = append(zc.Subzones, *config)
	return nil
}

// RemoveSubZoneConfig implements the scmutationexec.MutationVisitorStateUpdater
// interface.
func (mvs *mutationVisitorState) RemoveSubZoneConfig(
	ctx context.Context, tbl catalog.TableDescriptor, config *zonepb.Subzone,
) error {
	zc, err := mvs.getOrAddZoneConfig(ctx, tbl.GetID())
	if err != nil {
		return err
	}
	if zc == nil {
		return nil
	}
	for idx, subZone := range zc.Subzones {
		if config.IndexID == subZone.IndexID &&
			config.PartitionName == subZone.PartitionName {
			newSubZoneSpans := make([]zonepb.SubzoneSpan, 0, len(zc.SubzoneSpans))
			for _, subZoneSpan := range zc.SubzoneSpans {
				appendSpan := subZoneSpan.SubzoneIndex != int32(idx)
				if subZoneSpan.SubzoneIndex > int32(idx) {
					subZoneSpan.SubzoneIndex = subZoneSpan.SubzoneIndex - 1
				}
				if appendSpan {
					newSubZoneSpans = append(newSubZoneSpans, subZoneSpan)
				}
			}
			zc.SubzoneSpans = newSubZoneSpans
			zc.Subzones = append(zc.Subzones[:idx], zc.Subzones[idx+1:]...)
			break
		}
	}
	return nil
}
