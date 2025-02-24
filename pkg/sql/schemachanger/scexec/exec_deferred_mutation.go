// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scexec

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/errors"
)

type deferredState struct {
	databaseRoleSettingsToDelete []databaseRoleSettingToDelete
	schemaChangerJob             *jobs.Record
	schemaChangerJobUpdates      map[jobspb.JobID]schemaChangerJobUpdate
	scheduleIDsToDelete          []jobspb.ScheduleID
	statsToRefresh               catalog.DescriptorIDSet
	indexesToSplitAndScatter     []indexesToSplitAndScatter
	gcJobs
}

type databaseRoleSettingToDelete struct {
	dbID catid.DescID
}

type indexesToSplitAndScatter struct {
	tableID catid.DescID
	indexID catid.IndexID
}

type schemaChangerJobUpdate struct {
	isNonCancelable       bool
	runningStatus         string
	descriptorIDsToRemove catalog.DescriptorIDSet
}

var _ scmutationexec.DeferredMutationStateUpdater = (*deferredState)(nil)

func (s *deferredState) DeleteDatabaseRoleSettings(ctx context.Context, dbID descpb.ID) error {
	s.databaseRoleSettingsToDelete = append(s.databaseRoleSettingsToDelete,
		databaseRoleSettingToDelete{
			dbID: dbID,
		})
	return nil
}

func (s *deferredState) AddIndexForMaybeSplitAndScatter(
	tableID catid.DescID, indexID catid.IndexID,
) {
	s.indexesToSplitAndScatter = append(s.indexesToSplitAndScatter,
		indexesToSplitAndScatter{
			tableID: tableID,
			indexID: indexID,
		})
}

func (s *deferredState) DeleteSchedule(scheduleID jobspb.ScheduleID) {
	s.scheduleIDsToDelete = append(s.scheduleIDsToDelete, scheduleID)
}

func (s *deferredState) RefreshStats(descriptorID descpb.ID) {
	s.statsToRefresh.Add(descriptorID)
}

func (s *deferredState) AddNewSchemaChangerJob(
	jobID jobspb.JobID,
	stmts []scpb.Statement,
	isNonCancelable bool,
	auth scpb.Authorization,
	descriptorIDs catalog.DescriptorIDSet,
	runningStatus string,
) error {
	if s.schemaChangerJob != nil {
		return errors.AssertionFailedf("cannot create more than one new schema change job")
	}
	s.schemaChangerJob = MakeDeclarativeSchemaChangeJobRecord(
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
		stmtStrs[i] = stmt.RedactedStatement.StripMarkers()
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
		StatusMessage: jobs.StatusMessage(runningStatus),
		NonCancelable: isNonCancelable,
	}
	return rec
}

func (s *deferredState) UpdateSchemaChangerJob(
	jobID jobspb.JobID,
	isNonCancelable bool,
	runningStatus string,
	descriptorIDsToRemove catalog.DescriptorIDSet,
) error {
	if s.schemaChangerJobUpdates == nil {
		s.schemaChangerJobUpdates = make(map[jobspb.JobID]schemaChangerJobUpdate)
	} else if _, exists := s.schemaChangerJobUpdates[jobID]; exists {
		return errors.AssertionFailedf("cannot update job %d more than once", jobID)
	}
	s.schemaChangerJobUpdates[jobID] = schemaChangerJobUpdate{
		isNonCancelable:       isNonCancelable,
		runningStatus:         runningStatus,
		descriptorIDsToRemove: descriptorIDsToRemove,
	}
	return nil
}

func (s *deferredState) exec(
	ctx context.Context,
	c Catalog,
	tjr TransactionalJobRegistry,
	m DescriptorMetadataUpdater,
	q StatsRefreshQueue,
	iss IndexSpanSplitter,
) error {
	dbZoneConfigsToDelete, gcJobRecords := s.gcJobs.makeRecords(tjr.MakeJobID)
	// Any databases being GCed should have an entry even if none of its tables
	// are being dropped. This entry will be used to generate the GC jobs below.
	for _, id := range dbZoneConfigsToDelete.Ordered() {
		if err := c.DeleteZoneConfig(ctx, id); err != nil {
			return err
		}
	}
	if err := c.Run(ctx); err != nil {
		return err
	}
	for _, dbRoleSetting := range s.databaseRoleSettingsToDelete {
		err := m.DeleteDatabaseRoleSettings(ctx, dbRoleSetting.dbID)
		if err != nil {
			return err
		}
	}
	for _, scheduleID := range s.scheduleIDsToDelete {
		if err := m.DeleteSchedule(ctx, scheduleID); err != nil {
			return err
		}
	}
	for _, idx := range s.indexesToSplitAndScatter {
		descs, err := c.MustReadImmutableDescriptors(ctx, idx.tableID)
		if err != nil {
			return err
		}
		tableDesc := descs[0].(catalog.TableDescriptor)
		idxDesc, err := catalog.MustFindIndexByID(tableDesc, idx.indexID)
		if err != nil {
			return err
		}
		if err := iss.MaybeSplitIndexSpans(ctx, tableDesc, idxDesc); err != nil {
			return err
		}
	}
	s.statsToRefresh.ForEach(q.AddTableForStatsRefresh)
	// Note that we perform the system.jobs writes last in order to acquire locks
	// on the job rows in question as late as possible. If a restart is
	// encountered, these locks will be retained in subsequent epochs (assuming
	// that the transaction is not aborted due to, say, a deadlock). If we were
	// to lock the jobs table first, they would not provide any liveness benefit
	// because their entries are non-deterministic. The jobs writes are
	// particularly bad because that table is constantly being scanned.
	return manageJobs(
		ctx,
		gcJobRecords,
		s.schemaChangerJob,
		s.schemaChangerJobUpdates,
		tjr,
	)
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
			s.updatedProgress().StatusMessage = update.runningStatus
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
