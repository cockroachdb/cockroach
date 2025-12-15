// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descmetadata

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlinit"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// metadataUpdater which implements scexec.MetaDataUpdater that is used to update
// comments on different schema objects.
type metadataUpdater struct {
	ctx          context.Context
	txn          isql.Txn
	sessionData  *sessiondata.SessionData
	descriptors  *descs.Collection
	cacheEnabled bool

	// Fields needed for TTL schedule creation.
	settings  *cluster.Settings
	knobs     *jobs.TestingKnobs
	clusterID uuid.UUID
}

// NewMetadataUpdater creates a new comment updater, which can be used to
// create / destroy metadata (i.e. comments) associated with different
// schema objects.
func NewMetadataUpdater(
	ctx context.Context,
	txn isql.Txn,
	descriptors *descs.Collection,
	settingsValues *settings.Values,
	sessionData *sessiondata.SessionData,
	clusterSettings *cluster.Settings,
	knobs *jobs.TestingKnobs,
	clusterID uuid.UUID,
) scexec.DescriptorMetadataUpdater {
	return metadataUpdater{
		ctx:          ctx,
		txn:          txn,
		sessionData:  sessionData,
		descriptors:  descriptors,
		cacheEnabled: sessioninit.CacheEnabled.Get(settingsValues),
		settings:     clusterSettings,
		knobs:        knobs,
		clusterID:    clusterID,
	}
}

// DeleteDatabaseRoleSettings implement scexec.DescriptorMetaDataUpdater.
func (mu metadataUpdater) DeleteDatabaseRoleSettings(ctx context.Context, dbID descpb.ID) error {
	rowsDeleted, err := mu.txn.ExecEx(ctx,
		"delete-db-role-setting",
		mu.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(
			`DELETE FROM system.public.%s WHERE database_id = $1`,
			catconstants.DatabaseRoleSettingsTableName,
		),
		dbID,
	)
	if err != nil {
		return err
	}
	// If the cache is off or if no rows changed, there's no need to bump the
	// table version.
	if !mu.cacheEnabled || rowsDeleted == 0 {
		return nil
	}
	// Bump the table version for the role settings table when we modify it.
	desc, err := mu.descriptors.MutableByID(mu.txn.KV()).Table(ctx, keys.DatabaseRoleSettingsTableID)
	if err != nil {
		return err
	}
	desc.MaybeIncrementVersion()
	return mu.descriptors.WriteDesc(ctx, false /*kvTrace*/, desc, mu.txn.KV())
}

// DeleteSchedule implement scexec.DescriptorMetadataUpdater.
func (mu metadataUpdater) DeleteSchedule(ctx context.Context, scheduleID jobspb.ScheduleID) error {
	_, err := mu.txn.ExecEx(
		ctx,
		"delete-schedule",
		mu.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		"DELETE FROM system.scheduled_jobs WHERE schedule_id = $1",
		scheduleID,
	)
	return err
}

// UpdateTTLScheduleLabel implement scexec.DescriptorMetadataUpdater.
func (mu metadataUpdater) UpdateTTLScheduleLabel(
	ctx context.Context, tbl catalog.TableDescriptor,
) error {
	if !tbl.HasRowLevelTTL() {
		return nil
	}

	_, err := mu.txn.ExecEx(
		ctx,
		"update-ttl-schedule-label",
		mu.txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		"UPDATE system.scheduled_jobs SET schedule_name = $1 WHERE schedule_id = $2",
		ttlbase.BuildScheduleLabel(tbl),
		tbl.GetRowLevelTTL().ScheduleID,
	)
	return err
}

// UpdateTTLScheduleCron implement scexec.DescriptorMetadataUpdater.
func (mu metadataUpdater) UpdateTTLScheduleCron(
	ctx context.Context, scheduleID jobspb.ScheduleID, cronExpr string,
) error {
	env := jobs.JobSchedulerEnv(mu.knobs)
	schedules := jobs.ScheduledJobTxn(mu.txn)
	s, err := schedules.Load(ctx, env, scheduleID)
	if err != nil {
		return err
	}
	if err := s.SetScheduleAndNextRun(cronExpr); err != nil {
		return err
	}
	return schedules.Update(ctx, s)
}

// CreateRowLevelTTLSchedule implements scexec.DescriptorMetadataUpdater.
func (mu metadataUpdater) CreateRowLevelTTLSchedule(
	ctx context.Context, tbl catalog.TableDescriptor,
) error {
	if !tbl.HasRowLevelTTL() {
		return nil
	}

	// Get the mutable table descriptor to update the schedule ID.
	mutTbl, err := mu.descriptors.MutableByID(mu.txn.KV()).Table(ctx, tbl.GetID())
	if err != nil {
		return err
	}

	// Create the scheduled job using the shared helper.
	schedules := jobs.ScheduledJobTxn(mu.txn)
	version := clusterversion.ClusterVersion{Version: mu.settings.Version.ActiveVersion(ctx).Version}
	sj, err := ttlinit.CreateRowLevelTTLScheduledJob(
		ctx,
		mu.knobs,
		schedules,
		tbl.GetPrivileges().Owner(),
		tbl,
		mu.clusterID,
		version,
	)
	if err != nil {
		return err
	}

	// Update the table descriptor with the schedule ID.
	mutTbl.RowLevelTTL.ScheduleID = sj.ScheduleID()

	// Also update the ScheduleID in the declarative schema changer state's
	// RowLevelTTL elements. This is necessary so that if the schema change
	// fails and needs to roll back, the DeleteSchedule operation will have
	// the correct ScheduleID to delete.
	updateRowLevelTTLElementScheduleID(mutTbl, sj.ScheduleID())

	return mu.descriptors.WriteDesc(ctx, false /* kvTrace */, mutTbl, mu.txn.KV())
}

// updateRowLevelTTLElementScheduleID updates the ScheduleID in any RowLevelTTL
// elements in the declarative schema changer state. This is needed so that
// rollback operations can correctly identify and delete the schedule.
func updateRowLevelTTLElementScheduleID(mutTbl *tabledesc.Mutable, scheduleID jobspb.ScheduleID) {
	state := mutTbl.GetDeclarativeSchemaChangerState()
	if state == nil {
		return
	}

	// Look for RowLevelTTL elements going PUBLIC and update their ScheduleID.
	modified := false
	for i := range state.Targets {
		if ttl := state.Targets[i].GetRowLevelTTL(); ttl != nil {
			// Update RowLevelTTL elements that are going PUBLIC (being added)
			// and don't already have a ScheduleID set.
			if state.Targets[i].TargetStatus == scpb.Status_PUBLIC &&
				ttl.TableID == mutTbl.GetID() && ttl.ScheduleID == 0 {
				ttl.ScheduleID = scheduleID
				modified = true
			}
		}
	}

	if modified {
		mutTbl.SetDeclarativeSchemaChangerState(state)
	}
}
