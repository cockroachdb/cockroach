// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descmetadata

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessioninit"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlbase"
)

// metadataUpdater which implements scexec.MetaDataUpdater that is used to update
// comments on different schema objects.
type metadataUpdater struct {
	ctx          context.Context
	txn          isql.Txn
	sessionData  *sessiondata.SessionData
	descriptors  *descs.Collection
	cacheEnabled bool
}

// NewMetadataUpdater creates a new comment updater, which can be used to
// create / destroy metadata (i.e. comments) associated with different
// schema objects.
func NewMetadataUpdater(
	ctx context.Context,
	txn isql.Txn,
	descriptors *descs.Collection,
	settings *settings.Values,
	sessionData *sessiondata.SessionData,
) scexec.DescriptorMetadataUpdater {
	return metadataUpdater{
		ctx:          ctx,
		txn:          txn,
		sessionData:  sessionData,
		descriptors:  descriptors,
		cacheEnabled: sessioninit.CacheEnabled.Get(settings),
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
	ctx context.Context, tbl *tabledesc.Mutable,
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
		tbl.RowLevelTTL.ScheduleID,
	)
	return err
}
