// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/revlog/restorerevlog"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// selectTargetsWithRevlog loads backup descriptors, merges them with
// revision log schema changes through revlogTimestamp, and runs
// target matching against the merged set. This is the revlog
// counterpart of selectTargets used during restore planning.
func selectTargetsWithRevlog(
	ctx context.Context,
	p sql.PlanHookState,
	es cloud.ExternalStorage,
	backupManifests []backuppb.BackupManifest,
	layerToIterFactory backupinfo.LayerToBackupManifestFileIterFactory,
	targets tree.BackupTargetList,
	descriptorCoverage tree.DescriptorCoverage,
	endTime, revlogTimestamp hlc.Timestamp,
) (
	[]catalog.Descriptor,
	[]catalog.DatabaseDescriptor,
	map[tree.TablePattern]catalog.Descriptor,
	[]mtinfopb.TenantInfoWithUsage,
	bool,
	map[descpb.ID]struct{},
	error,
) {
	allBackupDescs, lastManifest, err := backupinfo.LoadSQLDescsFromBackupsAtTime(
		ctx, backupManifests, layerToIterFactory, endTime,
	)
	if err != nil {
		return nil, nil, nil, nil, false, nil,
			errors.Wrap(err, "loading backup descriptors for revlog merge")
	}
	mergedDescs, newDescIDs, err := restorerevlog.ApplyDescriptorChanges(
		ctx, es, allBackupDescs, endTime, revlogTimestamp,
	)
	if err != nil {
		return nil, nil, nil, nil, false, nil, err
	}
	descs, dbs, byPattern, tenants, setupTempDB, selectErr :=
		selectTargetsFromDescs(
			ctx, p, mergedDescs, lastManifest,
			targets, descriptorCoverage, endTime,
		)
	return descs, dbs, byPattern, tenants, setupTempDB,
		newDescIDs, selectErr
}
