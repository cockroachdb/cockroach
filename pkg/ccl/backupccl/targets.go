// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupresolver"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// getRelevantDescChanges finds the changes between start and end time to the
// SQL descriptors matching `descs` or `expandedDBs`, ordered by time. A
// descriptor revision matches if it is an earlier revision of a descriptor in
// descs (same ID) or has parentID in `expanded`. Deleted descriptors are
// represented as nil. Fills in the `priorIDs` map in the process, which maps
// a descriptor the ID by which it was previously known (e.g pre-TRUNCATE).
func getRelevantDescChanges(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	startTime, endTime hlc.Timestamp,
	descriptors []catalog.Descriptor,
	expanded []descpb.ID,
	priorIDs map[descpb.ID]descpb.ID,
	fullCluster bool,
) ([]BackupManifest_DescriptorRevision, error) {

	allChanges, err := getAllDescChanges(ctx, execCfg.Codec, execCfg.DB, startTime, endTime, priorIDs)
	if err != nil {
		return nil, err
	}

	// If no descriptors changed, we can just stop now and have RESTORE use the
	// normal list of descs (i.e. as of endTime).
	if len(allChanges) == 0 {
		return nil, nil
	}

	// interestingChanges will be every descriptor change relevant to the backup.
	var interestingChanges []BackupManifest_DescriptorRevision

	// interestingIDs are the descriptor for which we're interested in capturing
	// changes. This is initially the descriptors matched (as of endTime) by our
	// target spec, plus those that belonged to a DB that our spec expanded at any
	// point in the interval.
	interestingIDs := make(map[descpb.ID]struct{}, len(descriptors))

	systemTableIDsToExcludeFromBackup, err := GetSystemTableIDsToExcludeFromClusterBackup(ctx, execCfg)
	if err != nil {
		return nil, err
	}
	isExcludedDescriptor := func(id descpb.ID) bool {
		if _, isOptOutSystemTable := systemTableIDsToExcludeFromBackup[id]; id == keys.SystemDatabaseID || isOptOutSystemTable {
			return true
		}
		return false
	}

	isInterestingID := func(id descpb.ID) bool {
		// We're interested in changes to all descriptors if we're targeting all
		// descriptors except for the descriptors that we do not include in a
		// cluster backup.
		if fullCluster && !isExcludedDescriptor(id) {
			return true
		}
		// A change to an ID that we're interested in is obviously interesting.
		if _, ok := interestingIDs[id]; ok {
			return true
		}
		return false
	}

	// The descriptors that currently (endTime) match the target spec (desc) are
	// obviously interesting to our backup.
	for _, i := range descriptors {
		interestingIDs[i.GetID()] = struct{}{}
		if table, isTable := i.(catalog.TableDescriptor); isTable {

			for j := table.GetReplacementOf().ID; j != descpb.InvalidID; j = priorIDs[j] {
				interestingIDs[j] = struct{}{}
			}
		}
	}

	// We're also interested in any desc that belonged to a DB we're backing up.
	// We'll start by looking at all descriptors as of the beginning of the
	// interval and add to the set of IDs that we are interested any descriptor that
	// belongs to one of the parents we care about.
	interestingParents := make(map[descpb.ID]struct{}, len(expanded))
	for _, i := range expanded {
		interestingParents[i] = struct{}{}
	}

	if !startTime.IsEmpty() {
		starting, err := backupresolver.LoadAllDescs(ctx, execCfg, startTime)
		if err != nil {
			return nil, err
		}
		for _, i := range starting {
			switch desc := i.(type) {
			case catalog.TableDescriptor, catalog.TypeDescriptor, catalog.SchemaDescriptor:
				// We need to add to interestingIDs so that if we later see a delete for
				// this ID we still know it is interesting to us, even though we will not
				// have a parentID at that point (since the delete is a nil desc).
				if _, ok := interestingParents[desc.GetParentID()]; ok {
					interestingIDs[desc.GetID()] = struct{}{}
				}
			}
			if isInterestingID(i.GetID()) {
				desc := i
				// We inject a fake "revision" that captures the starting state for
				// matched descriptor, to allow restoring to times before its first rev
				// actually inside the window. This likely ends up duplicating the last
				// version in the previous BACKUP descriptor, but avoids adding more
				// complicated special-cases in RESTORE, so it only needs to look in a
				// single BACKUP to restore to a particular time.
				initial := BackupManifest_DescriptorRevision{Time: startTime, ID: i.GetID(), Desc: desc.DescriptorProto()}
				interestingChanges = append(interestingChanges, initial)
			}
		}
	}

	for _, change := range allChanges {
		// A change to an ID that we are interested in is obviously interesting --
		// a change is also interesting if it is to a table that has a parent that
		// we are interested and thereafter it also becomes an ID in which we are
		// interested in changes (since, as mentioned above, to decide if deletes
		// are interesting).
		if isInterestingID(change.ID) {
			interestingChanges = append(interestingChanges, change)
		} else if change.Desc != nil {
			desc := descbuilder.NewBuilder(change.Desc).BuildExistingMutable()
			switch desc := desc.(type) {
			case catalog.TableDescriptor, catalog.TypeDescriptor, catalog.SchemaDescriptor:
				if _, ok := interestingParents[desc.GetParentID()]; ok {
					interestingIDs[desc.GetID()] = struct{}{}
					interestingChanges = append(interestingChanges, change)
				}
			}
		}
	}

	sort.Slice(interestingChanges, func(i, j int) bool {
		return interestingChanges[i].Time.Less(interestingChanges[j].Time)
	})

	return interestingChanges, nil
}

// getAllDescChanges gets every sql descriptor change between start and end time
// returning its ID, content and the change time (with deletions represented as
// nil content).
func getAllDescChanges(
	ctx context.Context,
	codec keys.SQLCodec,
	db *kv.DB,
	startTime, endTime hlc.Timestamp,
	priorIDs map[descpb.ID]descpb.ID,
) ([]BackupManifest_DescriptorRevision, error) {
	startKey := codec.TablePrefix(keys.DescriptorTableID)
	endKey := startKey.PrefixEnd()

	allRevs, err := kvclient.GetAllRevisions(ctx, db, startKey, endKey, startTime, endTime)
	if err != nil {
		return nil, err
	}

	var res []BackupManifest_DescriptorRevision

	for _, revs := range allRevs {
		id, err := codec.DecodeDescMetadataID(revs.Key)
		if err != nil {
			return nil, err
		}
		for _, rev := range revs.Values {
			r := BackupManifest_DescriptorRevision{ID: descpb.ID(id), Time: rev.Timestamp}
			if len(rev.RawBytes) != 0 {
				var desc descpb.Descriptor
				if err := rev.GetProto(&desc); err != nil {
					return nil, err
				}
				r.Desc = &desc

				// Collect the prior IDs of table descriptors, as the ID may have been
				// changed during truncate prior to 20.2.
				// We update the modification time for the descriptors here with the
				// timestamp of the KV row so that we can identify the appropriate
				// descriptors to use during restore.
				// Note that the modification time of descriptors on disk is usually 0.
				// See the comment on MaybeSetDescriptorModificationTime... for more.
				t, _, _, _ := descpb.FromDescriptorWithMVCCTimestamp(r.Desc, rev.Timestamp)
				if priorIDs != nil && t != nil && t.ReplacementOf.ID != descpb.InvalidID {
					priorIDs[t.ID] = t.ReplacementOf.ID
				}
			}
			res = append(res, r)
		}
	}
	return res, nil
}

// fullClusterTargets returns all of the descriptors to be included in a full
// cluster backup, along with all the "complete databases" that we are backing
// up.
func fullClusterTargets(
	allDescs []catalog.Descriptor,
) ([]catalog.Descriptor, []catalog.DatabaseDescriptor, error) {
	fullClusterDescs := make([]catalog.Descriptor, 0, len(allDescs))
	fullClusterDBs := make([]catalog.DatabaseDescriptor, 0)

	systemTablesToBackup := GetSystemTablesToIncludeInClusterBackup()

	for _, desc := range allDescs {
		// If a descriptor is in the DROP state at `EndTime` we do not want to
		// include it in the backup.
		if desc.Dropped() {
			continue
		}
		switch desc := desc.(type) {
		case catalog.DatabaseDescriptor:
			dbDesc := dbdesc.NewBuilder(desc.DatabaseDesc()).BuildImmutableDatabase()
			fullClusterDescs = append(fullClusterDescs, desc)
			if dbDesc.GetID() != systemschema.SystemDB.GetID() {
				// The only database that isn't being fully backed up is the system DB.
				fullClusterDBs = append(fullClusterDBs, dbDesc)
			}
		case catalog.TableDescriptor:
			if desc.GetParentID() == keys.SystemDatabaseID {
				// Add only the system tables that we plan to include in a full cluster
				// backup.
				if _, ok := systemTablesToBackup[desc.GetName()]; ok {
					fullClusterDescs = append(fullClusterDescs, desc)
				}
			} else {
				fullClusterDescs = append(fullClusterDescs, desc)
			}
		case catalog.SchemaDescriptor:
			fullClusterDescs = append(fullClusterDescs, desc)
		case catalog.TypeDescriptor:
			fullClusterDescs = append(fullClusterDescs, desc)
		}
	}
	return fullClusterDescs, fullClusterDBs, nil
}

func fullClusterTargetsRestore(
	allDescs []catalog.Descriptor, lastBackupManifest BackupManifest,
) ([]catalog.Descriptor, []catalog.DatabaseDescriptor, []descpb.TenantInfoWithUsage, error) {
	fullClusterDescs, fullClusterDBs, err := fullClusterTargets(allDescs)
	var filteredDescs []catalog.Descriptor
	var filteredDBs []catalog.DatabaseDescriptor
	for _, desc := range fullClusterDescs {
		if desc.GetID() != keys.SystemDatabaseID {
			filteredDescs = append(filteredDescs, desc)
		}
	}
	for _, desc := range fullClusterDBs {
		if desc.GetID() != keys.SystemDatabaseID {
			filteredDBs = append(filteredDBs, desc)
		}
	}
	if err != nil {
		return nil, nil, nil, err
	}
	return filteredDescs, filteredDBs, lastBackupManifest.GetTenants(), nil
}

// fullClusterTargetsBackup returns the same descriptors referenced in
// fullClusterTargets, but rather than returning the entire database
// descriptor as the second argument, it only returns their IDs.
func fullClusterTargetsBackup(
	ctx context.Context, execCfg *sql.ExecutorConfig, endTime hlc.Timestamp,
) ([]catalog.Descriptor, []descpb.ID, error) {
	allDescs, err := backupresolver.LoadAllDescs(ctx, execCfg, endTime)
	if err != nil {
		return nil, nil, err
	}

	fullClusterDescs, fullClusterDBs, err := fullClusterTargets(allDescs)
	if err != nil {
		return nil, nil, err
	}

	fullClusterDBIDs := make([]descpb.ID, 0)
	for _, desc := range fullClusterDBs {
		fullClusterDBIDs = append(fullClusterDBIDs, desc.GetID())
	}
	if len(fullClusterDescs) == 0 {
		return nil, nil, errors.New("no descriptors available to backup at selected time")
	}
	return fullClusterDescs, fullClusterDBIDs, nil
}

// selectTargets loads all descriptors from the selected backup manifest(s),
// filters the descriptors based on the targets specified in the restore, and
// calculates the max descriptor ID in the backup.
// Post filtering, the method returns:
//  - A list of all descriptors (table, type, database, schema) along with their
//    parent databases.
//  - A list of database descriptors IFF the user is restoring on the cluster or
//    database level
//  - A list of tenants to restore, if applicable.
func selectTargets(
	ctx context.Context,
	p sql.PlanHookState,
	backupManifests []BackupManifest,
	targets tree.TargetList,
	descriptorCoverage tree.DescriptorCoverage,
	asOf hlc.Timestamp,
	restoreSystemUsers bool,
) ([]catalog.Descriptor, []catalog.DatabaseDescriptor, []descpb.TenantInfoWithUsage, error) {
	allDescs, lastBackupManifest := loadSQLDescsFromBackupsAtTime(backupManifests, asOf)

	if descriptorCoverage == tree.AllDescriptors {
		return fullClusterTargetsRestore(allDescs, lastBackupManifest)
	}

	if restoreSystemUsers {
		systemTables := make([]catalog.Descriptor, 0)
		var users catalog.Descriptor
		for _, desc := range allDescs {
			if desc.GetParentID() == systemschema.SystemDB.GetID() {
				switch desc.GetName() {
				case systemschema.UsersTable.GetName():
					users = desc
					systemTables = append(systemTables, desc)
				case systemschema.RoleMembersTable.GetName():
					systemTables = append(systemTables, desc)
					// TODO(casper): should we handle role_options table?
				}
			}
		}
		if users == nil {
			return nil, nil, nil, errors.Errorf("cannot restore system users as no system.users table in the backup")
		}
		return systemTables, nil, nil, nil
	}

	if targets.TenantID.IsSet() {
		for _, tenant := range lastBackupManifest.GetTenants() {
			// TODO(dt): for now it is zero-or-one but when that changes, we should
			// either keep it sorted or build a set here.
			if tenant.ID == targets.TenantID.ToUint64() {
				return nil, nil, []descpb.TenantInfoWithUsage{tenant}, nil
			}
		}
		return nil, nil, nil, errors.Errorf("tenant %d not in backup", targets.TenantID.ToUint64())
	}

	matched, err := backupresolver.DescriptorsMatchingTargets(ctx,
		p.CurrentDatabase(), p.CurrentSearchPath(), allDescs, targets, asOf)
	if err != nil {
		return nil, nil, nil, err
	}

	if len(matched.Descs) == 0 {
		return nil, nil, nil, errors.Errorf("no tables or databases matched the given targets: %s", tree.ErrString(&targets))
	}

	if lastBackupManifest.FormatVersion >= BackupFormatDescriptorTrackingVersion {
		if err := matched.CheckExpansions(lastBackupManifest.CompleteDbs); err != nil {
			return nil, nil, nil, err
		}
	}

	return matched.Descs, matched.RequestedDBs, nil, nil
}

// EntryFiles is a group of sst files of a backup table range
type EntryFiles []execinfrapb.RestoreFileSpec

// BackupTableEntry wraps information of a table retrieved
// from backup manifests.
// exported to cliccl for exporting data directly from backup sst.
type BackupTableEntry struct {
	Desc                 catalog.TableDescriptor
	Span                 roachpb.Span
	Files                []EntryFiles
	LastSchemaChangeTime hlc.Timestamp
}

// MakeBackupTableEntry looks up the descriptor of fullyQualifiedTableName
// from backupManifests and returns a BackupTableEntry, which contains
// the table descriptor, the primary index span, and the sst files.
func MakeBackupTableEntry(
	ctx context.Context,
	fullyQualifiedTableName string,
	backupManifests []BackupManifest,
	endTime hlc.Timestamp,
	user security.SQLUsername,
	backupCodec keys.SQLCodec,
) (BackupTableEntry, error) {
	var descName []string
	if descName = strings.Split(fullyQualifiedTableName, "."); len(descName) != 3 {
		return BackupTableEntry{}, errors.Newf("table name should be specified in format databaseName.schemaName.tableName")
	}

	if !endTime.IsEmpty() {
		ind := -1
		for i, b := range backupManifests {
			if b.StartTime.Less(endTime) && endTime.LessEq(b.EndTime) {
				if endTime != b.EndTime && b.MVCCFilter != MVCCFilter_All {
					errorHints := "reading data for requested time requires that BACKUP was created with %q" +
						" or should specify the time to be an exact backup time, nearest backup time is %s"
					return BackupTableEntry{}, errors.WithHintf(
						errors.Newf("unknown read time: %s", timeutil.Unix(0, endTime.WallTime).UTC()),
						errorHints, backupOptRevisionHistory, timeutil.Unix(0, b.EndTime.WallTime).UTC(),
					)
				}
				ind = i
				break
			}
		}
		if ind == -1 {
			return BackupTableEntry{}, errors.Newf("supplied backups do not cover requested time %s", timeutil.Unix(0, endTime.WallTime).UTC())
		}
		backupManifests = backupManifests[:ind+1]
	}

	allDescs, _ := loadSQLDescsFromBackupsAtTime(backupManifests, endTime)
	resolver, err := backupresolver.NewDescriptorResolver(allDescs)
	if err != nil {
		return BackupTableEntry{}, errors.Wrapf(err, "creating a new resolver for all descriptors")
	}

	found, _, desc, err := resolver.LookupObject(ctx, tree.ObjectLookupFlags{}, descName[0], descName[1], descName[2])
	if err != nil {
		return BackupTableEntry{}, errors.Wrapf(err, "looking up table %s", fullyQualifiedTableName)
	}
	if !found {
		return BackupTableEntry{}, errors.Newf("table %s not found", fullyQualifiedTableName)
	}
	tbMutable, ok := desc.(*tabledesc.Mutable)
	if !ok {
		return BackupTableEntry{}, errors.Newf("object %s not mutable", fullyQualifiedTableName)
	}
	tbDesc, err := catalog.AsTableDescriptor(tbMutable)
	if err != nil {
		return BackupTableEntry{}, errors.Wrapf(err, "fetching table %s descriptor", fullyQualifiedTableName)
	}

	tablePrimaryIndexSpan := tbDesc.PrimaryIndexSpan(backupCodec)

	if err := checkCoverage(ctx, []roachpb.Span{tablePrimaryIndexSpan}, backupManifests); err != nil {
		return BackupTableEntry{}, errors.Wrapf(err, "making spans for table %s", fullyQualifiedTableName)
	}

	entry := makeSimpleImportSpans(
		[]roachpb.Span{tablePrimaryIndexSpan},
		backupManifests,
		nil,           /*backupLocalityInfo*/
		roachpb.Key{}, /*lowWaterMark*/
	)

	lastSchemaChangeTime := findLastSchemaChangeTime(backupManifests, tbDesc, endTime)

	backupTableEntry := BackupTableEntry{
		tbDesc,
		tablePrimaryIndexSpan,
		make([]EntryFiles, 0),
		lastSchemaChangeTime,
	}

	for _, e := range entry {
		backupTableEntry.Files = append(backupTableEntry.Files, e.Files)
	}

	return backupTableEntry, nil
}

func findLastSchemaChangeTime(
	backupManifests []BackupManifest, tbDesc catalog.TableDescriptor, endTime hlc.Timestamp,
) hlc.Timestamp {
	lastSchemaChangeTime := endTime
	for i := len(backupManifests) - 1; i >= 0; i-- {
		manifest := backupManifests[i]
		for j := len(manifest.DescriptorChanges) - 1; j >= 0; j-- {
			rev := manifest.DescriptorChanges[j]

			if endTime.LessEq(rev.Time) {
				continue
			}

			if rev.ID == tbDesc.GetID() {
				d := descbuilder.NewBuilder(rev.Desc).BuildExistingMutable()
				revDesc, _ := catalog.AsTableDescriptor(d)
				if !reflect.DeepEqual(revDesc.PublicColumns(), tbDesc.PublicColumns()) {
					return lastSchemaChangeTime
				}
				lastSchemaChangeTime = rev.Time
			}
		}
	}
	return lastSchemaChangeTime
}

// checkMultiRegionCompatible checks if the given table is compatible to be
// restored into the given database according to its multi-region locality.
// It returns an error describing the incompatibility if not.
func checkMultiRegionCompatible(
	ctx context.Context,
	txn *kv.Txn,
	col *descs.Collection,
	table *tabledesc.Mutable,
	database catalog.DatabaseDescriptor,
) error {
	// If we are not dealing with an MR database and table there are no
	// compatibility checks that need to be performed.
	if !database.IsMultiRegion() && table.GetLocalityConfig() == nil {
		return nil
	}

	// If we are restoring a non-MR table into a MR database, allow it. We will
	// set the table to a REGIONAL BY TABLE IN PRIMARY REGION before writing the
	// table descriptor to disk.
	if database.IsMultiRegion() && table.GetLocalityConfig() == nil {
		return nil
	}

	// If we are restoring a MR table into a non-MR database, disallow it.
	if !database.IsMultiRegion() && table.GetLocalityConfig() != nil {
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"cannot restore descriptor for multi-region table %s into non-multi-region database %s",
			table.GetName(),
			database.GetName(),
		)
	}

	if table.IsLocalityGlobal() {
		// Global tables are allowed because they do not reference a particular
		// region.
		return nil
	}

	if table.IsLocalityRegionalByTable() {
		regionName, _ := table.GetRegionalByTableRegion()
		if regionName == catpb.RegionName(tree.PrimaryRegionNotSpecifiedName) {
			// REGIONAL BY PRIMARY REGION tables are allowed since they do not
			// reference a particular region.
			return nil
		}

		// For REGION BY TABLE IN <region> tables, allow the restore if the
		// database has the region.
		regionEnumID := database.GetRegionConfig().RegionEnumID
		regionEnum, err := col.Direct().MustGetTypeDescByID(ctx, txn, regionEnumID)
		if err != nil {
			return err
		}
		dbRegionNames, err := regionEnum.RegionNames()
		if err != nil {
			return err
		}
		existingRegions := make([]string, len(dbRegionNames))
		for i, dbRegionName := range dbRegionNames {
			if dbRegionName == regionName {
				return nil
			}
			existingRegions[i] = fmt.Sprintf("%q", dbRegionName)
		}

		return errors.Newf(
			"cannot restore REGIONAL BY TABLE %s IN REGION %q (table ID: %d) into database %q; region %q not found in database regions %s",
			table.GetName(), regionName, table.GetID(),
			database.GetName(), regionName, strings.Join(existingRegions, ", "),
		)
	}

	if table.IsLocalityRegionalByRow() {
		// Unlike the check for RegionalByTable above, we do not want to run a
		// verification on every row in a RegionalByRow table. If the table has a
		// row with a `crdb_region` that is not in the parent databases' regions,
		// this will be caught later in the restore when we attempt to remap the
		// backed up MR enum to point to the existing MR enum in the restoring
		// cluster.
		return nil
	}

	return errors.AssertionFailedf(
		"locality config of table %s (ID: %d) has locality %v which is unknown by RESTORE",
		table.GetName(), table.GetID(), table.GetLocalityConfig(),
	)
}
