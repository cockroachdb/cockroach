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
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupresolver"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
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
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
) ([]backuppb.BackupManifest_DescriptorRevision, error) {

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
	var interestingChanges []backuppb.BackupManifest_DescriptorRevision

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
				initial := backuppb.BackupManifest_DescriptorRevision{Time: startTime, ID: i.GetID(), Desc: desc.DescriptorProto()}
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
			desc := backupinfo.NewDescriptorForManifest(change.Desc)
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
) ([]backuppb.BackupManifest_DescriptorRevision, error) {
	startKey := codec.TablePrefix(keys.DescriptorTableID)
	endKey := startKey.PrefixEnd()

	allRevs, err := kvclient.GetAllRevisions(ctx, db, startKey, endKey, startTime, endTime)
	if err != nil {
		return nil, err
	}

	var res []backuppb.BackupManifest_DescriptorRevision

	for _, revs := range allRevs {
		id, err := codec.DecodeDescMetadataID(revs.Key)
		if err != nil {
			return nil, err
		}
		for _, rev := range revs.Values {
			r := backuppb.BackupManifest_DescriptorRevision{ID: descpb.ID(id), Time: rev.Timestamp}
			if len(rev.RawBytes) != 0 {
				// We update the modification time for the descriptors here with the
				// timestamp of the KV row so that we can identify the appropriate
				// descriptors to use during restore.
				// Note that the modification time of descriptors on disk is usually 0.
				// See the comment on descpb.FromSerializedValue for more details.
				b, err := descbuilder.FromSerializedValue(&rev)
				if err != nil {
					return nil, err
				}
				if b == nil {
					continue
				}
				desc := b.BuildCreatedMutable()
				r.Desc = desc.DescriptorProto()
				// Collect the prior IDs of table descriptors, as the ID may have been
				// changed during truncate prior to 20.2.
				switch t := desc.(type) {
				case *tabledesc.Mutable:
					if priorIDs != nil && t.ReplacementOf.ID != descpb.InvalidID {
						priorIDs[t.ID] = t.ReplacementOf.ID
					}
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
		case catalog.FunctionDescriptor:
			fullClusterDescs = append(fullClusterDescs, desc)
		}
	}
	return fullClusterDescs, fullClusterDBs, nil
}

func fullClusterTargetsRestore(
	ctx context.Context, allDescs []catalog.Descriptor, lastBackupManifest backuppb.BackupManifest,
) (
	[]catalog.Descriptor,
	[]catalog.DatabaseDescriptor,
	map[tree.TablePattern]catalog.Descriptor,
	[]mtinfopb.TenantInfoWithUsage,
	error,
) {
	ctx, span := tracing.ChildSpan(ctx, "backupccl.fullClusterTargetsRestore")
	_ = ctx // ctx is currently unused, but this new ctx should be used below in the future.
	defer span.Finish()

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
		return nil, nil, nil, nil, err
	}
	return filteredDescs, filteredDBs, nil, lastBackupManifest.GetTenants(), nil
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

// checkMissingIntroducedSpans asserts that each backup's IntroducedSpans
// contain all tables that require an introduction (i.e. a backup from ts=0), if
// they are also restore targets. This specifically entails checking the
// following invariant on each table we seek to restore: if the nth backup
// contains an online table in its backupManifest.Descriptors field or
// backupManifest.DescriptorsChanges field but the n-1th does not contain it online in
// its Descriptors field, then that table must be covered by the nth backup's
// manifest.IntroducedSpans field. Note: this check assumes that
// mainBackupManifests are sorted by Endtime and this check only applies to
// backups with a start time that is less than the restore AOST.
func checkMissingIntroducedSpans(
	restoringDescs []catalog.Descriptor,
	mainBackupManifests []backuppb.BackupManifest,
	endTime hlc.Timestamp,
	codec keys.SQLCodec,
) error {
	// Gather the tables we'd like to restore.
	requiredTables := make(map[descpb.ID]struct{})
	for _, restoringDesc := range restoringDescs {
		if _, ok := restoringDesc.(catalog.TableDescriptor); ok {
			requiredTables[restoringDesc.GetID()] = struct{}{}
		}
	}

	for i, b := range mainBackupManifests {
		if !endTime.IsEmpty() && endTime.Less(b.StartTime) {
			// No need to check a backup that began after the restore AOST.
			return nil
		}

		if i == 0 {
			// The full backup does not contain reintroduced spans.
			continue
		}

		// Gather the _online_ tables included in the previous backup.
		prevOnlineTables := make(map[descpb.ID]struct{})
		for _, desc := range mainBackupManifests[i-1].Descriptors {
			if table, _, _, _, _ := descpb.GetDescriptors(&desc); table != nil && table.Public() {
				prevOnlineTables[table.GetID()] = struct{}{}
			}
		}

		// Gather the tables that were reintroduced in the current backup (i.e.
		// backed up from ts=0).
		tablesIntroduced := make(map[descpb.ID]struct{})
		for _, span := range mainBackupManifests[i].IntroducedSpans {
			_, tableID, err := codec.DecodeTablePrefix(span.Key)
			if err != nil {
				return err
			}
			tablesIntroduced[descpb.ID(tableID)] = struct{}{}
		}

		// requiredIntroduction affirms that the given table in backup i was either
		// introduced in backup i, or included online in backup i-1.
		requiredIntroduction := func(table *descpb.TableDescriptor) error {
			if _, required := requiredTables[table.GetID()]; !required {
				// The table is not required for the restore, so there's no need to check coverage.
				return nil
			}

			if !includeTableSpans(table) {
				return nil
			}

			if _, introduced := tablesIntroduced[table.GetID()]; introduced {
				// The table was introduced, thus the table is properly covered at
				// this point in the backup chain.
				return nil
			}

			if _, inPrevBackup := prevOnlineTables[table.GetID()]; inPrevBackup {
				// The table was included in the previous backup, which implies the
				// table was online at prevBackup.Endtime, and therefore does not need
				// to be introduced.
				return nil
			}
			tableError := errors.Newf("table %q cannot be safely restored from this backup."+
				" This backup is affected by issue #88042, which produced incorrect backups after an IMPORT."+
				" To continue the restore, you can either:"+
				" 1) restore to a system time before the import completed, %v;"+
				" 2) restore with a newer backup chain (a full backup [+ incrementals])"+
				" taken after the current backup target;"+
				" 3) or remove table %v from the restore targets.",
				table.Name, mainBackupManifests[i].StartTime.GoTime().String(), table.Name)
			return errors.WithIssueLink(tableError, errors.IssueLink{
				IssueURL: "https://www.cockroachlabs.com/docs/advisories/a88042",
				Detail: `An incremental database backup with revision history can incorrectly backup data for a table
that was running an IMPORT at the time of the previous incremental in this chain of backups.`,
			})
		}

		for _, desc := range mainBackupManifests[i].Descriptors {
			// Check that all online tables at backup time were either introduced or
			// in the previous backup.
			if table, _, _, _, _ := descpb.GetDescriptors(&desc); table != nil && table.Public() {
				if err := requiredIntroduction(table); err != nil {
					return err
				}
			}
		}

		// As described in the docstring for getReintroducedSpans(), there are cases
		// where a descriptor may appear in manifest.DescriptorChanges but not
		// manifest.Descriptors. If a descriptor switched from offline to online at
		// any moment during the backup interval, it needs to be reintroduced.
		for _, desc := range mainBackupManifests[i].DescriptorChanges {
			if table, _, _, _, _ := descpb.GetDescriptors(desc.Desc); table != nil && table.Public() {
				if err := requiredIntroduction(table); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// selectTargets loads all descriptors from the selected backup manifest(s),
// filters the descriptors based on the targets specified in the restore, and
// calculates the max descriptor ID in the backup.
// Post filtering, the method returns:
//   - A list of all descriptors (table, type, database, schema, function) along with
//     their parent databases.
//   - A list of database descriptors IFF the user is restoring on the cluster or
//     database level.
//   - A map of table patterns to the resolved descriptor IFF the user is
//     restoring on the table level.
//   - A list of tenants to restore, if applicable.
func selectTargets(
	ctx context.Context,
	p sql.PlanHookState,
	backupManifests []backuppb.BackupManifest,
	targets tree.BackupTargetList,
	descriptorCoverage tree.DescriptorCoverage,
	asOf hlc.Timestamp,
) (
	[]catalog.Descriptor,
	[]catalog.DatabaseDescriptor,
	map[tree.TablePattern]catalog.Descriptor,
	[]mtinfopb.TenantInfoWithUsage,
	error,
) {
	ctx, span := tracing.ChildSpan(ctx, "backupccl.selectTargets")
	defer span.Finish()
	allDescs, lastBackupManifest, err := backupinfo.LoadSQLDescsFromBackupsAtTime(backupManifests, asOf)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if descriptorCoverage == tree.AllDescriptors {
		return fullClusterTargetsRestore(ctx, allDescs, lastBackupManifest)
	}

	if descriptorCoverage == tree.SystemUsers {
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
				case systemschema.RoleOptionsTable.GetName():
					systemTables = append(systemTables, desc)
				}
			}
		}
		if users == nil {
			return nil, nil, nil, nil, errors.Errorf("cannot restore system users as no system.users table in the backup")
		}
		return systemTables, nil, nil, nil, nil
	}

	if targets.TenantID.IsSet() {
		for _, tenant := range lastBackupManifest.GetTenants() {
			// TODO(dt): for now it is zero-or-one but when that changes, we should
			// either keep it sorted or build a set here.
			if tenant.ID == targets.TenantID.ID {
				return nil, nil, nil, []mtinfopb.TenantInfoWithUsage{tenant}, nil
			}
		}
		return nil, nil, nil, nil, errors.Errorf("tenant %d not in backup", targets.TenantID.ID)
	}

	matched, err := backupresolver.DescriptorsMatchingTargets(ctx,
		p.CurrentDatabase(), p.CurrentSearchPath(), allDescs, targets, asOf)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if len(matched.Descs) == 0 {
		return nil, nil, nil, nil, errors.Errorf("no tables or databases matched the given targets: %s", tree.ErrString(&targets))
	}

	if lastBackupManifest.FormatVersion >= backupinfo.BackupFormatDescriptorTrackingVersion {
		if err := matched.CheckExpansions(lastBackupManifest.CompleteDbs); err != nil {
			return nil, nil, nil, nil, err
		}
	}

	return matched.Descs, matched.RequestedDBs, matched.DescsByTablePattern, nil, nil
}

// EntryFiles is a group of sst files of a backup table range
type EntryFiles []execinfrapb.RestoreFileSpec

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
		typeDesc, err := col.ByID(txn).Get().Type(ctx, regionEnumID)
		if err != nil {
			return err
		}
		regionEnum := typeDesc.AsRegionEnumTypeDescriptor()
		if regionEnum == nil {
			return errors.AssertionFailedf("expected region enum type, not %s for type %q (%d)",
				typeDesc.GetKind(), typeDesc.GetName(), typeDesc.GetID())
		}
		var existingRegions []string
		var found bool
		_ = regionEnum.ForEachPublicRegion(func(dbRegionName catpb.RegionName) error {
			if dbRegionName == regionName {
				found = true
				return iterutil.StopIteration()
			}
			existingRegions = append(existingRegions, fmt.Sprintf("%q", dbRegionName))
			return nil
		})
		if found {
			return nil
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
