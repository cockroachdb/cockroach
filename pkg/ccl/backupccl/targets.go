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
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
	descs []catalog.Descriptor,
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
	interestingIDs := make(map[descpb.ID]struct{}, len(descs))

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
	for _, i := range descs {
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
		starting, err := backupresolver.LoadAllDescs(ctx, execCfg.Codec, execCfg.DB, startTime)
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
			desc := catalogkv.NewBuilder(change.Desc).BuildExistingMutable()
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

	allRevs, err := storageccl.GetAllRevisions(ctx, db, startKey, endKey, startTime, endTime)
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

func ensureInterleavesIncluded(tables []catalog.Descriptor) error {
	inBackup := make(map[descpb.ID]bool, len(tables))
	for _, t := range tables {
		inBackup[t.GetID()] = true
	}

	for _, d := range tables {
		table, ok := d.(catalog.TableDescriptor)
		if !ok {
			continue
		}
		if err := catalog.ForEachIndex(table, catalog.IndexOpts{
			AddMutations: true,
		}, func(index catalog.Index) error {
			for i := 0; i < index.NumInterleaveAncestors(); i++ {
				a := index.GetInterleaveAncestor(i)
				if !inBackup[a.TableID] {
					return errors.Errorf(
						"cannot backup table %q without interleave parent (ID %d)", table.GetName(), a.TableID,
					)
				}
			}
			for i := 0; i < index.NumInterleavedBy(); i++ {
				c := index.GetInterleavedBy(i)
				if !inBackup[c.Table] {
					return errors.Errorf(
						"cannot backup table %q without interleave child table (ID %d)", table.GetName(), c.Table,
					)
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func lookupDatabaseID(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, name string,
) (descpb.ID, error) {
	found, id, err := catalogkv.LookupDatabaseID(ctx, txn, codec, name)
	if err != nil {
		return descpb.InvalidID, err
	}
	if !found {
		return descpb.InvalidID, errors.Errorf("could not find ID for database %s", name)
	}
	return id, nil
}

func fullClusterTargetsRestore(
	allDescs []catalog.Descriptor, lastBackupManifest BackupManifest,
) ([]catalog.Descriptor, []catalog.DatabaseDescriptor, []descpb.TenantInfoWithUsage, error) {
	fullClusterDescs, fullClusterDBs, err := fullClusterTargets(allDescs)
	if err != nil {
		return nil, nil, nil, err
	}
	filteredDescs := make([]catalog.Descriptor, 0, len(fullClusterDescs))
	for _, desc := range fullClusterDescs {
		if _, isDefaultDB := catalogkeys.DefaultUserDBs[desc.GetName()]; !isDefaultDB && desc.GetID() != keys.SystemDatabaseID {
			filteredDescs = append(filteredDescs, desc)
		}
	}
	filteredDBs := make([]catalog.DatabaseDescriptor, 0, len(fullClusterDBs))
	for _, db := range fullClusterDBs {
		if _, isDefaultDB := catalogkeys.DefaultUserDBs[db.GetName()]; !isDefaultDB && db.GetID() != keys.SystemDatabaseID {
			filteredDBs = append(filteredDBs, db)
		}
	}

	// Restore all tenants during full-cluster restore.
	tenants := lastBackupManifest.GetTenants()

	return filteredDescs, filteredDBs, tenants, nil
}

// fullClusterTargets returns all of the tableDescriptors to be included in a
// full cluster backup, and all the user databases.
func fullClusterTargets(
	allDescs []catalog.Descriptor,
) ([]catalog.Descriptor, []catalog.DatabaseDescriptor, error) {
	fullClusterDescs := make([]catalog.Descriptor, 0, len(allDescs))
	fullClusterDBs := make([]catalog.DatabaseDescriptor, 0)

	systemTablesToBackup := GetSystemTablesToIncludeInClusterBackup()

	for _, desc := range allDescs {
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
				// Add all user tables that are not in a DROP state.
				if !desc.Dropped() {
					fullClusterDescs = append(fullClusterDescs, desc)
				}
			}
		case catalog.SchemaDescriptor:
			fullClusterDescs = append(fullClusterDescs, desc)
		case catalog.TypeDescriptor:
			fullClusterDescs = append(fullClusterDescs, desc)
		}
	}
	return fullClusterDescs, fullClusterDBs, nil
}

// fullClusterTargetsBackup returns the same descriptors referenced in
// fullClusterTargets, but rather than returning the entire database
// descriptor as the second argument, it only returns their IDs.
func fullClusterTargetsBackup(
	ctx context.Context, execCfg *sql.ExecutorConfig, endTime hlc.Timestamp,
) ([]catalog.Descriptor, []descpb.ID, error) {
	allDescs, err := backupresolver.LoadAllDescs(ctx, execCfg.Codec, execCfg.DB, endTime)
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
	mainBackupManifests []BackupManifest,
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
			if table, _, _, _ := descpb.FromDescriptor(&desc); table != nil && table.Public() {
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
			if table, _, _, _ := descpb.FromDescriptor(&desc); table != nil && table.Public() {
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
			if table, _, _, _ := descpb.FromDescriptor(desc.Desc); table != nil && table.Public() {
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
//   - A list of all descriptors (table, type, database, schema) along with their
//     parent databases.
//   - A list of database descriptors IFF the user is restoring on the cluster or
//     database level.
//   - A map of table patterns to the resolved descriptor IFF the user is
//     restoring on the table leve.
//   - A list of tenants to restore, if applicable.
func selectTargets(
	ctx context.Context,
	p sql.PlanHookState,
	backupManifests []BackupManifest,
	targets tree.TargetList,
	descriptorCoverage tree.DescriptorCoverage,
	asOf hlc.Timestamp,
) ([]catalog.Descriptor, []catalog.DatabaseDescriptor, []descpb.TenantInfoWithUsage, error) {
	allDescs, lastBackupManifest := loadSQLDescsFromBackupsAtTime(backupManifests, asOf)

	if descriptorCoverage == tree.AllDescriptors {
		return fullClusterTargetsRestore(allDescs, lastBackupManifest)
	}

	if targets.Tenant != (roachpb.TenantID{}) {
		for _, tenant := range lastBackupManifest.GetTenants() {
			// TODO(dt): for now it is zero-or-one but when that changes, we should
			// either keep it sorted or build a set here.
			if tenant.ID == targets.Tenant.ToUint64() {
				return nil, nil, []descpb.TenantInfoWithUsage{tenant}, nil
			}
		}
		return nil, nil, nil, errors.Errorf("tenant %d not in backup", targets.Tenant.ToUint64())
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

	introducedSpanFrontier, err := createIntroducedSpanFrontier(backupManifests, hlc.Timestamp{})
	if err != nil {
		return BackupTableEntry{}, err
	}

	entry := makeSimpleImportSpans(
		[]roachpb.Span{tablePrimaryIndexSpan},
		backupManifests,
		nil, /*backupLocalityInfo*/
		introducedSpanFrontier,
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
				d := catalogkv.NewBuilder(rev.Desc).BuildExistingMutable()
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
	codec keys.SQLCodec,
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
		if regionName == descpb.RegionName(tree.PrimaryRegionNotSpecifiedName) {
			// REGIONAL BY PRIMARY REGION tables are allowed since they do not
			// reference a particular region.
			return nil
		}

		// For REGION BY TABLE IN <region> tables, allow the restore if the
		// database has the region.
		regionEnumID := database.GetRegionConfig().RegionEnumID
		regionEnum, err := catalogkv.MustGetTypeDescByID(ctx, txn, codec, regionEnumID)
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
