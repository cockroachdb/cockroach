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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	codec keys.SQLCodec,
	db *kv.DB,
	startTime, endTime hlc.Timestamp,
	descs []catalog.Descriptor,
	expanded []descpb.ID,
	priorIDs map[descpb.ID]descpb.ID,
	descriptorCoverage tree.DescriptorCoverage,
) ([]BackupManifest_DescriptorRevision, error) {

	allChanges, err := getAllDescChanges(ctx, codec, db, startTime, endTime, priorIDs)
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
		starting, err := backupbase.LoadAllDescs(ctx, codec, db, startTime)
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
			if _, ok := interestingIDs[i.GetID()]; ok {
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

	isInterestingID := func(id descpb.ID) bool {
		// We're interested in changes to all descriptors if we're targeting all
		// descriptors except for the system database itself.
		if descriptorCoverage == tree.AllDescriptors && id != keys.SystemDatabaseID {
			return true
		}
		// A change to an ID that we're interested in is obviously interesting.
		if _, ok := interestingIDs[id]; ok {
			return true
		}
		return false
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
				if t != nil && t.ReplacementOf.ID != descpb.InvalidID {
					priorIDs[t.ID] = t.ReplacementOf.ID
				}
			}
			res = append(res, r)
		}
	}
	return res, nil
}

// validateMultiRegionBackup validates that for all tables included in the
// backup, their parent database is also being backed up. For multi-region
// tables, we require that the parent database is included to ensure that the
// multi-region enum (which is required for multi-region tables) is also
// present.
func validateMultiRegionBackup(
	backupStmt *annotatedBackupStatement,
	descs []catalog.Descriptor,
	tables []catalog.TableDescriptor,
) error {
	// We only need to block in the table backup case, so there's nothing to do
	// if we're running a cluster backup.
	if backupStmt.Coverage() == tree.AllDescriptors {
		return nil
	}
	// We build a map of the target databases here because the supplied list of
	// descriptors contains ALL database descriptors for the corresponding
	// tables (regardless of whether or not the databases are included in the
	// backup targets list). The map helps below so that we're not looping over
	// the descriptors slice for every table.
	databaseTargetIDs := map[descpb.ID]struct{}{}
	databaseTargetNames := map[tree.Name]struct{}{}
	for _, name := range backupStmt.Targets.Databases {
		databaseTargetNames[name] = struct{}{}
	}

	for _, desc := range descs {
		switch desc.(type) {
		case catalog.DatabaseDescriptor:
			// If the database descriptor found is included in the targets list, add
			// it to the targetsID map.
			if _, ok := databaseTargetNames[tree.Name(desc.GetName())]; ok {
				databaseTargetIDs[desc.GetID()] = struct{}{}
			}
		}
	}

	// Look through the list of tables and for every multi-region table, see if
	// its parent database is being backed up.
	for _, table := range tables {
		if table.GetLocalityConfig() != nil {
			if _, ok := databaseTargetIDs[table.GetParentID()]; !ok {
				// Found a table which is being backed up without its parent database.
				return pgerror.Newf(pgcode.FeatureNotSupported,
					"cannot backup individual table %d from multi-region database %d",
					table.GetID(),
					table.GetParentID(),
				)
			}
		}
	}
	return nil
}

func ensureInterleavesIncluded(tables []catalog.TableDescriptor) error {
	inBackup := make(map[descpb.ID]bool, len(tables))
	for _, t := range tables {
		inBackup[t.GetID()] = true
	}

	for _, table := range tables {
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
) ([]catalog.Descriptor, []catalog.DatabaseDescriptor, []descpb.TenantInfo, error) {
	fullClusterDescs, fullClusterDBs, err := backupbase.FullClusterTargets(allDescs)
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
	tenants := lastBackupManifest.Tenants

	return filteredDescs, filteredDBs, tenants, nil
}

func selectTargets(
	ctx context.Context,
	p sql.PlanHookState,
	backupManifests []BackupManifest,
	targets tree.TargetList,
	descriptorCoverage tree.DescriptorCoverage,
	asOf hlc.Timestamp,
) ([]catalog.Descriptor, []catalog.DatabaseDescriptor, []descpb.TenantInfo, error) {
	allDescs, lastBackupManifest := loadSQLDescsFromBackupsAtTime(backupManifests, asOf)

	if descriptorCoverage == tree.AllDescriptors {
		return fullClusterTargetsRestore(allDescs, lastBackupManifest)
	}

	if targets.Tenant != (roachpb.TenantID{}) {
		for _, tenant := range lastBackupManifest.Tenants {
			// TODO(dt): for now it is zero-or-one but when that changes, we should
			// either keep it sorted or build a set here.
			if tenant.ID == targets.Tenant.ToUint64() {
				return nil, nil, []descpb.TenantInfo{tenant}, nil
			}
		}
		return nil, nil, nil, errors.Errorf("tenant %d not in backup", targets.Tenant.ToUint64())
	}

	matched, err := backupbase.DescriptorsMatchingTargets(ctx,
		p.CurrentDatabase(), p.CurrentSearchPath(), allDescs, targets)
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
