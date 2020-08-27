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

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type descriptorsMatched struct {
	// all tables that match targets plus their parent databases.
	descs []sqlbase.Descriptor

	// the databases from which all tables were matched (eg a.* or DATABASE a).
	expandedDB []sqlbase.ID

	// explicitly requested DBs (e.g. DATABASE a).
	requestedDBs []*sqlbase.DatabaseDescriptor
}

func (d descriptorsMatched) checkExpansions(coveredDBs []sqlbase.ID) error {
	covered := make(map[sqlbase.ID]bool)
	for _, i := range coveredDBs {
		covered[i] = true
	}
	for _, i := range d.requestedDBs {
		if !covered[i.ID] {
			return errors.Errorf("cannot RESTORE DATABASE from a backup of individual tables (use SHOW BACKUP to determine available tables)")
		}
	}
	for _, i := range d.expandedDB {
		if !covered[i] {
			return errors.Errorf("cannot RESTORE <database>.* from a backup of individual tables (use SHOW BACKUP to determine available tables)")
		}
	}
	return nil
}

// descriptorResolver is the helper struct that enables reuse of the
// standard name resolution algorithm.
type descriptorResolver struct {
	descByID map[sqlbase.ID]sqlbase.Descriptor
	// Map: db name -> dbID
	dbsByName map[string]sqlbase.ID
	// Map: dbID -> obj name -> obj ID
	objsByName map[sqlbase.ID]map[string]sqlbase.ID
}

// LookupSchema implements the tree.TableNameTargetResolver interface.
func (r *descriptorResolver) LookupSchema(
	_ context.Context, dbName, scName string,
) (bool, tree.SchemaMeta, error) {
	if scName != tree.PublicSchema {
		return false, nil, nil
	}
	if dbID, ok := r.dbsByName[dbName]; ok {
		return true, r.descByID[dbID], nil
	}
	return false, nil, nil
}

// LookupObject implements the tree.TableNameExistingResolver interface.
func (r *descriptorResolver) LookupObject(
	_ context.Context, flags tree.ObjectLookupFlags, dbName, scName, obName string,
) (bool, tree.NameResolutionResult, error) {
	if flags.RequireMutable {
		panic("did not expect request for mutable descriptor")
	}
	if scName != tree.PublicSchema {
		return false, nil, nil
	}
	dbID, ok := r.dbsByName[dbName]
	if !ok {
		return false, nil, nil
	}
	if objMap, ok := r.objsByName[dbID]; ok {
		if objID, ok := objMap[obName]; ok {
			return true, r.descByID[objID], nil
		}
	}
	return false, nil, nil
}

// newDescriptorResolver prepares a descriptorResolver for the given
// known set of descriptors.
func newDescriptorResolver(descs []sqlbase.Descriptor) (*descriptorResolver, error) {
	r := &descriptorResolver{
		descByID:   make(map[sqlbase.ID]sqlbase.Descriptor),
		dbsByName:  make(map[string]sqlbase.ID),
		objsByName: make(map[sqlbase.ID]map[string]sqlbase.ID),
	}

	// Iterate to find the databases first. We need that because we also
	// check the ParentID for tables, and all the valid parents must be
	// known before we start to check that.
	for _, desc := range descs {
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			if _, ok := r.dbsByName[dbDesc.Name]; ok {
				return nil, errors.Errorf("duplicate database name: %q used for ID %d and %d",
					dbDesc.Name, r.dbsByName[dbDesc.Name], dbDesc.ID)
			}
			r.dbsByName[dbDesc.Name] = dbDesc.ID
		}

		// Incidentally, also remember all the descriptors by ID.
		if prevDesc, ok := r.descByID[desc.GetID()]; ok {
			return nil, errors.Errorf("duplicate descriptor ID: %d used by %q and %q",
				desc.GetID(), prevDesc.GetName(), desc.GetName())
		}
		r.descByID[desc.GetID()] = desc
	}
	// Now on to the tables.
	for _, desc := range descs {
		if tbDesc := desc.Table(hlc.Timestamp{}); tbDesc != nil {
			if tbDesc.Dropped() || tbDesc.Temporary {
				continue
			}
			parentDesc, ok := r.descByID[tbDesc.ParentID]
			if !ok {
				return nil, errors.Errorf("table %q has unknown ParentID %d", tbDesc.Name, tbDesc.ParentID)
			}
			if _, ok := r.dbsByName[parentDesc.GetName()]; !ok {
				return nil, errors.Errorf("table %q's ParentID %d (%q) is not a database",
					tbDesc.Name, tbDesc.ParentID, parentDesc.GetName())
			}
			objMap := r.objsByName[parentDesc.GetID()]
			if objMap == nil {
				objMap = make(map[string]sqlbase.ID)
			}
			if _, ok := objMap[tbDesc.Name]; ok {
				return nil, errors.Errorf("duplicate table name: %q.%q used for ID %d and %d",
					parentDesc.GetName(), tbDesc.Name, tbDesc.ID, objMap[tbDesc.Name])
			}
			objMap[tbDesc.Name] = tbDesc.ID
			r.objsByName[parentDesc.GetID()] = objMap
		}
	}

	return r, nil
}

// descriptorsMatchingTargets returns the descriptors that match the targets. A
// database descriptor is included in this set if it matches the targets (or the
// session database) or if one of its tables matches the targets. All expanded
// DBs, via either `foo.*` or `DATABASE foo` are noted, as are those explicitly
// named as DBs (e.g. with `DATABASE foo`, not `foo.*`). These distinctions are
// used e.g. by RESTORE.
//
// This is guaranteed to not return duplicates.
func descriptorsMatchingTargets(
	ctx context.Context,
	currentDatabase string,
	searchPath sessiondata.SearchPath,
	descriptors []sqlbase.Descriptor,
	targets tree.TargetList,
) (descriptorsMatched, error) {
	// TODO(dan): once CockroachDB supports schemas in addition to
	// catalogs, then this method will need to support it.

	ret := descriptorsMatched{}

	resolver, err := newDescriptorResolver(descriptors)
	if err != nil {
		return ret, err
	}

	alreadyRequestedDBs := make(map[sqlbase.ID]struct{})
	alreadyExpandedDBs := make(map[sqlbase.ID]struct{})
	// Process all the DATABASE requests.
	for _, d := range targets.Databases {
		dbID, ok := resolver.dbsByName[string(d)]
		if !ok {
			return ret, errors.Errorf("unknown database %q", d)
		}
		if _, ok := alreadyRequestedDBs[dbID]; !ok {
			desc := resolver.descByID[dbID]
			ret.descs = append(ret.descs, desc)
			ret.requestedDBs = append(ret.requestedDBs, desc.GetDatabase())
			ret.expandedDB = append(ret.expandedDB, dbID)
			alreadyRequestedDBs[dbID] = struct{}{}
			alreadyExpandedDBs[dbID] = struct{}{}
		}
	}

	// Process all the TABLE requests.
	// Pulling in a table needs to pull in the underlying database too.
	alreadyRequestedTables := make(map[sqlbase.ID]struct{})
	for _, pattern := range targets.Tables {
		var err error
		pattern, err = pattern.NormalizeTablePattern()
		if err != nil {
			return ret, err
		}

		switch p := pattern.(type) {
		case *tree.TableName:
			found, descI, err := p.ResolveExisting(ctx, resolver, tree.ObjectLookupFlags{}, currentDatabase, searchPath)
			if err != nil {
				return ret, err
			}
			doesNotExistErr := errors.Errorf(`table %q does not exist`, tree.ErrString(p))
			if !found {
				return ret, doesNotExistErr
			}
			desc := descI.(sqlbase.Descriptor)
			tableDesc := desc.Table(hlc.Timestamp{})

			// Verify that the table is in the correct state.
			if err := sql.FilterTableState(tableDesc); err != nil {
				// Return a does not exist error if explicitly asking for this table.
				return ret, doesNotExistErr
			}

			// If the parent database is not requested already, request it now.
			parentID := tableDesc.GetParentID()
			if _, ok := alreadyRequestedDBs[parentID]; !ok {
				parentDesc := resolver.descByID[parentID]
				ret.descs = append(ret.descs, parentDesc)
				alreadyRequestedDBs[parentID] = struct{}{}
			}
			// Then request the table itself.
			if _, ok := alreadyRequestedTables[desc.GetID()]; !ok {
				alreadyRequestedTables[desc.GetID()] = struct{}{}
				ret.descs = append(ret.descs, desc)
			}

		case *tree.AllTablesSelector:
			found, descI, err := p.TableNamePrefix.Resolve(ctx, resolver, currentDatabase, searchPath)
			if err != nil {
				return ret, err
			}
			if !found {
				return ret, sqlbase.NewInvalidWildcardError(tree.ErrString(p))
			}
			desc := descI.(sqlbase.Descriptor)

			// If the database is not requested already, request it now.
			dbID := desc.GetID()
			if _, ok := alreadyRequestedDBs[dbID]; !ok {
				ret.descs = append(ret.descs, desc)
				alreadyRequestedDBs[dbID] = struct{}{}
			}

			// Then request the expansion.
			if _, ok := alreadyExpandedDBs[desc.GetID()]; !ok {
				ret.expandedDB = append(ret.expandedDB, desc.GetID())
				alreadyExpandedDBs[desc.GetID()] = struct{}{}
			}

		default:
			return ret, errors.Errorf("unknown pattern %T: %+v", pattern, pattern)
		}
	}

	// Then process the database expansions.
	for dbID := range alreadyExpandedDBs {
		for _, tblID := range resolver.objsByName[dbID] {
			desc := resolver.descByID[tblID]
			table := desc.Table(hlc.Timestamp{})
			if err := sql.FilterTableState(table); err != nil {
				// Don't include this table in the expansion since it's not in a valid
				// state. Silently fail since this table was not directly requested,
				// but was just part of an expansion.
				continue
			}
			if _, ok := alreadyRequestedTables[tblID]; !ok {
				ret.descs = append(ret.descs, desc)
			}
		}
	}

	return ret, nil
}

// getRelevantDescChanges finds the changes between start and end time to the
// SQL descriptors matching `descs` or `expandedDBs`, ordered by time. A
// descriptor revision matches if it is an earlier revision of a descriptor in
// descs (same ID) or has parentID in `expanded`. Deleted descriptors are
// represented as nil. Fills in the `priorIDs` map in the process, which maps
// a descriptor the the ID by which it was previously known (e.g pre-TRUNCATE).
func getRelevantDescChanges(
	ctx context.Context,
	db *kv.DB,
	startTime, endTime hlc.Timestamp,
	descs []sqlbase.Descriptor,
	expanded []sqlbase.ID,
	priorIDs map[sqlbase.ID]sqlbase.ID,
	descriptorCoverage tree.DescriptorCoverage,
) ([]BackupManifest_DescriptorRevision, error) {

	allChanges, err := getAllDescChanges(ctx, db, startTime, endTime, priorIDs)
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
	interestingIDs := make(map[sqlbase.ID]struct{}, len(descs))

	// The descriptors that currently (endTime) match the target spec (desc) are
	// obviously interesting to our backup.
	for _, i := range descs {
		interestingIDs[i.GetID()] = struct{}{}
		if t := i.Table(hlc.Timestamp{}); t != nil {
			for j := t.ReplacementOf.ID; j != sqlbase.InvalidID; j = priorIDs[j] {
				interestingIDs[j] = struct{}{}
			}
		}
	}

	// We're also interested in any desc that belonged to a DB we're backing up.
	// We'll start by looking at all descriptors as of the beginning of the
	// interval and add to the set of IDs that we are interested any descriptor that
	// belongs to one of the parents we care about.
	interestingParents := make(map[sqlbase.ID]struct{}, len(expanded))
	for _, i := range expanded {
		interestingParents[i] = struct{}{}
	}

	if !startTime.IsEmpty() {
		starting, err := loadAllDescs(ctx, db, startTime)
		if err != nil {
			return nil, err
		}
		for _, i := range starting {
			if table := i.Table(hlc.Timestamp{}); table != nil {
				// We need to add to interestingIDs so that if we later see a delete for
				// this ID we still know it is interesting to us, even though we will not
				// have a parentID at that point (since the delete is a nil desc).
				if _, ok := interestingParents[table.ParentID]; ok {
					interestingIDs[table.ID] = struct{}{}
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
				initial := BackupManifest_DescriptorRevision{Time: startTime, ID: i.GetID(), Desc: &desc}
				interestingChanges = append(interestingChanges, initial)
			}
		}
	}

	isInterestingID := func(id sqlbase.ID) bool {
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
			if table := change.Desc.Table(hlc.Timestamp{}); table != nil {
				if _, ok := interestingParents[table.ParentID]; ok {
					interestingIDs[table.ID] = struct{}{}
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
	db *kv.DB,
	startTime, endTime hlc.Timestamp,
	priorIDs map[sqlbase.ID]sqlbase.ID,
) ([]BackupManifest_DescriptorRevision, error) {
	startKey := roachpb.Key(keys.MakeTablePrefix(keys.DescriptorTableID))
	endKey := startKey.PrefixEnd()

	allRevs, err := storageccl.GetAllRevisions(ctx, db, startKey, endKey, startTime, endTime)
	if err != nil {
		return nil, err
	}

	var res []BackupManifest_DescriptorRevision

	for _, revs := range allRevs {
		id, err := keys.DecodeDescMetadataID(revs.Key)
		if err != nil {
			return nil, err
		}
		for _, rev := range revs.Values {
			r := BackupManifest_DescriptorRevision{ID: sqlbase.ID(id), Time: rev.Timestamp}
			if len(rev.RawBytes) != 0 {
				var desc sqlbase.Descriptor
				if err := rev.GetProto(&desc); err != nil {
					return nil, err
				}
				r.Desc = &desc
				t := desc.Table(rev.Timestamp)
				if t != nil && t.ReplacementOf.ID != sqlbase.InvalidID {
					priorIDs[t.ID] = t.ReplacementOf.ID
				}
			}
			res = append(res, r)
		}
	}
	return res, nil
}

func allSQLDescriptors(ctx context.Context, txn *kv.Txn) ([]sqlbase.Descriptor, error) {
	startKey := roachpb.Key(keys.MakeTablePrefix(keys.DescriptorTableID))
	endKey := startKey.PrefixEnd()
	rows, err := txn.Scan(ctx, startKey, endKey, 0)
	if err != nil {
		return nil, err
	}

	sqlDescs := make([]sqlbase.Descriptor, len(rows))
	for i, row := range rows {
		if err := row.ValueProto(&sqlDescs[i]); err != nil {
			return nil, errors.NewAssertionErrorWithWrappedErrf(err,
				"%s: unable to unmarshal SQL descriptor", row.Key)
		}
		if row.Value != nil {
			sqlDescs[i].Table(row.Value.Timestamp)
		}
	}
	return sqlDescs, nil
}

func ensureInterleavesIncluded(tables []*sqlbase.TableDescriptor) error {
	inBackup := make(map[sqlbase.ID]bool, len(tables))
	for _, t := range tables {
		inBackup[t.ID] = true
	}

	for _, table := range tables {
		if err := table.ForeachNonDropIndex(func(index *sqlbase.IndexDescriptor) error {
			for _, a := range index.Interleave.Ancestors {
				if !inBackup[a.TableID] {
					return errors.Errorf(
						"cannot backup table %q without interleave parent (ID %d)", table.Name, a.TableID,
					)
				}
			}
			for _, c := range index.InterleavedBy {
				if !inBackup[c.Table] {
					return errors.Errorf(
						"cannot backup table %q without interleave child table (ID %d)", table.Name, c.Table,
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

func loadAllDescs(
	ctx context.Context, db *kv.DB, asOf hlc.Timestamp,
) ([]sqlbase.Descriptor, error) {
	var allDescs []sqlbase.Descriptor
	if err := db.Txn(
		ctx,
		func(ctx context.Context, txn *kv.Txn) error {
			var err error
			txn.SetFixedTimestamp(ctx, asOf)
			allDescs, err = allSQLDescriptors(ctx, txn)
			return err
		}); err != nil {
		return nil, err
	}
	return allDescs, nil
}

// ResolveTargetsToDescriptors performs name resolution on a set of targets and
// returns the resulting descriptors.
func ResolveTargetsToDescriptors(
	ctx context.Context,
	p sql.PlanHookState,
	endTime hlc.Timestamp,
	targets tree.TargetList,
	descriptorCoverage tree.DescriptorCoverage,
) ([]sqlbase.Descriptor, []sqlbase.ID, error) {
	allDescs, err := loadAllDescs(ctx, p.ExecCfg().DB, endTime)
	if err != nil {
		return nil, nil, err
	}

	if descriptorCoverage == tree.AllDescriptors {
		return fullClusterTargetsBackup(allDescs)
	}

	var matched descriptorsMatched
	if matched, err = descriptorsMatchingTargets(ctx,
		p.CurrentDatabase(), p.CurrentSearchPath(), allDescs, targets); err != nil {
		return nil, nil, err
	}

	// Ensure interleaved tables appear after their parent. Since parents must be
	// created before their children, simply sorting by ID accomplishes this.
	sort.Slice(matched.descs, func(i, j int) bool { return matched.descs[i].GetID() < matched.descs[j].GetID() })
	return matched.descs, matched.expandedDB, nil
}

// fullClusterTargetsBackup returns the same descriptors referenced in
// fullClusterTargets, but rather than returning the entire database
// descriptor as the second argument, it only returns their IDs.
func fullClusterTargetsBackup(
	allDescs []sqlbase.Descriptor,
) ([]sqlbase.Descriptor, []sqlbase.ID, error) {
	fullClusterDescs, fullClusterDBs, err := fullClusterTargets(allDescs)
	if err != nil {
		return nil, nil, err
	}

	fullClusterDBIDs := make([]sqlbase.ID, 0)
	for _, desc := range fullClusterDBs {
		fullClusterDBIDs = append(fullClusterDBIDs, desc.GetID())
	}
	return fullClusterDescs, fullClusterDBIDs, nil
}

// fullClusterTargets returns all of the tableDescriptors to be included in a
// full cluster backup, and all the user databases.
func fullClusterTargets(
	allDescs []sqlbase.Descriptor,
) ([]sqlbase.Descriptor, []*sqlbase.DatabaseDescriptor, error) {
	fullClusterDescs := make([]sqlbase.Descriptor, 0, len(allDescs))
	fullClusterDBs := make([]*sqlbase.DatabaseDescriptor, 0)

	systemTablesToBackup := make(map[string]struct{}, len(fullClusterSystemTables))
	for _, tableName := range fullClusterSystemTables {
		systemTablesToBackup[tableName] = struct{}{}
	}

	for _, desc := range allDescs {
		if dbDesc := desc.GetDatabase(); dbDesc != nil {
			fullClusterDescs = append(fullClusterDescs, desc)
			if dbDesc.ID != sqlbase.SystemDB.ID {
				// The only database that isn't being fully backed up is the system DB.
				fullClusterDBs = append(fullClusterDBs, dbDesc)
			}
		}
		if tableDesc := desc.Table(hlc.Timestamp{}); tableDesc != nil {
			if tableDesc.ParentID == sqlbase.SystemDB.ID {
				// Add only the system tables that we plan to include in a full cluster
				// backup.
				if _, ok := systemTablesToBackup[tableDesc.Name]; ok {
					fullClusterDescs = append(fullClusterDescs, desc)
				}
			} else {
				// Add all user tables that are not in a DROP state.
				if tableDesc.State != sqlbase.TableDescriptor_DROP {
					fullClusterDescs = append(fullClusterDescs, desc)
				}
			}
		}
	}
	return fullClusterDescs, fullClusterDBs, nil
}

func lookupDatabaseID(ctx context.Context, txn *kv.Txn, name string) (sqlbase.ID, error) {
	found, id, err := sqlbase.LookupDatabaseID(ctx, txn, name)
	if err != nil {
		return sqlbase.InvalidID, err
	}
	if !found {
		return sqlbase.InvalidID, errors.Errorf("could not find ID for database %s", name)
	}
	return id, nil
}

// CheckTableExists returns an error if a table already exists with given
// parent and name.
func CheckTableExists(
	ctx context.Context, settings *cluster.Settings, txn *kv.Txn, parentID sqlbase.ID, name string,
) error {
	found, _, err := sqlbase.LookupPublicTableID(ctx, settings, txn, parentID, name)
	if err != nil {
		return err
	}
	if found {
		return sqlbase.NewRelationAlreadyExistsError(name)
	}
	return nil
}

func fullClusterTargetsRestore(
	allDescs []sqlbase.Descriptor,
) ([]sqlbase.Descriptor, []*sqlbase.DatabaseDescriptor, error) {
	fullClusterDescs, fullClusterDBs, err := fullClusterTargets(allDescs)
	if err != nil {
		return nil, nil, err
	}
	filteredDescs := make([]sqlbase.Descriptor, 0, len(fullClusterDescs))
	for _, desc := range fullClusterDescs {
		if _, isDefaultDB := sql.DefaultUserDBs[desc.GetName()]; !isDefaultDB && desc.GetID() != sqlbase.SystemDB.ID {
			filteredDescs = append(filteredDescs, desc)
		}
	}
	filteredDBs := make([]*sqlbase.DatabaseDescriptor, 0, len(fullClusterDBs))
	for _, db := range fullClusterDBs {
		if _, isDefaultDB := sql.DefaultUserDBs[db.GetName()]; !isDefaultDB && db.GetID() != sqlbase.SystemDB.ID {
			filteredDBs = append(filteredDBs, db)
		}
	}

	return filteredDescs, filteredDBs, nil
}

func selectTargets(
	ctx context.Context,
	p sql.PlanHookState,
	backupManifests []BackupManifest,
	targets tree.TargetList,
	descriptorCoverage tree.DescriptorCoverage,
	asOf hlc.Timestamp,
) ([]sqlbase.Descriptor, []*sqlbase.DatabaseDescriptor, error) {
	allDescs, lastBackupManifest := loadSQLDescsFromBackupsAtTime(backupManifests, asOf)

	if descriptorCoverage == tree.AllDescriptors {
		return fullClusterTargetsRestore(allDescs)
	}

	matched, err := descriptorsMatchingTargets(ctx,
		p.CurrentDatabase(), p.CurrentSearchPath(), allDescs, targets)
	if err != nil {
		return nil, nil, err
	}

	if len(matched.descs) == 0 {
		return nil, nil, errors.Errorf("no tables or databases matched the given targets: %s", tree.ErrString(&targets))
	}

	if lastBackupManifest.FormatVersion >= BackupFormatDescriptorTrackingVersion {
		if err := matched.checkExpansions(lastBackupManifest.CompleteDbs); err != nil {
			return nil, nil, err
		}
	}

	return matched.descs, matched.requestedDBs, nil
}
