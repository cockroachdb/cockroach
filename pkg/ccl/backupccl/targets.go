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
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type descriptorsMatched struct {
	// All descriptors that match targets plus their parent databases.
	descs []catalog.Descriptor

	// The databases from which all tables were matched (eg a.* or DATABASE a).
	expandedDB []descpb.ID

	// Explicitly requested DBs (e.g. DATABASE a).
	requestedDBs []catalog.DatabaseDescriptor
}

func (d descriptorsMatched) checkExpansions(coveredDBs []descpb.ID) error {
	covered := make(map[descpb.ID]bool)
	for _, i := range coveredDBs {
		covered[i] = true
	}
	for _, i := range d.requestedDBs {
		if !covered[i.GetID()] {
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
	descByID map[descpb.ID]catalog.Descriptor
	// Map: db name -> dbID
	dbsByName map[string]descpb.ID
	// Map: dbID -> schema name -> schemaID
	schemasByName map[descpb.ID]map[string]descpb.ID
	// Map: dbID -> schema name -> obj name -> obj ID
	objsByName map[descpb.ID]map[string]map[string]descpb.ID
}

// LookupSchema implements the tree.ObjectNameTargetResolver interface.
func (r *descriptorResolver) LookupSchema(
	_ context.Context, dbName, scName string,
) (bool, tree.SchemaMeta, error) {
	dbID, ok := r.dbsByName[dbName]
	if !ok {
		return false, nil, nil
	}
	schemas := r.objsByName[dbID]
	if _, ok := schemas[scName]; ok {
		// TODO (rohany): Not sure if we want to change this to also
		//  use the resolved schema struct.
		if dbDesc, ok := r.descByID[dbID].(catalog.DatabaseDescriptor); ok {
			return true, dbDesc, nil
		}
	}
	return false, nil, nil
}

// LookupObject implements the tree.ObjectNameExistingResolver interface.
func (r *descriptorResolver) LookupObject(
	_ context.Context, flags tree.ObjectLookupFlags, dbName, scName, obName string,
) (bool, tree.NameResolutionResult, error) {
	if flags.RequireMutable {
		panic("did not expect request for mutable descriptor")
	}
	dbID, ok := r.dbsByName[dbName]
	if !ok {
		return false, nil, nil
	}
	if scMap, ok := r.objsByName[dbID]; ok {
		if objMap, ok := scMap[scName]; ok {
			if objID, ok := objMap[obName]; ok {
				return true, r.descByID[objID], nil
			}
		}
	}
	return false, nil, nil
}

// newDescriptorResolver prepares a descriptorResolver for the given
// known set of descriptors.
func newDescriptorResolver(descs []catalog.Descriptor) (*descriptorResolver, error) {
	r := &descriptorResolver{
		descByID:      make(map[descpb.ID]catalog.Descriptor),
		schemasByName: make(map[descpb.ID]map[string]descpb.ID),
		dbsByName:     make(map[string]descpb.ID),
		objsByName:    make(map[descpb.ID]map[string]map[string]descpb.ID),
	}

	// Iterate to find the databases first. We need that because we also
	// check the ParentID for tables, and all the valid parents must be
	// known before we start to check that.
	for _, desc := range descs {
		if _, isDB := desc.(catalog.DatabaseDescriptor); isDB {
			if _, ok := r.dbsByName[desc.GetName()]; ok {
				return nil, errors.Errorf("duplicate database name: %q used for ID %d and %d",
					desc.GetName(), r.dbsByName[desc.GetName()], desc.GetID())
			}
			r.dbsByName[desc.GetName()] = desc.GetID()
			r.objsByName[desc.GetID()] = make(map[string]map[string]descpb.ID)
			r.schemasByName[desc.GetID()] = make(map[string]descpb.ID)
			// Always add an entry for the public schema.
			r.objsByName[desc.GetID()][tree.PublicSchema] = make(map[string]descpb.ID)
			r.schemasByName[desc.GetID()][tree.PublicSchema] = keys.PublicSchemaID
		}

		// Incidentally, also remember all the descriptors by ID.
		if prevDesc, ok := r.descByID[desc.GetID()]; ok {
			return nil, errors.Errorf("duplicate descriptor ID: %d used by %q and %q",
				desc.GetID(), prevDesc.GetName(), desc.GetName())
		}
		r.descByID[desc.GetID()] = desc
	}

	// Add all schemas to the resolver.
	for _, desc := range descs {
		if sc, ok := desc.(catalog.SchemaDescriptor); ok {
			schemaMap := r.objsByName[sc.GetParentID()]
			if schemaMap == nil {
				schemaMap = make(map[string]map[string]descpb.ID)
			}
			schemaMap[sc.GetName()] = make(map[string]descpb.ID)
			r.objsByName[sc.GetParentID()] = schemaMap

			schemaNameMap := r.schemasByName[sc.GetParentID()]
			if schemaNameMap == nil {
				schemaNameMap = make(map[string]descpb.ID)
			}
			schemaNameMap[sc.GetName()] = sc.GetID()
			r.schemasByName[sc.GetParentID()] = schemaNameMap
		}
	}

	// registerDesc is a closure that registers a Descriptor into the resolver's
	// object registry.
	registerDesc := func(parentID descpb.ID, desc catalog.Descriptor, kind string) error {
		parentDesc, ok := r.descByID[parentID]
		if !ok {
			return errors.Errorf("%s %q has unknown ParentID %d", kind, desc.GetName(), parentID)
		}
		if _, ok := r.dbsByName[parentDesc.GetName()]; !ok {
			return errors.Errorf("%s %q's ParentID %d (%q) is not a database",
				kind, desc.GetName(), parentID, parentDesc.GetName())
		}

		// Look up what schema this descriptor belongs under.
		schemaMap := r.objsByName[parentDesc.GetID()]
		scID := desc.GetParentSchemaID()
		var scName string
		if scID == keys.PublicSchemaID {
			scName = tree.PublicSchema
		} else {
			scDescI, ok := r.descByID[scID]
			if !ok {
				return errors.Errorf("schema %d not found for desc %d", scID, desc.GetID())
			}
			scDesc, ok := scDescI.(catalog.SchemaDescriptor)
			if !ok {
				return errors.Errorf("descriptor %d is not a schema", scDescI.GetID())
			}
			scName = scDesc.GetName()
		}

		// Create an entry for the descriptor.
		objMap := schemaMap[scName]
		if objMap == nil {
			objMap = make(map[string]descpb.ID)
		}
		if _, ok := objMap[desc.GetName()]; ok {
			return errors.Errorf("duplicate %s name: %q.%q.%q used for ID %d and %d",
				kind, parentDesc.GetName(), scName, desc.GetName(), desc.GetID(), objMap[desc.GetName()])
		}
		objMap[desc.GetName()] = desc.GetID()
		r.objsByName[parentDesc.GetID()][scName] = objMap
		return nil
	}

	// Now on to the remaining descriptors.
	for _, desc := range descs {
		if desc.Dropped() {
			continue
		}
		var typeToRegister string
		switch desc := desc.(type) {
		case catalog.TableDescriptor:
			if desc.TableDesc().Temporary {
				continue
			}
			typeToRegister = "table"
		case catalog.TypeDescriptor:
			typeToRegister = "type"
		}
		if typeToRegister != "" {
			if err := registerDesc(desc.GetParentID(), desc, typeToRegister); err != nil {
				return nil, err
			}
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
	descriptors []catalog.Descriptor,
	targets tree.TargetList,
) (descriptorsMatched, error) {
	ret := descriptorsMatched{}

	resolver, err := newDescriptorResolver(descriptors)
	if err != nil {
		return ret, err
	}

	alreadyRequestedDBs := make(map[descpb.ID]struct{})
	alreadyExpandedDBs := make(map[descpb.ID]struct{})
	// Process all the DATABASE requests.
	for _, d := range targets.Databases {
		dbID, ok := resolver.dbsByName[string(d)]
		if !ok {
			return ret, errors.Errorf("unknown database %q", d)
		}
		if _, ok := alreadyRequestedDBs[dbID]; !ok {
			desc := resolver.descByID[dbID]
			ret.descs = append(ret.descs, desc)
			ret.requestedDBs = append(ret.requestedDBs,
				desc.(catalog.DatabaseDescriptor))
			ret.expandedDB = append(ret.expandedDB, dbID)
			alreadyRequestedDBs[dbID] = struct{}{}
			alreadyExpandedDBs[dbID] = struct{}{}
		}
	}

	alreadyRequestedSchemas := make(map[descpb.ID]struct{})
	maybeAddSchemaDesc := func(id descpb.ID, requirePublic bool) error {
		// Only add user defined schemas.
		if id == keys.PublicSchemaID {
			return nil
		}
		if _, ok := alreadyRequestedSchemas[id]; !ok {
			schemaDesc := resolver.descByID[id]
			if err := catalog.FilterDescriptorState(
				schemaDesc, tree.CommonLookupFlags{},
			); err != nil {
				if requirePublic {
					return errors.Wrapf(err, "schema %d was expected to be PUBLIC", id)
				}
				// If the schema is not public, but we don't require it to be, ignore
				// it.
				return nil
			}
			alreadyRequestedSchemas[id] = struct{}{}
			ret.descs = append(ret.descs, resolver.descByID[id])
		}

		return nil
	}
	getSchemaIDByName := func(scName string, dbID descpb.ID) (descpb.ID, error) {
		schemas, ok := resolver.schemasByName[dbID]
		if !ok {
			return 0, errors.Newf("database with ID %d not found", dbID)
		}
		schemaID, ok := schemas[scName]
		if !ok {
			return 0, errors.Newf("schema with name %s not found in DB %d", scName, dbID)
		}
		return schemaID, nil
	}

	alreadyRequestedTypes := make(map[descpb.ID]struct{})
	maybeAddTypeDesc := func(id descpb.ID) {
		if _, ok := alreadyRequestedTypes[id]; !ok {
			// Cross database type references have been disabled, so we don't
			// need to request the parent database because it has already been
			// requested by the table that holds this type.
			alreadyRequestedTypes[id] = struct{}{}
			ret.descs = append(ret.descs, resolver.descByID[id])
		}
	}
	getTypeByID := func(id descpb.ID) (catalog.TypeDescriptor, error) {
		desc, ok := resolver.descByID[id]
		if !ok {
			return nil, errors.Newf("type with ID %d not found", id)
		}
		typeDesc, ok := desc.(catalog.TypeDescriptor)
		if !ok {
			return nil, errors.Newf("descriptor %d is not a type, but a %T", id, desc)
		}
		return typeDesc, nil
	}

	// Process all the TABLE requests.
	// Pulling in a table needs to pull in the underlying database too.
	alreadyRequestedTables := make(map[descpb.ID]struct{})
	for _, pattern := range targets.Tables {
		var err error
		pattern, err = pattern.NormalizeTablePattern()
		if err != nil {
			return ret, err
		}

		switch p := pattern.(type) {
		case *tree.TableName:
			// TODO: As part of work for #34240, this should not be a TableName.
			//  Instead, it should be an UnresolvedObjectName.
			un := p.ToUnresolvedObjectName()
			found, prefix, descI, err := tree.ResolveExisting(ctx, un, resolver, tree.ObjectLookupFlags{}, currentDatabase, searchPath)
			if err != nil {
				return ret, err
			}
			p.ObjectNamePrefix = prefix
			doesNotExistErr := errors.Errorf(`table %q does not exist`, tree.ErrString(p))
			if !found {
				return ret, doesNotExistErr
			}
			tableDesc, isTable := descI.(catalog.TableDescriptor)
			// If the type assertion didn't work, then we resolved a type instead, so
			// error out.
			if !isTable {
				return ret, doesNotExistErr
			}

			// Verify that the table is in the correct state.
			if err := catalog.FilterDescriptorState(
				tableDesc, tree.CommonLookupFlags{},
			); err != nil {
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
			if _, ok := alreadyRequestedTables[tableDesc.GetID()]; !ok {
				alreadyRequestedTables[tableDesc.GetID()] = struct{}{}
				ret.descs = append(ret.descs, tableDesc)
			}
			// Since the table was directly requested, so is the schema. If the table
			// is PUBLIC, we expect the schema to also be PUBLIC.
			if err := maybeAddSchemaDesc(tableDesc.GetParentSchemaID(), true /* requirePublic */); err != nil {
				return ret, err
			}
			// Get all the types used by this table.
			typeIDs, err := tableDesc.GetAllReferencedTypeIDs(getTypeByID)
			if err != nil {
				return ret, err
			}
			for _, id := range typeIDs {
				maybeAddTypeDesc(id)
			}

		case *tree.AllTablesSelector:
			found, descI, err := p.ObjectNamePrefix.Resolve(ctx, resolver, currentDatabase, searchPath)
			if err != nil {
				return ret, err
			}
			if !found {
				return ret, sqlerrors.NewInvalidWildcardError(tree.ErrString(p))
			}
			desc := descI.(catalog.DatabaseDescriptor)

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
		for schemaName, schemas := range resolver.objsByName[dbID] {
			schemaID, err := getSchemaIDByName(schemaName, dbID)
			if err != nil {
				return ret, err
			}
			if err := maybeAddSchemaDesc(schemaID, false /* requirePublic */); err != nil {
				return ret, err
			}

			for _, id := range schemas {
				desc := resolver.descByID[id]
				switch desc := desc.(type) {
				case catalog.TableDescriptor:
					if err := catalog.FilterDescriptorState(
						desc, tree.CommonLookupFlags{},
					); err != nil {
						// Don't include this table in the expansion since it's not in a valid
						// state. Silently fail since this table was not directly requested,
						// but was just part of an expansion.
						continue
					}
					if _, ok := alreadyRequestedTables[id]; !ok {
						ret.descs = append(ret.descs, desc)
					}
					// If this table is a member of a user defined schema, then request the
					// user defined schema.
					if desc.GetParentSchemaID() != keys.PublicSchemaID {
						// Note, that although we're processing the database expansions,
						// since the table is in a PUBLIC state, we also expect the schema
						// to be in a similar state.
						if err := maybeAddSchemaDesc(desc.GetParentSchemaID(), true /* requirePublic */); err != nil {
							return ret, err
						}
					}
					// Get all the types used by this table.
					typeIDs, err := desc.GetAllReferencedTypeIDs(getTypeByID)
					if err != nil {
						return ret, err
					}
					for _, id := range typeIDs {
						maybeAddTypeDesc(id)
					}
				case catalog.TypeDescriptor:
					maybeAddTypeDesc(desc.GetID())
				}
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
		starting, err := loadAllDescs(ctx, codec, db, startTime)
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
			desc := catalogkv.UnwrapDescriptorRaw(ctx, change.Desc)
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

				// We update the modification time for the descriptors here with the
				// timestamp of the KV row so that we can identify the appropriate
				// descriptors to use during restore.
				// Note that the modification time of descriptors on disk is usually 0.
				// See the comment on MaybeSetDescriptorModificationTime... for more.
				descpb.MaybeSetDescriptorModificationTimeFromMVCCTimestamp(ctx, &desc, rev.Timestamp)

				// Collect the prior IDs of table descriptors, as the ID may have been
				// changed during truncate.
				r.Desc = &desc
				t := descpb.TableFromDescriptor(&desc, rev.Timestamp)
				if t != nil && t.ReplacementOf.ID != descpb.InvalidID {
					priorIDs[t.ID] = t.ReplacementOf.ID
				}
			}
			res = append(res, r)
		}
	}
	return res, nil
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

func loadAllDescs(
	ctx context.Context, codec keys.SQLCodec, db *kv.DB, asOf hlc.Timestamp,
) ([]catalog.Descriptor, error) {
	var allDescs []catalog.Descriptor
	if err := db.Txn(
		ctx,
		func(ctx context.Context, txn *kv.Txn) (err error) {
			txn.SetFixedTimestamp(ctx, asOf)
			allDescs, err = catalogkv.GetAllDescriptors(ctx, txn, codec)
			return err
		}); err != nil {
		return nil, err
	}
	return allDescs, nil
}

// ResolveTargetsToDescriptors performs name resolution on a set of targets and
// returns the resulting descriptors.
func ResolveTargetsToDescriptors(
	ctx context.Context, p sql.PlanHookState, endTime hlc.Timestamp, targets *tree.TargetList,
) ([]catalog.Descriptor, []descpb.ID, error) {
	allDescs, err := loadAllDescs(ctx, p.ExecCfg().Codec, p.ExecCfg().DB, endTime)
	if err != nil {
		return nil, nil, err
	}

	if targets == nil {
		return fullClusterTargetsBackup(allDescs)
	}

	var matched descriptorsMatched
	if matched, err = descriptorsMatchingTargets(ctx,
		p.CurrentDatabase(), p.CurrentSearchPath(), allDescs, *targets); err != nil {
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
	allDescs []catalog.Descriptor,
) ([]catalog.Descriptor, []descpb.ID, error) {
	fullClusterDescs, fullClusterDBs, err := fullClusterTargets(allDescs)
	if err != nil {
		return nil, nil, err
	}

	fullClusterDBIDs := make([]descpb.ID, 0)
	for _, desc := range fullClusterDBs {
		fullClusterDBIDs = append(fullClusterDBIDs, desc.GetID())
	}
	return fullClusterDescs, fullClusterDBIDs, nil
}

// fullClusterTargets returns all of the tableDescriptors to be included in a
// full cluster backup, and all the user databases.
func fullClusterTargets(
	allDescs []catalog.Descriptor,
) ([]catalog.Descriptor, []*dbdesc.Immutable, error) {
	fullClusterDescs := make([]catalog.Descriptor, 0, len(allDescs))
	fullClusterDBs := make([]*dbdesc.Immutable, 0)

	systemTablesToBackup := getSystemTablesToIncludeInClusterBackup()

	for _, desc := range allDescs {
		switch desc := desc.(type) {
		case catalog.DatabaseDescriptor:
			dbDesc := dbdesc.NewImmutable(*desc.DatabaseDesc())
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
				if desc.GetState() != descpb.DescriptorState_DROP {
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

// CheckObjectExists returns an error if an object already exists with a given
// parent, parent schema and name.
func CheckObjectExists(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
) error {
	found, id, err := catalogkv.LookupObjectID(ctx, txn, codec, parentID, parentSchemaID, name)
	if err != nil {
		return err
	}
	if found {
		// Find what object we collided with.
		desc, err := catalogkv.GetAnyDescriptorByID(ctx, txn, codec, id, catalogkv.Immutable)
		if err != nil {
			return sqlerrors.WrapErrorWhileConstructingObjectAlreadyExistsErr(err)
		}
		return sqlerrors.MakeObjectAlreadyExistsError(desc.DescriptorProto(), name)
	}
	return nil
}

func fullClusterTargetsRestore(
	allDescs []catalog.Descriptor, lastBackupManifest BackupManifest,
) ([]catalog.Descriptor, []catalog.DatabaseDescriptor, []descpb.TenantInfo, error) {
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

	matched, err := descriptorsMatchingTargets(ctx,
		p.CurrentDatabase(), p.CurrentSearchPath(), allDescs, targets)
	if err != nil {
		return nil, nil, nil, err
	}

	if len(matched.descs) == 0 {
		return nil, nil, nil, errors.Errorf("no tables or databases matched the given targets: %s", tree.ErrString(&targets))
	}

	if lastBackupManifest.FormatVersion >= BackupFormatDescriptorTrackingVersion {
		if err := matched.checkExpansions(lastBackupManifest.CompleteDbs); err != nil {
			return nil, nil, nil, err
		}
	}

	return matched.descs, matched.requestedDBs, nil, nil
}
