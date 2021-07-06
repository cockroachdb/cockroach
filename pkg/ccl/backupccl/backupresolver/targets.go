// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupresolver

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// DescriptorsMatched is a struct containing the set of
// descriptors matching a target descriptor (or set of targets).
type DescriptorsMatched struct {
	// All descriptors that match targets plus their parent databases.
	Descs []catalog.Descriptor

	// The databases from which all tables were matched (eg a.* or DATABASE a).
	ExpandedDB []descpb.ID

	// Explicitly requested DBs (e.g. DATABASE a).
	RequestedDBs []catalog.DatabaseDescriptor
}

// CheckExpansions determines if matched targets are covered by the specified
// descriptors.
func (d DescriptorsMatched) CheckExpansions(coveredDBs []descpb.ID) error {
	covered := make(map[descpb.ID]bool)
	for _, i := range coveredDBs {
		covered[i] = true
	}
	for _, i := range d.RequestedDBs {
		if !covered[i.GetID()] {
			return errors.Errorf("cannot RESTORE DATABASE from a backup of individual tables (use SHOW BACKUP to determine available tables)")
		}
	}
	for _, i := range d.ExpandedDB {
		if !covered[i] {
			return errors.Errorf("cannot RESTORE <database>.* from a backup of individual tables (use SHOW BACKUP to determine available tables)")
		}
	}
	return nil
}

// DescriptorResolver is the helper struct that enables reuse of the
// standard name resolution algorithm.
type DescriptorResolver struct {
	// Map: descriptorID -> descriptor
	DescByID map[descpb.ID]catalog.Descriptor
	// Map: db name -> dbID
	DbsByName map[string]descpb.ID
	// Map: dbID -> schema name -> schemaID
	SchemasByName map[descpb.ID]map[string]descpb.ID
	// Map: dbID -> schema name -> obj name -> obj ID
	ObjsByName map[descpb.ID]map[string]map[string]descpb.ID
}

// LookupSchema implements the resolver.ObjectNameTargetResolver interface.
func (r *DescriptorResolver) LookupSchema(
	ctx context.Context, dbName, scName string,
) (bool, catalog.ResolvedObjectPrefix, error) {
	dbID, ok := r.DbsByName[dbName]
	if !ok {
		return false, catalog.ResolvedObjectPrefix{}, nil
	}
	schemas := r.SchemasByName[dbID]
	if scID, ok := schemas[scName]; ok {
		dbDesc, dbOk := r.DescByID[dbID].(catalog.DatabaseDescriptor)
		scDesc, scOk := r.DescByID[scID].(catalog.SchemaDescriptor)
		if !scOk && scID == keys.PublicSchemaID {
			scDesc, scOk = schemadesc.GetPublicSchema(), true
		}
		if dbOk && scOk {
			return true, catalog.ResolvedObjectPrefix{
				Database: dbDesc,
				Schema:   scDesc,
			}, nil
		}
	}
	return false, catalog.ResolvedObjectPrefix{}, nil
}

// LookupObject implements the tree.ObjectNameExistingResolver interface.
func (r *DescriptorResolver) LookupObject(
	ctx context.Context, flags tree.ObjectLookupFlags, dbName, scName, obName string,
) (bool, catalog.ResolvedObjectPrefix, catalog.Descriptor, error) {
	if flags.RequireMutable {
		panic("did not expect request for mutable descriptor")
	}
	dbID, ok := r.DbsByName[dbName]
	if !ok {
		return false, catalog.ResolvedObjectPrefix{}, nil, nil
	}
	scID, ok := r.SchemasByName[dbID][scName]
	if !ok {
		return false, catalog.ResolvedObjectPrefix{}, nil, nil
	}
	if scMap, ok := r.ObjsByName[dbID]; ok {
		if objMap, ok := scMap[scName]; ok {
			if objID, ok := objMap[obName]; ok {
				var sc catalog.SchemaDescriptor
				if scID == keys.PublicSchemaID {
					sc = schemadesc.GetPublicSchema()
				} else {
					sc, ok = r.DescByID[scID].(catalog.SchemaDescriptor)
					if !ok {
						return false, catalog.ResolvedObjectPrefix{}, nil, errors.AssertionFailedf(
							"expected schema for ID %d, got %T", scID, r.DescByID[scID])
					}
				}

				return true, catalog.ResolvedObjectPrefix{
					Database: r.DescByID[dbID].(catalog.DatabaseDescriptor),
					Schema:   sc,
				}, r.DescByID[objID], nil
			}
		}
	}
	return false, catalog.ResolvedObjectPrefix{}, nil, nil
}

// NewDescriptorResolver prepares a DescriptorResolver for the given
// known set of descriptors.
func NewDescriptorResolver(descs []catalog.Descriptor) (*DescriptorResolver, error) {
	r := &DescriptorResolver{
		DescByID:      make(map[descpb.ID]catalog.Descriptor),
		SchemasByName: make(map[descpb.ID]map[string]descpb.ID),
		DbsByName:     make(map[string]descpb.ID),
		ObjsByName:    make(map[descpb.ID]map[string]map[string]descpb.ID),
	}

	// Iterate to find the databases first. We need that because we also
	// check the ParentID for tables, and all the valid parents must be
	// known before we start to check that.
	for _, desc := range descs {
		if _, isDB := desc.(catalog.DatabaseDescriptor); isDB {
			if _, ok := r.DbsByName[desc.GetName()]; ok {
				return nil, errors.Errorf("duplicate database name: %q used for ID %d and %d",
					desc.GetName(), r.DbsByName[desc.GetName()], desc.GetID())
			}
			r.DbsByName[desc.GetName()] = desc.GetID()
			r.ObjsByName[desc.GetID()] = make(map[string]map[string]descpb.ID)
			r.SchemasByName[desc.GetID()] = make(map[string]descpb.ID)
			// Always add an entry for the public schema.
			r.ObjsByName[desc.GetID()][tree.PublicSchema] = make(map[string]descpb.ID)
			r.SchemasByName[desc.GetID()][tree.PublicSchema] = keys.PublicSchemaID
		}

		// Incidentally, also remember all the descriptors by ID.
		if prevDesc, ok := r.DescByID[desc.GetID()]; ok {
			return nil, errors.Errorf("duplicate descriptor ID: %d used by %q and %q",
				desc.GetID(), prevDesc.GetName(), desc.GetName())
		}
		r.DescByID[desc.GetID()] = desc
	}

	// Add all schemas to the resolver.
	for _, desc := range descs {
		if sc, ok := desc.(catalog.SchemaDescriptor); ok {
			schemaMap := r.ObjsByName[sc.GetParentID()]
			if schemaMap == nil {
				schemaMap = make(map[string]map[string]descpb.ID)
			}
			schemaMap[sc.GetName()] = make(map[string]descpb.ID)
			r.ObjsByName[sc.GetParentID()] = schemaMap

			schemaNameMap := r.SchemasByName[sc.GetParentID()]
			if schemaNameMap == nil {
				schemaNameMap = make(map[string]descpb.ID)
			}
			schemaNameMap[sc.GetName()] = sc.GetID()
			r.SchemasByName[sc.GetParentID()] = schemaNameMap
		}
	}

	// registerDesc is a closure that registers a Descriptor into the resolver's
	// object registry.
	registerDesc := func(parentID descpb.ID, desc catalog.Descriptor, kind string) error {
		parentDesc, ok := r.DescByID[parentID]
		if !ok {
			return errors.Errorf("%s %q has unknown ParentID %d", kind, desc.GetName(), parentID)
		}
		if _, ok := r.DbsByName[parentDesc.GetName()]; !ok {
			return errors.Errorf("%s %q's ParentID %d (%q) is not a database",
				kind, desc.GetName(), parentID, parentDesc.GetName())
		}

		// Look up what schema this descriptor belongs under.
		schemaMap := r.ObjsByName[parentDesc.GetID()]
		scID := desc.GetParentSchemaID()
		var scName string
		if scID == keys.PublicSchemaID {
			scName = tree.PublicSchema
		} else {
			scDescI, ok := r.DescByID[scID]
			if !ok {
				return errors.Errorf("schema %d not found for desc %d", scID, desc.GetID())
			}
			scDesc, err := catalog.AsSchemaDescriptor(scDescI)
			if err != nil {
				return err
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
		r.ObjsByName[parentDesc.GetID()][scName] = objMap
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
			if desc.IsTemporary() {
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

// DescriptorsMatchingTargets returns the descriptors that match the targets. A
// database descriptor is included in this set if it matches the targets (or the
// session database) or if one of its tables matches the targets. All expanded
// DBs, via either `foo.*` or `DATABASE foo` are noted, as are those explicitly
// named as DBs (e.g. with `DATABASE foo`, not `foo.*`). These distinctions are
// used e.g. by RESTORE.
//
// This is guaranteed to not return duplicates.
func DescriptorsMatchingTargets(
	ctx context.Context,
	currentDatabase string,
	searchPath sessiondata.SearchPath,
	descriptors []catalog.Descriptor,
	targets tree.TargetList,
	asOf hlc.Timestamp,
) (DescriptorsMatched, error) {
	ret := DescriptorsMatched{}

	r, err := NewDescriptorResolver(descriptors)
	if err != nil {
		return ret, err
	}

	alreadyRequestedDBs := make(map[descpb.ID]struct{})
	alreadyExpandedDBs := make(map[descpb.ID]struct{})
	invalidRestoreTsErr := errors.Errorf("supplied backups do not cover requested time")
	// Process all the DATABASE requests.
	for _, d := range targets.Databases {
		dbID, ok := r.DbsByName[string(d)]
		if !ok {
			if asOf.IsEmpty() {
				return ret, errors.Errorf("database %q does not exist", d)
			}
			return ret, errors.Wrapf(invalidRestoreTsErr, "database %q does not exist, or invalid RESTORE timestamp", d)
		}
		if _, ok := alreadyRequestedDBs[dbID]; !ok {
			desc := r.DescByID[dbID]
			ret.Descs = append(ret.Descs, desc)
			ret.RequestedDBs = append(ret.RequestedDBs,
				desc.(catalog.DatabaseDescriptor))
			ret.ExpandedDB = append(ret.ExpandedDB, dbID)
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
			schemaDesc := r.DescByID[id]
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
			ret.Descs = append(ret.Descs, r.DescByID[id])
		}

		return nil
	}
	getSchemaIDByName := func(scName string, dbID descpb.ID) (descpb.ID, error) {
		schemas, ok := r.SchemasByName[dbID]
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
			ret.Descs = append(ret.Descs, r.DescByID[id])
		}
	}
	getTypeByID := func(id descpb.ID) (catalog.TypeDescriptor, error) {
		desc, ok := r.DescByID[id]
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
			un := p.ToUnresolvedObjectName()
			found, prefix, descI, err := resolver.ResolveExisting(ctx, un, r, tree.ObjectLookupFlags{}, currentDatabase, searchPath)
			if err != nil {
				return ret, err
			}
			// If the prefix is more informative than the input, take it.
			if (prefix.ExplicitDatabase && !p.ExplicitCatalog) ||
				(prefix.ExplicitSchema && !p.ExplicitSchema) {
				p.ObjectNamePrefix = prefix.NamePrefix()
			}
			doesNotExistErr := errors.Errorf(`table %q does not exist`, tree.ErrString(p))
			if !found {
				if asOf.IsEmpty() {
					return ret, doesNotExistErr
				}
				return ret, errors.Wrapf(invalidRestoreTsErr, `table %q does not exist, or invalid RESTORE timestamp`, tree.ErrString(p))
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
				parentDesc := r.DescByID[parentID]
				ret.Descs = append(ret.Descs, parentDesc)
				alreadyRequestedDBs[parentID] = struct{}{}
			}
			// Then request the table itself.
			if _, ok := alreadyRequestedTables[tableDesc.GetID()]; !ok {
				alreadyRequestedTables[tableDesc.GetID()] = struct{}{}
				ret.Descs = append(ret.Descs, tableDesc)
			}
			// Since the table was directly requested, so is the schema. If the table
			// is PUBLIC, we expect the schema to also be PUBLIC.
			if err := maybeAddSchemaDesc(tableDesc.GetParentSchemaID(), true /* requirePublic */); err != nil {
				return ret, err
			}
			// Get all the types used by this table.
			desc := r.DescByID[tableDesc.GetParentID()]
			dbDesc := desc.(catalog.DatabaseDescriptor)
			typeIDs, err := tableDesc.GetAllReferencedTypeIDs(dbDesc, getTypeByID)
			if err != nil {
				return ret, err
			}
			for _, id := range typeIDs {
				maybeAddTypeDesc(id)
			}

		case *tree.AllTablesSelector:
			found, prefix, err := resolver.ResolveObjectNamePrefix(ctx, r, currentDatabase, searchPath, &p.ObjectNamePrefix)
			if err != nil {
				return ret, err
			}
			if !found {
				return ret, sqlerrors.NewInvalidWildcardError(tree.ErrString(p))
			}

			// If the database is not requested already, request it now.
			dbID := prefix.Database.GetID()
			if _, ok := alreadyRequestedDBs[dbID]; !ok {
				ret.Descs = append(ret.Descs, prefix.Database)
				alreadyRequestedDBs[dbID] = struct{}{}
			}

			// Then request the expansion.
			if _, ok := alreadyExpandedDBs[prefix.Database.GetID()]; !ok {
				ret.ExpandedDB = append(ret.ExpandedDB, prefix.Database.GetID())
				alreadyExpandedDBs[prefix.Database.GetID()] = struct{}{}
			}

		default:
			return ret, errors.Errorf("unknown pattern %T: %+v", pattern, pattern)
		}
	}

	// Then process the database expansions.
	for dbID := range alreadyExpandedDBs {
		for schemaName, schemas := range r.ObjsByName[dbID] {
			schemaID, err := getSchemaIDByName(schemaName, dbID)
			if err != nil {
				return ret, err
			}
			if err := maybeAddSchemaDesc(schemaID, false /* requirePublic */); err != nil {
				return ret, err
			}

			for _, id := range schemas {
				desc := r.DescByID[id]
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
						ret.Descs = append(ret.Descs, desc)
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
					dbRaw := r.DescByID[desc.GetParentID()]
					dbDesc := dbRaw.(catalog.DatabaseDescriptor)
					typeIDs, err := desc.GetAllReferencedTypeIDs(dbDesc, getTypeByID)
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

// LoadAllDescs returns all of the descriptors in the cluster.
func LoadAllDescs(
	ctx context.Context, codec keys.SQLCodec, db *kv.DB, asOf hlc.Timestamp,
) ([]catalog.Descriptor, error) {
	var allDescs []catalog.Descriptor
	if err := db.Txn(
		ctx,
		func(ctx context.Context, txn *kv.Txn) (err error) {
			txn.SetFixedTimestamp(ctx, asOf)
			allDescs, err = catalogkv.GetAllDescriptors(
				ctx, txn, codec, true, /* shouldRunPostDeserializationChanges */
			)
			return err
		}); err != nil {
		return nil, err
	}
	return allDescs, nil
}

// ResolveTargetsToDescriptors performs name resolution on a set of targets and
// returns the resulting descriptors.
//
// TODO(ajwerner): adopt the collection here.
func ResolveTargetsToDescriptors(
	ctx context.Context, p sql.PlanHookState, endTime hlc.Timestamp, targets *tree.TargetList,
) ([]catalog.Descriptor, []descpb.ID, error) {
	allDescs, err := LoadAllDescs(ctx, p.ExecCfg().Codec, p.ExecCfg().DB, endTime)
	if err != nil {
		return nil, nil, err
	}

	var matched DescriptorsMatched
	if matched, err = DescriptorsMatchingTargets(ctx,
		p.CurrentDatabase(), p.CurrentSearchPath(), allDescs, *targets, endTime); err != nil {
		return nil, nil, err
	}

	// Ensure interleaved tables appear after their parent. Since parents must be
	// created before their children, simply sorting by ID accomplishes this.
	sort.Slice(matched.Descs, func(i, j int) bool { return matched.Descs[i].GetID() < matched.Descs[j].GetID() })
	return matched.Descs, matched.ExpandedDB, nil
}
