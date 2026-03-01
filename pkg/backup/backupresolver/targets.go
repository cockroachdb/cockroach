// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupresolver

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
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

	// A map of explicitly requested TablePatterns to their resolutions.
	DescsByTablePattern map[tree.TablePattern]catalog.Descriptor
}

// MissingTableErr is a custom error type for Missing Table when resolver.ResolveExisting()
// is called in DescriptorsMatchingTargets
type MissingTableErr struct {
	wrapped   error
	tableName string
}

// Error() implements the erorr interface for MissingTableErr and outputs an error string
func (e *MissingTableErr) Error() string {
	return fmt.Sprintf("table %q does not exist, or invalid RESTORE timestamp: %v", e.GetTableName(), e.wrapped.Error())
}

// Unwrap() implements the erorr interface for MissingTableErr and outputs wrapped error
// implemented so that errors.As can be used with MissingTableErr
func (e *MissingTableErr) Unwrap() error {
	return e.wrapped
}

// GetTableName is an accessor function for the member variable tableName
func (e *MissingTableErr) GetTableName() string {
	return e.tableName
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
	// Note: this map does not contain any user-defined functions because function
	// descriptors don't have namespace entries.
	ObjsByName map[descpb.ID]map[string]map[string]descpb.ID
	// Map: dbID -> schema name -> []obj ID
	ObjIDsBySchema map[descpb.ID]map[string]*catalog.DescriptorIDSet
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
		// TODO(richardjcai): We should remove the check for keys.PublicSchemaID
		// in 22.2, when we're guaranteed to not have synthesized public schemas
		// for the non-system databases.
		if !scOk && scID == keys.SystemPublicSchemaID ||
			!scOk && scID == keys.PublicSchemaIDForBackup {
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
	// LookupObject guarantees that the ResolvedObjectPrefix is always
	// populated, even if the object itself cannot be found. This information
	// is used to generate the appropriate error at higher level layers.
	resolvedPrefix := catalog.ResolvedObjectPrefix{}
	if flags.RequireMutable {
		panic("did not expect request for mutable descriptor")
	}
	dbID, ok := r.DbsByName[dbName]
	if !ok {
		return false, resolvedPrefix, nil, nil
	}
	resolvedPrefix.Database, ok = r.DescByID[dbID].(catalog.DatabaseDescriptor)
	if !ok {
		return false, resolvedPrefix, nil, nil
	}
	scID, ok := r.SchemasByName[dbID][scName]
	if !ok {
		return false, resolvedPrefix, nil, nil
	}
	if scMap, ok := r.ObjsByName[dbID]; ok {
		if objMap, ok := scMap[scName]; ok {
			if objID, ok := objMap[obName]; ok {
				if scID == keys.PublicSchemaID {
					resolvedPrefix.Schema = schemadesc.GetPublicSchema()
				} else {
					resolvedPrefix.Schema, ok = r.DescByID[scID].(catalog.SchemaDescriptor)
					if !ok {
						return false, resolvedPrefix, nil, errors.AssertionFailedf(
							"expected schema for ID %d, got %T", scID, r.DescByID[scID])
					}
				}
				return true, resolvedPrefix, r.DescByID[objID], nil
			}
		}
	}
	return false, catalog.ResolvedObjectPrefix{}, nil, nil
}

// NewDescriptorResolver prepares a DescriptorResolver for the given
// known set of descriptors.
func NewDescriptorResolver(descs []catalog.Descriptor) (*DescriptorResolver, error) {
	r := &DescriptorResolver{
		DescByID:       make(map[descpb.ID]catalog.Descriptor),
		SchemasByName:  make(map[descpb.ID]map[string]descpb.ID),
		DbsByName:      make(map[string]descpb.ID),
		ObjsByName:     make(map[descpb.ID]map[string]map[string]descpb.ID),
		ObjIDsBySchema: make(map[descpb.ID]map[string]*catalog.DescriptorIDSet),
	}

	// Iterate to find the databases first. We need that because we also
	// check the ParentID for tables, and all the valid parents must be
	// known before we start to check that.
	for _, desc := range descs {
		if desc.Dropped() {
			continue
		}
		if _, isDB := desc.(catalog.DatabaseDescriptor); isDB {
			if _, ok := r.DbsByName[desc.GetName()]; ok {
				return nil, errors.Errorf("duplicate database name: %q used for ID %d and %d",
					desc.GetName(), r.DbsByName[desc.GetName()], desc.GetID())
			}
			r.DbsByName[desc.GetName()] = desc.GetID()
			r.ObjsByName[desc.GetID()] = make(map[string]map[string]descpb.ID)
			r.SchemasByName[desc.GetID()] = make(map[string]descpb.ID)
			r.ObjIDsBySchema[desc.GetID()] = make(map[string]*catalog.DescriptorIDSet)

			if !desc.(catalog.DatabaseDescriptor).HasPublicSchemaWithDescriptor() {
				r.ObjsByName[desc.GetID()][catconstants.PublicSchemaName] = make(map[string]descpb.ID)
				r.SchemasByName[desc.GetID()][catconstants.PublicSchemaName] = keys.PublicSchemaIDForBackup
				r.ObjIDsBySchema[desc.GetID()][catconstants.PublicSchemaName] = &catalog.DescriptorIDSet{}
			}
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
		if desc.Dropped() {
			continue
		}
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

			objIDsMap := r.ObjIDsBySchema[sc.GetParentID()]
			if objIDsMap == nil {
				objIDsMap = make(map[string]*catalog.DescriptorIDSet)
			}
			objIDsMap[sc.GetName()] = &catalog.DescriptorIDSet{}
			r.ObjIDsBySchema[sc.GetParentID()] = objIDsMap
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
		// TODO(richardjcai): We can remove this in 22.2, still have to handle
		// this case in the mixed version cluster.
		if scID == keys.PublicSchemaIDForBackup {
			scName = catconstants.PublicSchemaName
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

		// Create an entry for the descriptor in `r.ObjsByName` (if it's not a
		// function) and `r.ObjIDsBySchema`.
		// Note: `r.DescByID` has been previously populated already.
		if kind != "function" {
			objMap := schemaMap[scName]
			if objMap == nil {
				objMap = make(map[string]descpb.ID)
			}
			descName := desc.GetName()
			if _, ok := objMap[descName]; ok {
				return errors.Errorf("duplicate %s name: %q.%q.%q used for ID %d and %d",
					kind, parentDesc.GetName(), scName, descName, desc.GetID(), objMap[descName])
			}
			objMap[descName] = desc.GetID()
			r.ObjsByName[parentDesc.GetID()][scName] = objMap
		}

		objIDsMap := r.ObjIDsBySchema[parentDesc.GetID()]
		objIDs := objIDsMap[scName]
		if objIDs == nil {
			objIDs = &catalog.DescriptorIDSet{}
		}
		objIDs.Add(desc.GetID())
		r.ObjIDsBySchema[parentDesc.GetID()][scName] = objIDs
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
		case catalog.FunctionDescriptor:
			typeToRegister = "function"
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
// This is guaranteed to not return duplicates, other than in DescsByTablePattern,
// which will contain a descriptor for every element of targets.Tables.
func DescriptorsMatchingTargets(
	ctx context.Context,
	currentDatabase string,
	searchPath sessiondata.SearchPath,
	descriptors []catalog.Descriptor,
	targets tree.BackupTargetList,
	asOf hlc.Timestamp,
) (DescriptorsMatched, error) {
	ret := DescriptorsMatched{
		DescsByTablePattern: make(map[tree.TablePattern]catalog.Descriptor, len(targets.Tables.TablePatterns)),
	}

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
			// Verify that the database is in the correct state.
			if desc == nil || !desc.Public() {
				// Return a does not exist error if explicitly asking for this database.
				return ret, errors.Errorf(`database %q does not exist`, d)
			}
			ret.Descs = append(ret.Descs, desc)
			ret.RequestedDBs = append(ret.RequestedDBs, desc.(catalog.DatabaseDescriptor))
			// If backup a whole DB, we need to expand the DB like what we do for "db.*"
			ret.ExpandedDB = append(ret.ExpandedDB, dbID)
			alreadyRequestedDBs[dbID] = struct{}{}
			alreadyExpandedDBs[dbID] = struct{}{}
		}
	}

	alreadyRequestedSchemas := make(map[descpb.ID]struct{})
	maybeAddSchemaDesc := func(id descpb.ID, requirePublic bool) error {
		// Only add user defined schemas.
		if id == keys.PublicSchemaIDForBackup {
			return nil
		}
		if _, ok := alreadyRequestedSchemas[id]; !ok {
			schemaDesc := r.DescByID[id]
			if schemaDesc == nil {
				if requirePublic {
					return errors.Wrapf(err, "cannot find schema %d", id)
				}
				return nil
			}
			// Ignore schemas in `DROP` state. This means we will include `PUBLIC`,
			// `OFFLINE`, and `ADD` schemas into the backup.
			if schemaDesc.Dropped() {
				if requirePublic {
					return errors.Wrapf(err, "schema %d was expected to be PUBLIC; get DROP", id)
				}
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
	// Process specific SCHEMAs requested for a database.
	alreadyRequestedSchemasByDBs := make(map[descpb.ID]map[string]struct{})
	for _, pattern := range targets.Tables.TablePatterns {
		var err error
		origPat := pattern
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
				return ret, &MissingTableErr{invalidRestoreTsErr, tree.ErrString(p)}
			}
			tableDesc, isTable := descI.(catalog.TableDescriptor)
			// If the type assertion didn't work, then we resolved a type instead, so
			// error out. Otherwise verify that the table is in the correct state.
			if !isTable || tableDesc == nil || !tableDesc.Public() {
				return ret, doesNotExistErr
			}

			ret.DescsByTablePattern[origPat] = descI

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
			typeIDs, _, err := tableDesc.GetAllReferencedTypeIDs(dbDesc, getTypeByID)
			if err != nil {
				return ret, err
			}
			for _, id := range typeIDs {
				maybeAddTypeDesc(id)
			}
			// TODO(chengxiong): get all the user-defined functions used by this
			// table. This is needed when we start supporting udf references from
			// other objects.
		case *tree.AllTablesSelector:
			// We should only back up targets in the scoped schema if the table
			// pattern is fully qualified, i.e., `db.schema.*`, both the schema
			// field and catalog field were set.
			hasSchemaScope := p.ExplicitSchema && p.ExplicitCatalog
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

			// If the target was fully qualified, i.e. `db.schema.*` then
			// `hasSchemaScope` would be set to true above.
			//
			// After resolution if the target does not have ExplicitCatalog
			// set to true, it means that the target was of the form `schema.*`.
			// In this case, we want to only backup the object in the schema scope.
			//
			// If neither of the above cases apply, the target is of the form `db.*`.
			// In this case we want to backup all objects in db and so `hasSchemaScope`
			// should be set to false.
			if !hasSchemaScope && !p.ExplicitCatalog {
				hasSchemaScope = true
			}

			// If we are given a specified schema scope, i.e., `db.schema.*`
			// or `schema.*`, add the schema to `alreadyRequestedSchemasByDBs`
			if hasSchemaScope {
				if _, ok := alreadyRequestedSchemasByDBs[dbID]; !ok {
					scMap := make(map[string]struct{})
					alreadyRequestedSchemasByDBs[dbID] = scMap
				}
				scMap := alreadyRequestedSchemasByDBs[dbID]
				scMap[p.Schema()] = struct{}{}
				ret.DescsByTablePattern[origPat] = prefix.Schema
			} else {
				ret.DescsByTablePattern[origPat] = prefix.Database
			}
		default:
			return ret, errors.Errorf("unknown pattern %T: %+v", pattern, pattern)
		}
	}

	addObjectDescsInSchema := func(objectsIDs *catalog.DescriptorIDSet) error {
		for _, id := range objectsIDs.Ordered() {
			desc := r.DescByID[id]
			if desc == nil || (!desc.Public() && !desc.Offline()) {
				// Don't include this object in the expansion since it's not in a valid
				// state. Silently fail since this object was not directly requested,
				// but was just part of an expansion.
				continue
			}
			// If this object is a member of a user defined schema, then request the
			// user defined schema.
			if desc.GetParentSchemaID() != keys.PublicSchemaIDForBackup {
				// Note, that although we're processing the database expansions,
				// since the table is in a PUBLIC state, we also expect the schema
				// to be in a similar state.
				if err := maybeAddSchemaDesc(desc.GetParentSchemaID(), true /* requirePublic */); err != nil {
					return err
				}
			}
			switch desc := desc.(type) {
			case catalog.TableDescriptor:
				if _, ok := alreadyRequestedTables[id]; !ok {
					ret.Descs = append(ret.Descs, desc)
				}
				// Get all the types used by this table.
				dbRaw := r.DescByID[desc.GetParentID()]
				dbDesc := dbRaw.(catalog.DatabaseDescriptor)
				typeIDs, _, err := desc.GetAllReferencedTypeIDs(dbDesc, getTypeByID)
				if err != nil {
					return err
				}
				for _, id := range typeIDs {
					maybeAddTypeDesc(id)
				}
				// TODO(chengxiong): get all the user-defined functions used by this
				// table. This is needed when we start supporting udf references from
				// other objects.
			case catalog.TypeDescriptor:
				maybeAddTypeDesc(desc.GetID())
			case catalog.FunctionDescriptor:
				// It's safe to append the Function descriptor directly since functions
				// are only added when adding all descriptors in a schema.
				ret.Descs = append(ret.Descs, desc)
				for _, id := range desc.GetDependsOnTypes() {
					maybeAddTypeDesc(id)
				}
			}
		}
		return nil
	}

	// Then process the database expansions.
	for dbID := range alreadyExpandedDBs {
		if requestedSchemas, ok := alreadyRequestedSchemasByDBs[dbID]; !ok {
			// If it's an expanded DB but no specific schema requested, then it's a
			// "db.*" expansion. We need to loop through all schemas of the DB.
			for schemaName, objIDs := range r.ObjIDsBySchema[dbID] {
				schemaID, err := getSchemaIDByName(schemaName, dbID)
				if err != nil {
					return ret, err
				}
				if err := maybeAddSchemaDesc(schemaID, false /* requirePublic */); err != nil {
					return ret, err
				}
				if err := addObjectDescsInSchema(objIDs); err != nil {
					return ret, err
				}
			}
		} else {
			for schemaName := range requestedSchemas {
				objIDs := r.ObjIDsBySchema[dbID][schemaName]
				if err := addObjectDescsInSchema(objIDs); err != nil {
					return ret, err
				}
			}
		}
	}

	return ret, nil
}

// LoadAllDescs returns all of the descriptors in the cluster.
func LoadAllDescs(
	ctx context.Context, execCfg *sql.ExecutorConfig, asOf hlc.Timestamp,
) (allDescs []catalog.Descriptor, _ error) {
	if err := sql.DescsTxn(ctx, execCfg, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		err := txn.KV().SetFixedTimestamp(ctx, asOf)
		if err != nil {
			return err
		}
		all, err := col.GetAllDescriptors(ctx, txn.KV())
		allDescs = all.OrderedDescriptors()
		return err
	}); err != nil {
		return nil, err
	}
	return allDescs, nil
}

// wildcardResult holds the resolved database, optional target schema, and
// scope flag for an AllTablesSelector wildcard pattern.
type wildcardResult struct {
	db catalog.DatabaseDescriptor
	// targetSchema is non-nil if the wildcard pattern has an explicit schema
	// component, e.g. `db.schema.*`, `schema.*`, or bare `*`.
	targetSchema catalog.SchemaDescriptor
	// hasSchemaScope is true when the wildcard pattern is a 3-part name
	// like my_db.my_schema.*, i.e. both catalog and schema are explicit.
	//
	// TODO(msbutler): we should remove this, and simply rely on targetSchema.
	// Currently specifying `schema.*` will back up the whole parent database, if
	// the currentDatabase is the actual parent of the schema,
	// which is unexpected. If the currentDatabase is some other db,
	// resolveWildcard will fail.
	hasSchemaScope bool
}

// resolveWildcard determines the database, optional target schema, and schema
// scope for an AllTablesSelector wildcard pattern.
//
// TODO(msbutler): we should remove the hasSchemaScope flag, and simply rely on
// targetSchema. See test cases for how this hasSchemaScope flag leads to
// weirdness.
func resolveWildcard(
	ctx context.Context,
	p *tree.AllTablesSelector,
	r *simpleResolver,
	currentDatabase string,
	searchPath sessiondata.SearchPath,
) (wildcardResult, error) {
	if p.ExplicitSchema && !p.ExplicitCatalog {
		// 2-part pattern like "db.*". The parser puts the first part in
		// SchemaName, not CatalogName. Check if it's actually a database.
		potentialDBName := p.ObjectNamePrefix.Schema()
		maybeDB, err := r.col.ByName(r.txn).MaybeGet().Database(ctx, potentialDBName)
		if err != nil {
			return wildcardResult{}, err
		}
		if maybeDB != nil {
			return wildcardResult{db: maybeDB}, nil
		}
	}

	// Normal resolution: treat SchemaName as a schema name (or resolve
	// bare "*" via the current database and search path).
	found, prefix, err := resolver.ResolveObjectNamePrefix(
		ctx, r, currentDatabase, searchPath, &p.ObjectNamePrefix,
	)
	if err != nil {
		return wildcardResult{}, err
	}
	if !found {
		return wildcardResult{}, sqlerrors.NewInvalidWildcardError(tree.ErrString(p))
	}
	return wildcardResult{
		db:             prefix.Database,
		targetSchema:   prefix.Schema,
		hasSchemaScope: p.ExplicitSchema && p.ExplicitCatalog,
	}, nil
}

// ResolveTargets performs name resolution on a set of targets and returns
// the resulting descriptors using Collection APIs to avoid loading all
// descriptors into memory.
//
// This implementation uses Collection methods to load only the necessary
// descriptors, making it O(targets) instead of O(all_descriptors).
//
// Returns:
//   - []catalog.Descriptor: result - qll resolved descriptors.
//   - []descpb.ID: expandedDBs - database IDs where we used a wildcard expansion.
//   - []catalog.DatabaseDescriptor: requestedDBs - explicitly requested DATABASE targets.
//   - map[tree.TablePattern]catalog.Descriptor: descsByTablePattern - TABLE patterns to descriptors.
func ResolveTargets(
	ctx context.Context, p sql.PlanHookState, endTime hlc.Timestamp, targets *tree.BackupTargetList,
) (
	result []catalog.Descriptor,
	expandedDBs []descpb.ID,
	requestedDBs []catalog.DatabaseDescriptor,
	descsByTablePattern map[tree.TablePattern]catalog.Descriptor,
	retErr error,
) {
	descsByTablePattern = make(map[tree.TablePattern]catalog.Descriptor)
	seenDescs := make(map[descpb.ID]struct{})

	// Helper to add descriptor if not already seen.
	addDesc := func(desc catalog.Descriptor) {
		if desc == nil {
			return
		}
		if _, seen := seenDescs[desc.GetID()]; !seen {
			result = append(result, desc)
			seenDescs[desc.GetID()] = struct{}{}
		}
	}

	err := sql.DescsTxn(ctx, p.ExecCfg(), func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		if err := txn.KV().SetFixedTimestamp(ctx, endTime); err != nil {
			return err
		}

		currentDatabase := p.CurrentDatabase()
		searchPath := p.CurrentSearchPath()

		// Create a resolver for name resolution using Collection APIs.
		r := &simpleResolver{col: col, txn: txn.KV()}

		// Helper to get type descriptors referenced by a table.
		getReferencedTypes := func(table catalog.TableDescriptor, db catalog.DatabaseDescriptor) error {
			getTypeByID := func(id descpb.ID) (catalog.TypeDescriptor, error) {
				typ, err := col.ByIDWithoutLeased(txn.KV()).Get().Type(ctx, id)
				if err != nil {
					return nil, err
				}
				return typ, nil
			}

			typeIDs, _, err := table.GetAllReferencedTypeIDs(db, getTypeByID)
			if err != nil {
				return err
			}
			for _, typeID := range typeIDs {
				typ, err := col.ByIDWithoutLeased(txn.KV()).Get().Type(ctx, typeID)
				if err != nil {
					return err
				}
				addDesc(typ)
			}
			return nil
		}

		// Helper to get function descriptors referenced by a table.
		getReferencedFunctions := func(table catalog.TableDescriptor) error {
			fnIDs, err := table.GetAllReferencedFunctionIDs()
			if err != nil {
				return err
			}
			for _, fnID := range fnIDs.Ordered() {
				fn, err := col.ByIDWithoutLeased(txn.KV()).Get().Function(ctx, fnID)
				if err != nil {
					return err
				}
				addDesc(fn)
			}
			return nil
		}

		// Helper to check if a schema should be skipped (dropped, virtual, or temporary).
		shouldSkipSchema := func(schema catalog.SchemaDescriptor) bool {
			if schema == nil {
				return true
			}
			if schema.Dropped() {
				return true
			}
			kind := schema.SchemaKind()
			return kind == catalog.SchemaVirtual || kind == catalog.SchemaTemporary
		}

		// Helper to add schema descriptor.
		addSchemaDesc := func(schemaID descpb.ID) error {
			// Skip public schema placeholders.
			if schemaID == keys.PublicSchemaIDForBackup || schemaID == keys.PublicSchemaID {
				return nil
			}
			schema, err := col.ByIDWithoutLeased(txn.KV()).Get().Schema(ctx, schemaID)
			if err != nil {
				return err
			}
			if !shouldSkipSchema(schema) {
				addDesc(schema)
			}
			return nil
		}

		// Process DATABASE targets.
		for _, dbName := range targets.Databases {
			db, err := col.ByName(txn.KV()).Get().Database(ctx, string(dbName))
			if err != nil {
				return err
			}
			if !db.Public() {
				return sqlerrors.NewUndefinedDatabaseError(tree.ErrString(&dbName))
			}
			addDesc(db)

			// Track completeDBs and requestedDBs for DATABASE targets.
			expandedDBs = append(expandedDBs, db.GetID())
			requestedDBs = append(requestedDBs, db)

			// For database backup, get all objects in the database.
			allInDB, err := col.GetAllInDatabase(ctx, txn.KV(), db)
			if err != nil {
				return err
			}

			err = allInDB.ForEachDescriptor(func(desc catalog.Descriptor) error {
				// Skip virtual tables and temp tables.
				if table, ok := desc.(catalog.TableDescriptor); ok {
					if !(desc.Public() || desc.Offline()) {
						return nil
					}
					if table.IsVirtualTable() || table.IsTemporary() {
						return nil
					}
					if err := getReferencedTypes(table, db); err != nil {
						return err
					}
					if err := getReferencedFunctions(table); err != nil {
						return err
					}
					if err := addSchemaDesc(table.GetParentSchemaID()); err != nil {
						return err
					}
					addDesc(table)
					return nil
				}

				// For schemas, skip dropped, virtual, and temporary schemas.
				// Include PUBLIC, OFFLINE, and ADD schemas.
				if schema, ok := desc.(catalog.SchemaDescriptor); ok {
					if !shouldSkipSchema(schema) {
						addDesc(schema)
					}
					return nil
				}

				// For other descriptors (types, functions), apply standard filter.
				if !(desc.Public() || desc.Offline()) {
					return nil
				}
				addDesc(desc)
				return nil
			})
			if err != nil {
				return err
			}
		}

		// Process TABLE targets.
		for _, origPattern := range targets.Tables.TablePatterns {
			pattern, err := origPattern.NormalizeTablePattern()
			if err != nil {
				return err
			}

			switch p := pattern.(type) {
			case *tree.TableName:
				// Resolve individual table.
				un := p.ToUnresolvedObjectName()

				found, prefix, desc, err := resolver.ResolveExisting(
					ctx, un, r, tree.ObjectLookupFlags{DesiredObjectKind: tree.TableObject}, currentDatabase, searchPath,
				)
				if err != nil {
					return err
				}
				if !found {
					return errors.Errorf("table %q does not exist", tree.ErrString(p))
				}

				table, ok := desc.(catalog.TableDescriptor)
				if !ok {
					return errors.Errorf("table %q does not exist", tree.ErrString(p))
				}
				if !table.Public() || table.IsTemporary() {
					return errors.Errorf("table %q cannot be backed up", tree.ErrString(p))
				}

				// Add the table and its parent database.
				addDesc(table)
				addDesc(prefix.Database)

				// Add the schema if it's user-defined.
				if err := addSchemaDesc(table.GetParentSchemaID()); err != nil {
					return err
				}

				// Add referenced types and functions.
				if err := getReferencedTypes(table, prefix.Database); err != nil {
					return err
				}
				if err := getReferencedFunctions(table); err != nil {
					return err
				}

				// Track descsByTablePattern for TABLE patterns.
				// Use the original pattern as the key, not the normalized one, to match
				// the behavior of DescriptorsMatchingTargets.
				descsByTablePattern[origPattern] = table

			case *tree.AllTablesSelector:
				res, err := resolveWildcard(ctx, p, r, currentDatabase, searchPath)
				if err != nil {
					return err
				}
				db := res.db
				targetSchema := res.targetSchema
				hasSchemaScope := res.hasSchemaScope

				// Add the database.
				addDesc(db)

				// Track completeDBs for all wildcard patterns (db.*, db.schema.*, schema.*).
				// This matches the behavior of DescriptorsMatchingTargets which always
				// adds to ExpandedDB for any wildcard pattern.
				expandedDBs = append(expandedDBs, db.GetID())

				if hasSchemaScope && targetSchema != nil {
					// Skip virtual and temporary schemas.
					if shouldSkipSchema(targetSchema) {
						return errors.Errorf("schema %q cannot be backed up", tree.ErrNameString(targetSchema.GetName()))
					}

					// Get all objects in the specific schema.
					objects, err := col.GetAllObjectsInSchema(ctx, txn.KV(), db, targetSchema)
					if err != nil {
						return err
					}

					// Add the schema itself.
					addDesc(targetSchema)

					err = objects.ForEachDescriptor(func(desc catalog.Descriptor) error {
						if !(desc.Public() || desc.Offline()) {
							return nil
						}
						// Skip virtual tables and temp tables.
						if table, ok := desc.(catalog.TableDescriptor); ok {
							if table.IsVirtualTable() || table.IsTemporary() {
								return nil
							}
							if err := getReferencedTypes(table, db); err != nil {
								return err
							}
							if err := getReferencedFunctions(table); err != nil {
								return err
							}
							addDesc(table)
							return nil
						}
						// For non-table descriptors (types, functions), add them directly.
						addDesc(desc)
						return nil
					})
					if err != nil {
						return err
					}
				} else {
					// Get all objects in the database.
					allInDB, err := col.GetAllInDatabase(ctx, txn.KV(), db)
					if err != nil {
						return err
					}

					err = allInDB.ForEachDescriptor(func(desc catalog.Descriptor) error {
						// Skip virtual tables and temp tables.
						if table, ok := desc.(catalog.TableDescriptor); ok {
							if !(desc.Public() || desc.Offline()) {
								return nil
							}
							if table.IsVirtualTable() || table.IsTemporary() {
								return nil
							}
							if err := getReferencedTypes(table, db); err != nil {
								return err
							}
							if err := getReferencedFunctions(table); err != nil {
								return err
							}
							if err := addSchemaDesc(table.GetParentSchemaID()); err != nil {
								return err
							}
							addDesc(table)
							return nil
						}
						// For schemas, skip dropped, virtual, and temporary schemas.
						// Include PUBLIC, OFFLINE, and ADD schemas.
						if schema, ok := desc.(catalog.SchemaDescriptor); ok {
							if !shouldSkipSchema(schema) {
								addDesc(schema)
							}
							return nil
						}
						// For other descriptors (types, functions), apply standard filter.
						if !(desc.Public() || desc.Offline()) {
							return nil
						}
						addDesc(desc)
						return nil
					})
					if err != nil {
						return err
					}
				}

			default:
				return errors.Errorf("unknown pattern %T: %+v", pattern, pattern)
			}
		}

		return nil
	})

	// Sort descriptors by ID to ensure consistent ordering for privilege checks
	// and SHOW BACKUP output.
	slices.SortFunc(result, func(a, b catalog.Descriptor) int {
		return cmp.Compare(a.GetID(), b.GetID())
	})

	return result, expandedDBs, requestedDBs, descsByTablePattern, err
}

// simpleResolver implements the resolver interfaces needed for name resolution
// using the Collection APIs.
type simpleResolver struct {
	col *descs.Collection
	txn *kv.Txn
}

// LookupSchema implements the resolver.ObjectNameTargetResolver interface.
func (r *simpleResolver) LookupSchema(
	ctx context.Context, dbName, scName string,
) (bool, catalog.ResolvedObjectPrefix, error) {
	db, err := r.col.ByName(r.txn).MaybeGet().Database(ctx, dbName)
	if err != nil || db == nil {
		return false, catalog.ResolvedObjectPrefix{}, err
	}

	schema, err := r.col.ByName(r.txn).MaybeGet().Schema(ctx, db, scName)
	if err != nil {
		return false, catalog.ResolvedObjectPrefix{}, err
	}

	// Handle synthetic public schema.
	if schema == nil && scName == catconstants.PublicSchemaName {
		if !db.HasPublicSchemaWithDescriptor() {
			schema = schemadesc.GetPublicSchema()
		}
	}

	if schema == nil {
		return false, catalog.ResolvedObjectPrefix{}, nil
	}

	return true, catalog.ResolvedObjectPrefix{
		Database: db,
		Schema:   schema,
	}, nil
}

// LookupObject implements the tree.ObjectNameExistingResolver interface.
func (r *simpleResolver) LookupObject(
	ctx context.Context, flags tree.ObjectLookupFlags, dbName, scName, obName string,
) (bool, catalog.ResolvedObjectPrefix, catalog.Descriptor, error) {
	if flags.DesiredObjectKind != tree.TableObject {
		return false, catalog.ResolvedObjectPrefix{}, nil, errors.Newf("resolver can only lookup tables")
	}

	resolvedPrefix := catalog.ResolvedObjectPrefix{}

	db, err := r.col.ByName(r.txn).MaybeGet().Database(ctx, dbName)
	if err != nil || db == nil {
		return false, resolvedPrefix, nil, err
	}
	resolvedPrefix.Database = db

	schema, err := r.col.ByName(r.txn).MaybeGet().Schema(ctx, db, scName)
	if err != nil {
		return false, resolvedPrefix, nil, err
	}

	// Handle public schema.
	if schema == nil && scName == catconstants.PublicSchemaName {
		if !db.HasPublicSchemaWithDescriptor() {
			schema = schemadesc.GetPublicSchema()
		}
	}

	if schema == nil {
		return false, resolvedPrefix, nil, nil
	}
	resolvedPrefix.Schema = schema

	table, err := r.col.ByName(r.txn).MaybeGet().Table(ctx, db, schema, obName)
	if err != nil {
		return false, resolvedPrefix, nil, err
	}
	if table != nil {
		return true, resolvedPrefix, table, nil
	}

	return false, resolvedPrefix, nil, nil
}
