// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

var _ resolver.SchemaResolver = &planner{}

// runWithOptions sets the provided resolution flags for the
// duration of the call of the passed argument fn.
//
// This is meant to be used like this (for example):
//
// var someVar T
// var err error
// p.runWithOptions(resolveFlags{skipCache: true}, func() {
//    someVar, err = ResolveExistingTableObject(ctx, p, ...)
// })
// if err != nil { ... }
// use(someVar)
func (p *planner) runWithOptions(flags resolveFlags, fn func()) {
	if flags.skipCache {
		defer func(prev bool) { p.avoidLeasedDescriptors = prev }(p.avoidLeasedDescriptors)
		p.avoidLeasedDescriptors = true
	}
	if flags.contextDatabaseID != descpb.InvalidID {
		defer func(prev descpb.ID) { p.contextDatabaseID = prev }(p.contextDatabaseID)
		p.contextDatabaseID = flags.contextDatabaseID
	}
	fn()
}

type resolveFlags struct {
	skipCache         bool
	contextDatabaseID descpb.ID
}

func (p *planner) ResolveMutableTableDescriptor(
	ctx context.Context, tn *tree.TableName, required bool, requiredType tree.RequiredTableKind,
) (prefix catalog.ResolvedObjectPrefix, table *tabledesc.Mutable, err error) {
	prefix, desc, err := resolver.ResolveMutableExistingTableObject(ctx, p, tn, required, requiredType)
	if err != nil {
		return prefix, nil, err
	}

	// Ensure that the current user can access the target schema.
	if desc != nil {
		if err := p.canResolveDescUnderSchema(ctx, prefix.Schema, desc); err != nil {
			return catalog.ResolvedObjectPrefix{}, nil, err
		}
	}

	return prefix, desc, nil
}

// TODO(ajwerner): Remove this and things like it to use more generic
// functionality. We really need to centralize the privilege checking.
func (p *planner) resolveUncachedTableDescriptor(
	ctx context.Context, tn *tree.TableName, required bool, requiredType tree.RequiredTableKind,
) (table catalog.TableDescriptor, err error) {
	var prefix catalog.ResolvedObjectPrefix
	var desc catalog.Descriptor
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		lookupFlags := tree.ObjectLookupFlags{
			CommonLookupFlags:    tree.CommonLookupFlags{Required: required},
			DesiredObjectKind:    tree.TableObject,
			DesiredTableDescKind: requiredType,
		}
		desc, prefix, err = resolver.ResolveExistingObject(
			ctx, p, tn.ToUnresolvedObjectName(), lookupFlags,
		)
	})
	if err != nil || desc == nil {
		return nil, err
	}
	table = desc.(catalog.TableDescriptor)
	// Ensure that the current user can access the target schema.
	if err := p.canResolveDescUnderSchema(ctx, prefix.Schema, table); err != nil {
		return nil, err
	}

	return table, nil
}

func (p *planner) ResolveTargetObject(
	ctx context.Context, un *tree.UnresolvedObjectName,
) (
	db catalog.DatabaseDescriptor,
	schema catalog.SchemaDescriptor,
	namePrefix tree.ObjectNamePrefix,
	err error,
) {
	var prefix catalog.ResolvedObjectPrefix
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		prefix, namePrefix, err = resolver.ResolveTargetObject(ctx, p, un)
	})
	if err != nil {
		return nil, nil, namePrefix, err
	}
	return prefix.Database, prefix.Schema, namePrefix, err
}

// LookupSchema implements the resolver.ObjectNameTargetResolver interface.
func (p *planner) LookupSchema(
	ctx context.Context, dbName, scName string,
) (found bool, scMeta catalog.ResolvedObjectPrefix, err error) {
	dbDesc, err := p.Descriptors().GetImmutableDatabaseByName(ctx, p.txn, dbName,
		tree.DatabaseLookupFlags{AvoidLeased: p.avoidLeasedDescriptors})
	if err != nil || dbDesc == nil {
		return false, catalog.ResolvedObjectPrefix{}, err
	}
	sc := p.Accessor()
	var resolvedSchema catalog.SchemaDescriptor
	resolvedSchema, err = sc.GetSchemaByName(
		ctx, p.txn, dbDesc, scName, p.CommonLookupFlags(false /* required */),
	)
	if err != nil || resolvedSchema == nil {
		return false, catalog.ResolvedObjectPrefix{}, err
	}
	return true, catalog.ResolvedObjectPrefix{
		Database: dbDesc,
		Schema:   resolvedSchema,
	}, nil
}

// SchemaExists implements the eval.DatabaseCatalog interface.
func (p *planner) SchemaExists(ctx context.Context, dbName, scName string) (found bool, err error) {
	found, _, err = p.LookupSchema(ctx, dbName, scName)
	return found, err
}

// LookupObject implements the tree.ObjectNameExistingResolver interface.
func (p *planner) LookupObject(
	ctx context.Context, flags tree.ObjectLookupFlags, dbName, scName, obName string,
) (found bool, prefix catalog.ResolvedObjectPrefix, objMeta catalog.Descriptor, err error) {
	sc := p.Accessor()
	flags.CommonLookupFlags.Required = false
	flags.CommonLookupFlags.AvoidLeased = p.avoidLeasedDescriptors

	// Check if we are looking up a type which matches a built-in type in
	// CockroachDB but is an extension type on the public schema in PostgreSQL.
	if flags.DesiredObjectKind == tree.TypeObject && scName == tree.PublicSchema {
		if alias, ok := types.PublicSchemaAliases[obName]; ok {
			if flags.RequireMutable {
				return true, catalog.ResolvedObjectPrefix{}, nil, pgerror.Newf(pgcode.WrongObjectType, "type %q is a built-in type", obName)
			}

			found, prefix, err = p.LookupSchema(ctx, dbName, scName)
			if err != nil || !found {
				return found, prefix, nil, err
			}
			dbDesc, err := p.Descriptors().GetImmutableDatabaseByName(ctx, p.txn, dbName,
				tree.DatabaseLookupFlags{AvoidLeased: p.avoidLeasedDescriptors})
			if err != nil {
				return found, prefix, nil, err
			}
			if dbDesc.HasPublicSchemaWithDescriptor() {
				publicSchemaID := dbDesc.GetSchemaID(tree.PublicSchema)
				return true, prefix, typedesc.MakeSimpleAlias(alias, publicSchemaID), nil
			}
			return true, prefix, typedesc.MakeSimpleAlias(alias, keys.PublicSchemaID), nil
		}
	}

	prefix, objMeta, err = sc.GetObjectDesc(ctx, p.txn, dbName, scName, obName, flags)
	return objMeta != nil, prefix, objMeta, err
}

// CommonLookupFlags is part of the resolver.SchemaResolver interface.
func (p *planner) CommonLookupFlags(required bool) tree.CommonLookupFlags {
	return tree.CommonLookupFlags{
		Required:    required,
		AvoidLeased: p.avoidLeasedDescriptors,
	}
}

// IsTableVisible is part of the eval.DatabaseCatalog interface.
func (p *planner) IsTableVisible(
	ctx context.Context, curDB string, searchPath sessiondata.SearchPath, tableID oid.Oid,
) (isVisible, exists bool, err error) {
	tableDesc, err := p.LookupTableByID(ctx, descpb.ID(tableID))
	if err != nil {
		// If a "not found" error happened here, we return "not exists" rather than
		// the error.
		if errors.Is(err, catalog.ErrDescriptorNotFound) ||
			errors.Is(err, catalog.ErrDescriptorDropped) ||
			pgerror.GetPGCode(err) == pgcode.UndefinedTable ||
			pgerror.GetPGCode(err) == pgcode.UndefinedObject {
			return false, false, nil //nolint:returnerrcheck
		}
		return false, false, err
	}
	schemaID := tableDesc.GetParentSchemaID()
	schemaDesc, err := p.Descriptors().GetImmutableSchemaByID(ctx, p.Txn(), schemaID,
		tree.SchemaLookupFlags{
			Required:    true,
			AvoidLeased: p.avoidLeasedDescriptors})
	if err != nil {
		return false, false, err
	}
	if schemaDesc.SchemaKind() != catalog.SchemaVirtual {
		dbID := tableDesc.GetParentID()
		_, dbDesc, err := p.Descriptors().GetImmutableDatabaseByID(ctx, p.Txn(), dbID,
			tree.DatabaseLookupFlags{
				Required:    true,
				AvoidLeased: p.avoidLeasedDescriptors})
		if err != nil {
			return false, false, err
		}
		if dbDesc.GetName() != curDB {
			// If the table is in a different database, then it's considered to be
			// "not existing" instead of just "not visible"; this matches PostgreSQL.
			return false, false, nil
		}
	}
	iter := searchPath.Iter()
	for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
		if schemaDesc.GetName() == scName {
			return true, true, nil
		}
	}
	return false, true, nil
}

// IsTypeVisible is part of the eval.DatabaseCatalog interface.
func (p *planner) IsTypeVisible(
	ctx context.Context, curDB string, searchPath sessiondata.SearchPath, typeID oid.Oid,
) (isVisible bool, exists bool, err error) {
	// Check builtin types first. They are always globally visible.
	if _, ok := types.OidToType[typeID]; ok {
		return true, true, nil
	}

	if !types.IsOIDUserDefinedType(typeID) {
		return false, false, nil //nolint:returnerrcheck
	}

	id, err := typedesc.UserDefinedTypeOIDToID(typeID)
	if err != nil {
		return false, false, err
	}
	typName, _, err := p.GetTypeDescriptor(ctx, id)
	if err != nil {
		// If a "not found" error happened here, we return "not exists" rather than
		// the error.
		if errors.Is(err, catalog.ErrDescriptorNotFound) ||
			errors.Is(err, catalog.ErrDescriptorDropped) ||
			pgerror.GetPGCode(err) == pgcode.UndefinedObject {
			return false, false, nil //nolint:returnerrcheck
		}
		return false, false, err
	}
	if typName.CatalogName.String() != curDB {
		// If the type is in a different database, then it's considered to be
		// "not existing" instead of just "not visible"; this matches PostgreSQL.
		return false, false, nil
	}
	iter := searchPath.Iter()
	for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
		if typName.SchemaName.String() == scName {
			return true, true, nil
		}
	}
	return false, true, nil
}

// HasAnyPrivilege is part of the eval.DatabaseCatalog interface.
func (p *planner) HasAnyPrivilege(
	ctx context.Context,
	specifier eval.HasPrivilegeSpecifier,
	user username.SQLUsername,
	privs []privilege.Privilege,
) (eval.HasAnyPrivilegeResult, error) {
	desc, err := p.ResolveDescriptorForPrivilegeSpecifier(
		ctx,
		specifier,
	)
	if err != nil {
		return eval.HasNoPrivilege, err
	}
	if desc == nil {
		return eval.ObjectNotFound, nil
	}

	for _, priv := range privs {
		// RULE was only added for compatibility with Postgres, and Postgres
		// never allows RULE to be granted, even if the user has ALL privileges.
		// See https://www.postgresql.org/docs/8.1/sql-grant.html
		// and https://www.postgresql.org/docs/release/8.2.0/.
		if priv.Kind == privilege.RULE {
			continue
		}

		if err := p.CheckPrivilegeForUser(ctx, desc, priv.Kind, user); err != nil {
			if pgerror.GetPGCode(err) == pgcode.InsufficientPrivilege {
				continue
			}
			return eval.HasNoPrivilege, err
		}

		if priv.GrantOption {
			if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.ValidateGrantOption) {
				if err := p.CheckPrivilegeForUser(ctx, desc, privilege.GRANT, user); err != nil {
					if pgerror.GetPGCode(err) == pgcode.InsufficientPrivilege {
						continue
					}
					return eval.HasNoPrivilege, err
				}
			} else {
				if err := p.CheckGrantOptionsForUser(ctx, desc, []privilege.Kind{priv.Kind}, user, true /* isGrant */); err != nil {
					if pgerror.GetPGCode(err) == pgcode.WarningPrivilegeNotGranted {
						continue
					}
					return eval.HasNoPrivilege, err
				}
			}
		}
		return eval.HasPrivilege, nil
	}

	return eval.HasNoPrivilege, nil
}

// ResolveDescriptorForPrivilegeSpecifier resolves a tree.HasPrivilegeSpecifier
// and returns the descriptor for the given object.
func (p *planner) ResolveDescriptorForPrivilegeSpecifier(
	ctx context.Context, specifier eval.HasPrivilegeSpecifier,
) (catalog.Descriptor, error) {
	if specifier.DatabaseName != nil {
		return p.Descriptors().GetImmutableDatabaseByName(
			ctx, p.txn, *specifier.DatabaseName, tree.DatabaseLookupFlags{Required: true},
		)
	} else if specifier.DatabaseOID != nil {
		_, database, err := p.Descriptors().GetImmutableDatabaseByID(
			ctx, p.txn, descpb.ID(*specifier.DatabaseOID), tree.DatabaseLookupFlags{},
		)
		return database, err
	} else if specifier.SchemaName != nil {
		database, err := p.Descriptors().GetImmutableDatabaseByName(
			ctx, p.txn, *specifier.SchemaDatabaseName, tree.DatabaseLookupFlags{Required: true},
		)
		if err != nil {
			return nil, err
		}
		return p.Descriptors().GetImmutableSchemaByName(
			ctx, p.txn, database, *specifier.SchemaName, tree.SchemaLookupFlags{Required: *specifier.SchemaIsRequired},
		)
	} else if specifier.TableName != nil || specifier.TableOID != nil {
		var table catalog.TableDescriptor
		var err error
		if specifier.TableName != nil {
			var tn *tree.TableName
			tn, err = parser.ParseQualifiedTableName(*specifier.TableName)
			if err != nil {
				return nil, err
			}
			if _, err = p.ResolveTableName(ctx, tn); err != nil {
				return nil, err
			}

			if p.SessionData().Database != "" && p.SessionData().Database != string(tn.CatalogName) {
				// Postgres does not allow cross-database references in these
				// functions, so we don't either.
				return nil, pgerror.Newf(pgcode.FeatureNotSupported,
					"cross-database references are not implemented: %s", tn)
			}
			_, table, err = p.Descriptors().GetImmutableTableByName(
				ctx, p.txn, tn, tree.ObjectLookupFlags{},
			)
		} else {
			table, err = p.Descriptors().GetImmutableTableByID(
				ctx, p.txn, descpb.ID(*specifier.TableOID), tree.ObjectLookupFlags{},
			)
			// When a TableOID is specified and the relation is not found, we return NULL.
			if err != nil && sqlerrors.IsUndefinedRelationError(err) {
				// nolint:returnerrcheck
				return nil, nil
			}
		}
		if err != nil {
			return nil, err
		}
		if *specifier.IsSequence {
			// has_table_privilege works with sequences, but has_sequence_privilege does not work with tables
			if !table.IsSequence() {
				return nil, pgerror.Newf(pgcode.WrongObjectType,
					"\"%s\" is not a sequence", table.GetName())
			}
		} else {
			if err := validateColumnForHasPrivilegeSpecifier(
				table,
				specifier,
			); err != nil {
				return nil, err
			}
		}
		return table, nil
	}
	return nil, errors.AssertionFailedf("invalid HasPrivilegeSpecifier")
}

func validateColumnForHasPrivilegeSpecifier(
	table catalog.TableDescriptor, specifier eval.HasPrivilegeSpecifier,
) error {
	if specifier.ColumnName != nil {
		_, err := table.FindColumnWithName(*specifier.ColumnName)
		return err
	}
	if specifier.ColumnAttNum != nil {
		for _, col := range table.PublicColumns() {
			if uint32(col.GetPGAttributeNum()) == *specifier.ColumnAttNum {
				return nil
			}
		}
		return pgerror.Newf(
			pgcode.UndefinedColumn,
			"column %d of relation %s does not exist",
			*specifier.ColumnAttNum,
			tree.Name(table.GetName()),
		)

	}
	return nil
}

// GetTypeDescriptor implements the descpb.TypeDescriptorResolver interface.
func (p *planner) GetTypeDescriptor(
	ctx context.Context, id descpb.ID,
) (tree.TypeName, catalog.TypeDescriptor, error) {
	desc, err := p.Descriptors().GetImmutableTypeByID(ctx, p.txn, id, tree.ObjectLookupFlags{})
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	// Note that the value of required doesn't matter for lookups by ID.
	_, dbDesc, err := p.Descriptors().GetImmutableDatabaseByID(ctx, p.txn, desc.GetParentID(), p.CommonLookupFlags(true /* required */))
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	sc, err := p.Descriptors().GetImmutableSchemaByID(
		ctx, p.txn, desc.GetParentSchemaID(), tree.SchemaLookupFlags{Required: true})
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	name := tree.MakeQualifiedTypeName(dbDesc.GetName(), sc.GetName(), desc.GetName())
	return name, desc, nil
}

// ResolveType implements the TypeReferenceResolver interface.
func (p *planner) ResolveType(
	ctx context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{Required: true, RequireMutable: false},
		DesiredObjectKind: tree.TypeObject,
	}
	desc, prefix, err := resolver.ResolveExistingObject(ctx, p, name, lookupFlags)
	if err != nil {
		return nil, err
	}
	// For "reasons" we always fully qualify type names which are resolved via
	// the type reference resolver.
	//
	// TODO(ajwerner): Understand these reasons.
	prefix.ExplicitDatabase = prefix.Database != nil
	prefix.ExplicitSchema = prefix.Schema != nil
	tn := tree.MakeTypeNameWithPrefix(prefix.NamePrefix(), name.Object())
	tdesc := desc.(catalog.TypeDescriptor)

	// Disllow cross-database type resolution. Note that we check
	// p.contextDatabaseID != descpb.InvalidID when we have been restricted to
	// accessing types in the database with ID = p.contextDatabaseID by
	// p.runWithOptions. So, check to see if the resolved descriptor's parentID
	// matches, unless the descriptor's parentID is invalid. This could happen
	// when the type being resolved is a builtin type prefaced with a virtual
	// schema like `pg_catalog.int`. Resolution for these types returns a dummy
	// TypeDescriptor, so ignore those cases.
	if p.contextDatabaseID != descpb.InvalidID && tdesc.GetParentID() != descpb.InvalidID && tdesc.GetParentID() != p.contextDatabaseID {
		return nil, pgerror.Newf(
			pgcode.FeatureNotSupported, "cross database type references are not supported: %s", tn.String())
	}

	// Ensure that the user can access the target schema.
	if err := p.canResolveDescUnderSchema(ctx, prefix.Schema, tdesc); err != nil {
		return nil, err
	}

	return tdesc.MakeTypesT(ctx, &tn, p)
}

// ResolveTypeByOID implements the tree.TypeResolver interface.
func (p *planner) ResolveTypeByOID(ctx context.Context, oid oid.Oid) (*types.T, error) {
	id, err := typedesc.UserDefinedTypeOIDToID(oid)
	if err != nil {
		return nil, err
	}
	name, desc, err := p.GetTypeDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	return desc.MakeTypesT(ctx, &name, p)
}

// ObjectLookupFlags is part of the resolver.SchemaResolver interface.
func (p *planner) ObjectLookupFlags(required, requireMutable bool) tree.ObjectLookupFlags {
	flags := p.CommonLookupFlags(required)
	flags.RequireMutable = requireMutable
	return tree.ObjectLookupFlags{CommonLookupFlags: flags}
}

// getDescriptorsFromTargetListForPrivilegeChange fetches the descriptors for the targets.
func getDescriptorsFromTargetListForPrivilegeChange(
	ctx context.Context, p *planner, targets tree.TargetList,
) ([]catalog.Descriptor, error) {
	const required = true
	flags := tree.CommonLookupFlags{
		Required:       required,
		AvoidLeased:    p.avoidLeasedDescriptors,
		RequireMutable: true,
	}
	if targets.Databases != nil {
		if len(targets.Databases) == 0 {
			return nil, errNoDatabase
		}
		descs := make([]catalog.Descriptor, 0, len(targets.Databases))
		for _, database := range targets.Databases {
			descriptor, err := p.Descriptors().
				GetMutableDatabaseByName(ctx, p.txn, string(database), flags)
			if err != nil {
				return nil, err
			}
			descs = append(descs, descriptor)
		}
		if len(descs) == 0 {
			return nil, errNoMatch
		}
		return descs, nil
	}

	if targets.Types != nil {
		if len(targets.Types) == 0 {
			return nil, errNoType
		}
		descs := make([]catalog.Descriptor, 0, len(targets.Types))
		for _, typ := range targets.Types {
			_, descriptor, err := p.ResolveMutableTypeDescriptor(ctx, typ, required)
			if err != nil {
				return nil, err
			}

			descs = append(descs, descriptor)
		}

		if len(descs) == 0 {
			return nil, errNoMatch
		}
		return descs, nil
	}

	if targets.Schemas != nil {
		if len(targets.Schemas) == 0 {
			return nil, errNoSchema
		}
		if targets.AllTablesInSchema {
			// Get all the descriptors for the tables in the specified schemas.
			db, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, p.CurrentDatabase(), flags)
			if err != nil {
				return nil, err
			}
			var descs []catalog.Descriptor
			for _, sc := range targets.Schemas {
				_, objectIDs, err := resolver.GetObjectNamesAndIDs(
					ctx, p.txn, p, p.ExecCfg().Codec, db, sc.Schema(), true, /* explicitPrefix */
				)
				if err != nil {
					return nil, err
				}
				muts, err := p.Descriptors().GetMutableDescriptorsByID(ctx, p.txn, objectIDs...)
				if err != nil {
					return nil, err
				}
				for _, mut := range muts {
					if mut != nil && mut.DescriptorType() == catalog.Table {
						descs = append(descs, mut)
					}
				}
			}

			return descs, nil
		}

		descs := make([]catalog.Descriptor, 0, len(targets.Schemas))

		// Resolve the databases being changed
		type schemaWithDBDesc struct {
			schema string
			dbDesc *dbdesc.Mutable
		}
		var targetSchemas []schemaWithDBDesc
		for _, sc := range targets.Schemas {
			dbName := p.CurrentDatabase()
			if sc.ExplicitCatalog {
				dbName = sc.Catalog()
			}
			db, err := p.Descriptors().GetMutableDatabaseByName(ctx, p.txn, dbName, flags)
			if err != nil {
				return nil, err
			}
			targetSchemas = append(targetSchemas, schemaWithDBDesc{schema: sc.Schema(), dbDesc: db})
		}

		for _, sc := range targetSchemas {
			resSchema, err := p.Descriptors().GetSchemaByName(
				ctx, p.txn, sc.dbDesc, sc.schema, flags)
			if err != nil {
				return nil, err
			}
			switch resSchema.SchemaKind() {
			case catalog.SchemaUserDefined:
				descs = append(descs, resSchema)
			default:
				return nil, pgerror.Newf(pgcode.InvalidSchemaName,
					"cannot change privileges on schema %q", resSchema.GetName())
			}
		}
		return descs, nil
	}

	if len(targets.Tables) == 0 {
		return nil, errNoTable
	}
	descs := make([]catalog.Descriptor, 0, len(targets.Tables))
	for _, tableTarget := range targets.Tables {
		tableGlob, err := tableTarget.NormalizeTablePattern()
		if err != nil {
			return nil, err
		}
		_, objectIDs, err := expandTableGlob(ctx, p, tableGlob)
		if err != nil {
			return nil, err
		}
		muts, err := p.Descriptors().GetMutableDescriptorsByID(ctx, p.txn, objectIDs...)
		if err != nil {
			return nil, err
		}
		for _, mut := range muts {
			if mut != nil && mut.DescriptorType() == catalog.Table {
				descs = append(descs, mut)
			}
		}
	}
	if len(descs) == 0 {
		return nil, errNoMatch
	}
	return descs, nil
}

// getFullyQualifiedTableNamesFromIDs resolves a list of table IDs to their
// fully qualified names.
func (p *planner) getFullyQualifiedTableNamesFromIDs(
	ctx context.Context, ids []descpb.ID,
) (fullyQualifiedNames []*tree.TableName, _ error) {
	for _, id := range ids {
		desc, err := p.Descriptors().GetImmutableTableByID(ctx, p.txn, id, tree.ObjectLookupFlags{
			CommonLookupFlags: tree.CommonLookupFlags{
				AvoidLeased:    true,
				IncludeDropped: true,
				IncludeOffline: true,
			},
		})
		if err != nil {
			return nil, err
		}
		fqName, err := p.getQualifiedTableName(ctx, desc)
		if err != nil {
			return nil, err
		}
		fullyQualifiedNames = append(fullyQualifiedNames, fqName)
	}
	return fullyQualifiedNames, nil
}

// getQualifiedTableName returns the database-qualified name of the table
// or view represented by the provided descriptor. It is a sort of
// reverse of the Resolve() functions.
func (p *planner) getQualifiedTableName(
	ctx context.Context, desc catalog.TableDescriptor,
) (*tree.TableName, error) {
	_, dbDesc, err := p.Descriptors().GetImmutableDatabaseByID(ctx, p.txn, desc.GetParentID(),
		tree.DatabaseLookupFlags{
			Required:       true,
			IncludeOffline: true,
			IncludeDropped: true,
			AvoidLeased:    true,
		})
	if err != nil {
		return nil, err
	}
	// Get the schema name. Use some specialized logic to deal with descriptors
	// from other temporary schemas.
	//
	// TODO(ajwerner): We shouldn't need this temporary logic if we properly
	// tracked all descriptors as we read them and made them available in the
	// collection. We should only be hitting this edge case when dropping a
	// database, in which case we've already read all of the temporary schema
	// information from the namespace table.
	var schemaName tree.Name
	schemaID := desc.GetParentSchemaID()
	scDesc, err := p.Descriptors().GetImmutableSchemaByID(ctx, p.txn, schemaID,
		tree.SchemaLookupFlags{
			IncludeOffline: true,
			IncludeDropped: true,
			AvoidLeased:    true,
		})
	switch {
	case scDesc != nil:
		schemaName = tree.Name(scDesc.GetName())
	case desc.IsTemporary() && scDesc == nil:
		// We've lost track of the session which owned this schema, but we
		// can come up with a name that is also going to be unique and
		// informative and looks like a pg_temp_<session_id> name.
		schemaName = tree.Name(fmt.Sprintf("pg_temp_%d", schemaID))
	default:
		return nil, errors.Wrapf(err,
			"resolving schema name for %s.[%d].%s",
			tree.Name(dbDesc.GetName()),
			schemaID,
			tree.Name(desc.GetName()),
		)
	}

	tbName := tree.MakeTableNameWithSchema(
		tree.Name(dbDesc.GetName()),
		schemaName,
		tree.Name(desc.GetName()),
	)
	return &tbName, nil
}

// GetQualifiedTableNameByID returns the qualified name of the table,
// view or sequence represented by the provided ID and table kind.
func (p *planner) GetQualifiedTableNameByID(
	ctx context.Context, id int64, requiredType tree.RequiredTableKind,
) (*tree.TableName, error) {
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags:    tree.CommonLookupFlags{Required: true},
		DesiredObjectKind:    tree.TableObject,
		DesiredTableDescKind: requiredType,
	}

	table, err := p.Descriptors().GetImmutableTableByID(
		ctx, p.txn, descpb.ID(id), lookupFlags)
	if err != nil {
		return nil, err
	}
	return p.getQualifiedTableName(ctx, table)
}

// getQualifiedSchemaName returns the database-qualified name of the
// schema represented by the provided descriptor.
func (p *planner) getQualifiedSchemaName(
	ctx context.Context, desc catalog.SchemaDescriptor,
) (*tree.ObjectNamePrefix, error) {
	_, dbDesc, err := p.Descriptors().GetImmutableDatabaseByID(ctx, p.txn, desc.GetParentID(),
		tree.DatabaseLookupFlags{
			Required:    true,
			AvoidLeased: true,
		})
	if err != nil {
		return nil, err
	}
	return &tree.ObjectNamePrefix{
		CatalogName:     tree.Name(dbDesc.GetName()),
		SchemaName:      tree.Name(desc.GetName()),
		ExplicitCatalog: true,
		ExplicitSchema:  true,
	}, nil
}

// getQualifiedTypeName returns the database-qualified name of the type
// represented by the provided descriptor.
func (p *planner) getQualifiedTypeName(
	ctx context.Context, desc catalog.TypeDescriptor,
) (*tree.TypeName, error) {
	_, dbDesc, err := p.Descriptors().GetImmutableDatabaseByID(ctx, p.txn, desc.GetParentID(),
		tree.DatabaseLookupFlags{
			Required:    true,
			AvoidLeased: true,
		})
	if err != nil {
		return nil, err
	}

	schemaID := desc.GetParentSchemaID()
	scDesc, err := p.Descriptors().GetImmutableSchemaByID(
		ctx, p.txn, schemaID, tree.SchemaLookupFlags{Required: true},
	)
	if err != nil {
		return nil, err
	}

	typeName := tree.MakeQualifiedTypeName(
		dbDesc.GetName(),
		scDesc.GetName(),
		desc.GetName(),
	)

	return &typeName, nil
}

// findTableContainingIndex returns the descriptor of a table
// containing the index of the given name.
// This is used by expandMutableIndexName().
//
// An error is returned if the index name is ambiguous (i.e. exists in
// multiple tables). If no table is found and requireTable is true, an
// error will be returned, otherwise the TableName and descriptor
// returned will be nil.
func findTableContainingIndex(
	ctx context.Context,
	txn *kv.Txn,
	sc resolver.SchemaResolver,
	codec keys.SQLCodec,
	dbName, scName string,
	idxName tree.UnrestrictedName,
	lookupFlags tree.CommonLookupFlags,
) (result *tree.TableName, desc *tabledesc.Mutable, err error) {
	sa := sc.Accessor()
	dbDesc, err := sa.GetDatabaseDesc(ctx, txn, dbName, lookupFlags)
	if dbDesc == nil || err != nil {
		return nil, nil, err
	}

	tns, _, err := sa.GetObjectNamesAndIDs(
		ctx, txn, dbDesc, scName, tree.DatabaseListFlags{
			CommonLookupFlags: lookupFlags,
			ExplicitPrefix:    true,
		},
	)
	if err != nil {
		return nil, nil, err
	}

	result = nil
	for i := range tns {
		tn := &tns[i]
		_, tableDesc, err := resolver.ResolveMutableExistingTableObject(
			ctx, sc, tn, false /*required*/, tree.ResolveAnyTableKind,
		)
		if err != nil {
			return nil, nil, err
		}
		if tableDesc == nil || !(tableDesc.IsTable() || tableDesc.MaterializedView()) {
			continue
		}

		idx, err := tableDesc.FindIndexWithName(string(idxName))
		if err != nil || idx.Dropped() {
			// err is nil if the index does not exist on the table.
			continue
		}
		if result != nil {
			return nil, nil, pgerror.Newf(pgcode.AmbiguousParameter,
				"index name %q is ambiguous (found in %s and %s)",
				idxName, tn.String(), result.String())
		}
		result = tn
		desc = tableDesc
	}
	if result == nil && lookupFlags.Required {
		return nil, nil, pgerror.Newf(pgcode.UndefinedObject,
			"index %q does not exist", idxName)
	}
	return result, desc, nil
}

// expandMutableIndexName ensures that the index name is qualified with a table
// name, and searches the table name if not yet specified.
//
// It returns the TableName of the underlying table for convenience.
// If no table is found and requireTable is true an error will be
// returned, otherwise the TableName returned will be nil.
//
// It *may* return the descriptor of the underlying table, depending
// on the lookup path. This can be used in the caller to avoid a 2nd
// lookup.
func expandMutableIndexName(
	ctx context.Context, p *planner, index *tree.TableIndexName, requireTable bool,
) (tn *tree.TableName, desc *tabledesc.Mutable, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		tn, desc, err = expandIndexName(ctx, p.txn, p, p.ExecCfg().Codec, index, requireTable)
	})
	return tn, desc, err
}

func expandIndexName(
	ctx context.Context,
	txn *kv.Txn,
	sc resolver.SchemaResolver,
	codec keys.SQLCodec,
	index *tree.TableIndexName,
	requireTable bool,
) (tn *tree.TableName, desc *tabledesc.Mutable, err error) {
	tn = &index.Table
	if tn.Table() != "" {
		// The index and its table prefix must exist already. Resolve the table.
		_, desc, err = resolver.ResolveMutableExistingTableObject(ctx, sc, tn, requireTable, tree.ResolveRequireTableOrViewDesc)
		if err != nil {
			return nil, nil, err
		}
		if desc != nil && desc.IsView() && !desc.MaterializedView() {
			return nil, nil, pgerror.Newf(pgcode.WrongObjectType,
				"%q is not a table or materialized view", tn.Table())
		}
		return tn, desc, nil
	}

	// On the first call to expandMutableIndexName(), index.Table.Table() is empty.
	// Once the table name is resolved for the index below, index.Table
	// references the table name.

	// Look up the table prefix.
	found, _, err := resolver.ResolveObjectNamePrefix(
		ctx, sc, sc.CurrentDatabase(), sc.CurrentSearchPath(), &tn.ObjectNamePrefix,
	)
	if err != nil {
		return nil, nil, err
	}
	if !found {
		if requireTable {
			err = pgerror.Newf(pgcode.UndefinedObject,
				"schema or database was not found while searching index: %q",
				tree.ErrString(&index.Index))
			err = errors.WithHint(err, "check the current database and search_path are valid")
			return nil, nil, err
		}
		return nil, nil, nil
	}

	lookupFlags := sc.CommonLookupFlags(requireTable)
	var foundTn *tree.TableName
	foundTn, desc, err = findTableContainingIndex(
		ctx, txn, sc, codec, tn.Catalog(), tn.Schema(), index.Index, lookupFlags,
	)
	if err != nil {
		return nil, nil, err
	}

	if foundTn != nil {
		// Memoize the table name that was found. tn is a reference to the table name
		// stored in index.Table.
		*tn = *foundTn
	}
	return tn, desc, nil
}

// getTableAndIndex returns the table and index descriptors for a
// TableIndexName.
//
// It can return indexes that are being rolled out.
func (p *planner) getTableAndIndex(
	ctx context.Context, tableWithIndex *tree.TableIndexName, privilege privilege.Kind,
) (*tabledesc.Mutable, catalog.Index, error) {
	var catalog optCatalog
	catalog.init(p)
	catalog.reset()

	idx, qualifiedName, err := cat.ResolveTableIndex(
		ctx, &catalog, cat.Flags{AvoidDescriptorCaches: true}, tableWithIndex,
	)
	if err != nil {
		return nil, nil, err
	}
	if err := catalog.CheckPrivilege(ctx, idx.Table(), privilege); err != nil {
		return nil, nil, err
	}
	optIdx := idx.(*optIndex)

	// Set the object name for logging if it's missing.
	if tableWithIndex.Table.ObjectName == "" {
		tableWithIndex.Table = tree.MakeTableNameFromPrefix(
			qualifiedName.ObjectNamePrefix, qualifiedName.ObjectName,
		)
	}

	// Use the descriptor collection to get a proper handle to the mutable
	// descriptor for the relevant table and use that mutable object to
	// get a handle to the corresponding index.
	tableID := optIdx.tab.desc.GetID()
	mut, err := p.Descriptors().GetMutableTableVersionByID(ctx, tableID, p.Txn())
	if err != nil {
		return nil, nil, errors.NewAssertionErrorWithWrappedErrf(err,
			"failed to re-resolve table %d for index %s", tableID, tableWithIndex)
	}
	retIdx, err := mut.FindIndexWithID(optIdx.idx.GetID())
	if err != nil {
		return nil, nil, errors.NewAssertionErrorWithWrappedErrf(err,
			"retrieving index %s (%d) from table which was known to already exist for table %d",
			tableWithIndex, optIdx.idx.GetID(), tableID)
	}
	return mut, retIdx, nil
}

// expandTableGlob expands pattern into a list of objects represented
// as a tree.TableNames.
func expandTableGlob(
	ctx context.Context, p *planner, pattern tree.TablePattern,
) (tree.TableNames, descpb.IDs, error) {
	var catalog optCatalog
	catalog.init(p)
	catalog.reset()

	return cat.ExpandDataSourceGlob(ctx, &catalog, cat.Flags{}, pattern)
}

// fkSelfResolver is a SchemaResolver that inserts itself between a
// user of name resolution and another SchemaResolver, and will answer
// lookups of the new table being created. This is needed in the case
// of CREATE TABLE with a foreign key self-reference: the target of
// the FK definition is a table that does not exist yet.
type fkSelfResolver struct {
	resolver.SchemaResolver
	prefix       catalog.ResolvedObjectPrefix
	newTableName *tree.TableName
	newTableDesc *tabledesc.Mutable
}

var _ resolver.SchemaResolver = &fkSelfResolver{}

// LookupObject implements the tree.ObjectNameExistingResolver interface.
func (r *fkSelfResolver) LookupObject(
	ctx context.Context, flags tree.ObjectLookupFlags, dbName, scName, obName string,
) (found bool, prefix catalog.ResolvedObjectPrefix, objMeta catalog.Descriptor, err error) {
	if dbName == r.prefix.Database.GetName() &&
		scName == r.prefix.Schema.GetName() &&
		obName == r.newTableName.Table() {
		table := r.newTableDesc
		if flags.RequireMutable {
			return true, r.prefix, table, nil
		}
		return true, r.prefix, table.ImmutableCopy(), nil
	}
	flags.IncludeOffline = false
	return r.SchemaResolver.LookupObject(ctx, flags, dbName, scName, obName)
}

// internalLookupCtx can be used in contexts where all descriptors
// have been recently read, to accelerate the lookup of
// inter-descriptor relationships.
//
// This is used mainly in the generators for virtual tables,
// aliased as tableLookupFn below.
//
// It only reveals physical descriptors (not virtual descriptors).
//
// TODO(ajwerner): remove in 21.2 or whenever cross-database references are
// fully removed.
type internalLookupCtx struct {
	dbNames     map[descpb.ID]string
	dbIDs       []descpb.ID
	dbDescs     map[descpb.ID]catalog.DatabaseDescriptor
	schemaDescs map[descpb.ID]catalog.SchemaDescriptor
	schemaNames map[descpb.ID]string
	schemaIDs   []descpb.ID
	tbDescs     map[descpb.ID]catalog.TableDescriptor
	tbIDs       []descpb.ID
	typDescs    map[descpb.ID]catalog.TypeDescriptor
	typIDs      []descpb.ID
}

// GetSchemaName looks up a schema with the given id in the LookupContext.
func (l *internalLookupCtx) GetSchemaName(
	ctx context.Context, id, parentDBID descpb.ID, version clusterversion.Handle,
) (string, bool, error) {
	dbDesc, err := l.getDatabaseByID(parentDBID)
	if err != nil {
		return "", false, err
	}

	// If a db does not have a public schema backed by a descriptor, we can
	// assume that its public schema ID is 29. This is valid since we cannot
	// drop the public schema in v21.2 or v22.1.
	if !dbDesc.HasPublicSchemaWithDescriptor() {
		if id == keys.PublicSchemaID {
			return tree.PublicSchema, true, nil
		}
	}

	if parentDBID == keys.SystemDatabaseID {
		if id == keys.SystemPublicSchemaID {
			return tree.PublicSchema, true, nil
		}
	}

	schemaName, found := l.schemaNames[id]
	return schemaName, found, nil
}

// tableLookupFn can be used to retrieve a table descriptor and its corresponding
// database descriptor using the table's ID.
type tableLookupFn = *internalLookupCtx

// newInternalLookupCtxFromDescriptorProtos "unwraps" the descriptors into the
// appropriate implementation of Descriptor before constructing a new
// internalLookupCtx. It also hydrates any table descriptors with enum
// information. It is intended only for use when dealing with backups.
func newInternalLookupCtxFromDescriptorProtos(
	ctx context.Context, rawDescs []descpb.Descriptor, prefix catalog.DatabaseDescriptor,
) (*internalLookupCtx, error) {
	descriptors := make([]catalog.Descriptor, len(rawDescs))
	for i := range rawDescs {
		descriptors[i] = descbuilder.NewBuilder(&rawDescs[i]).BuildImmutable()
	}
	lCtx := newInternalLookupCtx(descriptors, prefix)
	if err := descs.HydrateGivenDescriptors(ctx, descriptors); err != nil {
		return nil, err
	}
	return lCtx, nil
}

// newInternalLookupCtx provides cached access to a set of descriptors for use
// in virtual tables.
func newInternalLookupCtx(
	descs []catalog.Descriptor, prefix catalog.DatabaseDescriptor,
) *internalLookupCtx {
	dbNames := make(map[descpb.ID]string)
	dbDescs := make(map[descpb.ID]catalog.DatabaseDescriptor)
	schemaDescs := make(map[descpb.ID]catalog.SchemaDescriptor)
	schemaNames := make(map[descpb.ID]string)

	tbDescs := make(map[descpb.ID]catalog.TableDescriptor)
	typDescs := make(map[descpb.ID]catalog.TypeDescriptor)
	var tbIDs, typIDs, dbIDs, schemaIDs []descpb.ID

	// Record descriptors for name lookups.
	for i := range descs {
		switch desc := descs[i].(type) {
		case catalog.DatabaseDescriptor:
			dbNames[desc.GetID()] = desc.GetName()
			dbDescs[desc.GetID()] = desc
			if prefix == nil || prefix.GetID() == desc.GetID() {
				// Only make the database visible for iteration if the prefix was included.
				dbIDs = append(dbIDs, desc.GetID())
			}
		case catalog.TableDescriptor:
			tbDescs[desc.GetID()] = desc
			if prefix == nil || prefix.GetID() == desc.GetParentID() {
				// Only make the table visible for iteration if the prefix was included.
				tbIDs = append(tbIDs, desc.GetID())
			}
		case catalog.TypeDescriptor:
			typDescs[desc.GetID()] = desc
			if prefix == nil || prefix.GetID() == desc.GetParentID() {
				// Only make the type visible for iteration if the prefix was included.
				typIDs = append(typIDs, desc.GetID())
			}
		case catalog.SchemaDescriptor:
			schemaDescs[desc.GetID()] = desc
			if prefix == nil || prefix.GetID() == desc.GetParentID() {
				// Only make the schema visible for iteration if the prefix was included.
				schemaIDs = append(schemaIDs, desc.GetID())
				schemaNames[desc.GetID()] = desc.GetName()
			}
		}
	}

	return &internalLookupCtx{
		dbNames:     dbNames,
		dbDescs:     dbDescs,
		schemaDescs: schemaDescs,
		schemaNames: schemaNames,
		schemaIDs:   schemaIDs,
		tbDescs:     tbDescs,
		typDescs:    typDescs,
		tbIDs:       tbIDs,
		dbIDs:       dbIDs,
		typIDs:      typIDs,
	}
}

func (l *internalLookupCtx) getDatabaseByID(id descpb.ID) (catalog.DatabaseDescriptor, error) {
	db, ok := l.dbDescs[id]
	if !ok {
		return nil, sqlerrors.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", id))
	}
	return db, nil
}

func (l *internalLookupCtx) getTableByID(id descpb.ID) (catalog.TableDescriptor, error) {
	tb, ok := l.tbDescs[id]
	if !ok {
		return nil, sqlerrors.NewUndefinedRelationError(
			tree.NewUnqualifiedTableName(tree.Name(fmt.Sprintf("[%d]", id))))
	}
	return tb, nil
}

func (l *internalLookupCtx) getTypeByID(id descpb.ID) (catalog.TypeDescriptor, error) {
	typ, ok := l.typDescs[id]
	if !ok {
		return nil, sqlerrors.NewUndefinedRelationError(
			tree.NewUnqualifiedTableName(tree.Name(fmt.Sprintf("[%d]", id))))
	}
	return typ, nil
}

func (l *internalLookupCtx) getSchemaByID(id descpb.ID) (catalog.SchemaDescriptor, error) {
	sc, ok := l.schemaDescs[id]
	if !ok {
		return nil, sqlerrors.NewUndefinedSchemaError(fmt.Sprintf("[%d]", id))
	}
	return sc, nil
}

// getSchemaNameByID returns the schema name given an ID for a schema.
func (l *internalLookupCtx) getSchemaNameByID(id descpb.ID) (string, error) {
	// TODO(richardjcai): Remove this in 22.2, once it is guaranteed that
	//    public schemas are regular UDS.
	if id == keys.PublicSchemaID {
		return tree.PublicSchema, nil
	}
	schema, err := l.getSchemaByID(id)
	if err != nil {
		return "", err
	}
	return schema.GetName(), nil
}

func (l *internalLookupCtx) getDatabaseName(table catalog.Descriptor) string {
	parentName := l.dbNames[table.GetParentID()]
	if parentName == "" {
		// The parent database was deleted. This is possible e.g. when
		// a database is dropped with CASCADE, and someone queries
		// this table before the dropped table descriptors are
		// effectively deleted.
		parentName = fmt.Sprintf("[%d]", table.GetParentID())
	}
	return parentName
}

func (l *internalLookupCtx) getSchemaName(table catalog.TableDescriptor) string {
	schemaName := l.schemaNames[table.GetParentSchemaID()]
	if schemaName == "" {
		// The parent schema was deleted. This is possible e.g. when
		// a schema is dropped with CASCADE, and someone queries
		// this table before the dropped table descriptors are
		// effectively deleted.
		schemaName = fmt.Sprintf("[%d]", table.GetParentSchemaID())
	}
	return schemaName
}

// getTableNameFromTableDescriptor returns a TableName object for a given
// TableDescriptor.
func getTableNameFromTableDescriptor(
	l simpleSchemaResolver, table catalog.TableDescriptor, dbPrefix string,
) (tree.TableName, error) {
	var tableName tree.TableName
	tableDbDesc, err := l.getDatabaseByID(table.GetParentID())
	if err != nil {
		return tree.TableName{}, err
	}
	var parentSchemaName tree.Name
	// TODO(richardjcai): Remove this in 22.2.
	if table.GetParentSchemaID() == keys.PublicSchemaID {
		parentSchemaName = tree.PublicSchemaName
	} else {
		parentSchema, err := l.getSchemaByID(table.GetParentSchemaID())
		if err != nil {
			return tree.TableName{}, err
		}
		parentSchemaName = tree.Name(parentSchema.GetName())
	}
	tableName = tree.MakeTableNameWithSchema(tree.Name(tableDbDesc.GetName()),
		parentSchemaName, tree.Name(table.GetName()))
	tableName.ExplicitCatalog = tableDbDesc.GetName() != dbPrefix
	return tableName, nil
}

// getTypeNameFromTypeDescriptor returns a TypeName object for a given
// TableDescriptor.
func getTypeNameFromTypeDescriptor(
	l simpleSchemaResolver, typ catalog.TypeDescriptor,
) (tree.TypeName, error) {
	var typeName tree.TypeName
	tableDbDesc, err := l.getDatabaseByID(typ.GetParentID())
	if err != nil {
		return typeName, err
	}
	var parentSchemaName string
	// TODO(richardjcai): Remove this in 22.2.
	if typ.GetParentSchemaID() == keys.PublicSchemaID {
		parentSchemaName = tree.PublicSchema
	} else {
		parentSchema, err := l.getSchemaByID(typ.GetParentSchemaID())
		if err != nil {
			return typeName, err
		}
		parentSchemaName = parentSchema.GetName()
	}
	typeName = tree.MakeQualifiedTypeName(tableDbDesc.GetName(),
		parentSchemaName, typ.GetName())
	return typeName, nil
}

// ResolveMutableTypeDescriptor resolves a type descriptor for mutable access.
func (p *planner) ResolveMutableTypeDescriptor(
	ctx context.Context, name *tree.UnresolvedObjectName, required bool,
) (catalog.ResolvedObjectPrefix, *typedesc.Mutable, error) {
	prefix, desc, err := resolver.ResolveMutableType(ctx, p, name, required)
	if err != nil {
		return catalog.ResolvedObjectPrefix{}, nil, err
	}

	if desc != nil {
		// Ensure that the user can access the target schema.
		if err := p.canResolveDescUnderSchema(ctx, prefix.Schema, desc); err != nil {
			return catalog.ResolvedObjectPrefix{}, nil, err
		}
		tn := tree.MakeTypeNameWithPrefix(prefix.NamePrefix(), desc.GetName())
		name.SetAnnotation(&p.semaCtx.Annotations, &tn)
	}

	return prefix, desc, nil
}

// The versions below are part of the work for #34240.
// TODO(radu): clean these up when everything is switched over.

// See ResolveMutableTableDescriptor.
func (p *planner) ResolveMutableTableDescriptorEx(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType tree.RequiredTableKind,
) (catalog.ResolvedObjectPrefix, *tabledesc.Mutable, error) {
	tn := name.ToTableName()
	prefix, table, err := resolver.ResolveMutableExistingTableObject(ctx, p, &tn, required, requiredType)
	if err != nil {
		return catalog.ResolvedObjectPrefix{}, nil, err
	}
	name.SetAnnotation(&p.semaCtx.Annotations, &tn)

	if table != nil {
		// Ensure that the user can access the target schema.
		if err := p.canResolveDescUnderSchema(ctx, prefix.Schema, table); err != nil {
			return catalog.ResolvedObjectPrefix{}, nil, err
		}
	}

	return prefix, table, nil
}

// ResolveMutableTableDescriptorExAllowNoPrimaryKey performs the
// same logic as ResolveMutableTableDescriptorEx but allows for
// the resolved table to not have a primary key.
func (p *planner) ResolveMutableTableDescriptorExAllowNoPrimaryKey(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType tree.RequiredTableKind,
) (catalog.ResolvedObjectPrefix, *tabledesc.Mutable, error) {
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags:      tree.CommonLookupFlags{Required: required, RequireMutable: true},
		AllowWithoutPrimaryKey: true,
		DesiredObjectKind:      tree.TableObject,
		DesiredTableDescKind:   requiredType,
	}
	desc, prefix, err := resolver.ResolveExistingObject(ctx, p, name, lookupFlags)
	if err != nil || desc == nil {
		return catalog.ResolvedObjectPrefix{}, nil, err
	}
	tn := tree.MakeTableNameFromPrefix(prefix.NamePrefix(), tree.Name(name.Object()))
	name.SetAnnotation(&p.semaCtx.Annotations, &tn)
	table := desc.(*tabledesc.Mutable)

	// Ensure that the user can access the target schema.
	if err := p.canResolveDescUnderSchema(ctx, prefix.Schema, table); err != nil {
		return catalog.ResolvedObjectPrefix{}, nil, err
	}

	return prefix, table, err
}

// See ResolveUncachedTableDescriptor.
func (p *planner) ResolveUncachedTableDescriptorEx(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType tree.RequiredTableKind,
) (table catalog.TableDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		table, err = p.ResolveExistingObjectEx(ctx, name, required, requiredType)
	})
	return table, err
}

// See ResolveExistingTableObject.
func (p *planner) ResolveExistingObjectEx(
	ctx context.Context,
	name *tree.UnresolvedObjectName,
	required bool,
	requiredType tree.RequiredTableKind,
) (res catalog.TableDescriptor, err error) {
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags:    p.CommonLookupFlags(required),
		DesiredObjectKind:    tree.TableObject,
		DesiredTableDescKind: requiredType,
	}
	desc, prefix, err := resolver.ResolveExistingObject(ctx, p, name, lookupFlags)
	if err != nil || desc == nil {
		return nil, err
	}
	tn := tree.MakeTableNameFromPrefix(prefix.NamePrefix(), tree.Name(name.Object()))
	name.SetAnnotation(&p.semaCtx.Annotations, &tn)
	table := desc.(catalog.TableDescriptor)

	// Ensure that the user can access the target schema.
	if err := p.canResolveDescUnderSchema(ctx, prefix.Schema, table); err != nil {
		return nil, err
	}

	return table, nil
}

// ResolvedName is a convenience wrapper for UnresolvedObjectName.Resolved.
func (p *planner) ResolvedName(u *tree.UnresolvedObjectName) tree.ObjectName {
	return u.Resolved(&p.semaCtx.Annotations)
}

type simpleSchemaResolver interface {
	getDatabaseByID(id descpb.ID) (catalog.DatabaseDescriptor, error)
	getSchemaByID(id descpb.ID) (catalog.SchemaDescriptor, error)
	getTableByID(id descpb.ID) (catalog.TableDescriptor, error)
}
