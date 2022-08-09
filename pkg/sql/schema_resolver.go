// Copyright 2022 The Cockroach Authors.
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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

var _ resolver.SchemaResolver = &schemaResolver{}

// schemaResolve implements the resolver.SchemaResolver interface.
// Currently, this is only being embedded in the planner but also a convenience
// for inejcting it into the declarative schema changer.
// It holds sessionDataStack and a transaction handle which are reset when
// planner is reset.
// TODO (Chengxiong) refactor this out into a separate package.
type schemaResolver struct {
	descCollection   *descs.Collection
	sessionDataStack *sessiondata.Stack
	txn              *kv.Txn
	authAccessor     scbuild.AuthorizationAccessor

	// skipDescriptorCache, when true, instructs all code that accesses table/view
	// descriptors to force reading the descriptors within the transaction. This
	// is necessary to read descriptors from the store for:
	//   1. Descriptors that are part of a schema change but are not modified by the
	//      schema change. (reading a table in CREATE VIEW)
	//   2. Disable the use of the table cache in tests.
	skipDescriptorCache bool

	// typeResolutionDbID is the ID of a database. If set, type resolution steps
	// will disallow resolution of types that have a parentID != typeResolutionDbID
	// when it is set.
	typeResolutionDbID descpb.ID
}

// Accessor implements the resolver.SchemaResolver interface.
func (sr *schemaResolver) Accessor() catalog.Accessor {
	return sr.descCollection
}

// CurrentSearchPath implements the resolver.SchemaResolver interface.
func (sr *schemaResolver) CurrentSearchPath() sessiondata.SearchPath {
	return sr.sessionDataStack.Top().SearchPath
}

// CommonLookupFlags implements the resolver.SchemaResolver interface.
func (sr *schemaResolver) CommonLookupFlags(required bool) tree.CommonLookupFlags {
	return tree.CommonLookupFlags{
		Required:    required,
		AvoidLeased: sr.skipDescriptorCache,
	}
}

// LookupObject implements the tree.ObjectNameExistingResolver interface.
func (sr *schemaResolver) LookupObject(
	ctx context.Context, flags tree.ObjectLookupFlags, dbName, scName, obName string,
) (found bool, prefix catalog.ResolvedObjectPrefix, objMeta catalog.Descriptor, err error) {
	sc := sr.Accessor()
	flags.CommonLookupFlags.Required = false
	flags.CommonLookupFlags.AvoidLeased = sr.skipDescriptorCache

	// Check if we are looking up a type which matches a built-in type in
	// CockroachDB but is an extension type on the public schema in PostgreSQL.
	if flags.DesiredObjectKind == tree.TypeObject && scName == tree.PublicSchema {
		if alias, ok := types.PublicSchemaAliases[obName]; ok {
			if flags.RequireMutable {
				return true, catalog.ResolvedObjectPrefix{}, nil, pgerror.Newf(pgcode.WrongObjectType, "type %q is a built-in type", obName)
			}

			found, prefix, err = sr.LookupSchema(ctx, dbName, scName)
			if err != nil || !found {
				return found, prefix, nil, err
			}
			dbDesc, err := sr.descCollection.GetImmutableDatabaseByName(ctx, sr.txn, dbName,
				tree.DatabaseLookupFlags{AvoidLeased: sr.skipDescriptorCache})
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

	prefix, objMeta, err = sc.GetObjectDesc(ctx, sr.txn, dbName, scName, obName, flags)
	return objMeta != nil, prefix, objMeta, err
}

// LookupSchema implements the resolver.ObjectNameTargetResolver interface.
func (sr *schemaResolver) LookupSchema(
	ctx context.Context, dbName, scName string,
) (found bool, scMeta catalog.ResolvedObjectPrefix, err error) {
	dbDesc, err := sr.descCollection.GetImmutableDatabaseByName(ctx, sr.txn, dbName,
		tree.DatabaseLookupFlags{AvoidLeased: sr.skipDescriptorCache})
	if err != nil || dbDesc == nil {
		return false, catalog.ResolvedObjectPrefix{}, err
	}
	sc := sr.Accessor()
	var resolvedSchema catalog.SchemaDescriptor
	resolvedSchema, err = sc.GetSchemaByName(
		ctx, sr.txn, dbDesc, scName, sr.CommonLookupFlags(false /* required */),
	)
	if err != nil || resolvedSchema == nil {
		return false, catalog.ResolvedObjectPrefix{}, err
	}
	return true, catalog.ResolvedObjectPrefix{
		Database: dbDesc,
		Schema:   resolvedSchema,
	}, nil
}

// CurrentDatabase implements the tree.QualifiedNameResolver interface.
func (sr *schemaResolver) CurrentDatabase() string {
	return sr.sessionDataStack.Top().Database
}

// GetQualifiedTableNameByID returns the qualified name of the table,
// view or sequence represented by the provided ID and table kind.
func (sr *schemaResolver) GetQualifiedTableNameByID(
	ctx context.Context, id int64, requiredType tree.RequiredTableKind,
) (*tree.TableName, error) {
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags:    tree.CommonLookupFlags{Required: true},
		DesiredObjectKind:    tree.TableObject,
		DesiredTableDescKind: requiredType,
	}

	table, err := sr.descCollection.GetImmutableTableByID(
		ctx, sr.txn, descpb.ID(id), lookupFlags)
	if err != nil {
		return nil, err
	}
	return sr.getQualifiedTableName(ctx, table)
}

// getQualifiedTableName returns the database-qualified name of the table
// or view represented by the provided descriptor. It is a sort of
// reverse of the Resolve() functions.
func (sr *schemaResolver) getQualifiedTableName(
	ctx context.Context, desc catalog.TableDescriptor,
) (*tree.TableName, error) {
	_, dbDesc, err := sr.descCollection.GetImmutableDatabaseByID(ctx, sr.txn, desc.GetParentID(),
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
	scDesc, err := sr.descCollection.GetImmutableSchemaByID(ctx, sr.txn, schemaID,
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

// ResolveType implements the tree.TypeReferenceResolver interface.
func (sr *schemaResolver) ResolveType(
	ctx context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	lookupFlags := tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{Required: true, RequireMutable: false},
		DesiredObjectKind: tree.TypeObject,
	}
	desc, prefix, err := resolver.ResolveExistingObject(ctx, sr, name, lookupFlags)
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
	// typeResolutionDbID != descpb.InvalidID when we have been restricted to
	// accessing types in the database with ID = typeResolutionDbID by
	// p.runWithOptions. So, check to see if the resolved descriptor's parentID
	// matches, unless the descriptor's parentID is invalid. This could happen
	// when the type being resolved is a builtin type prefaced with a virtual
	// schema like `pg_catalog.int`. Resolution for these types returns a dummy
	// TypeDescriptor, so ignore those cases.
	if sr.typeResolutionDbID != descpb.InvalidID && tdesc.GetParentID() != descpb.InvalidID && tdesc.GetParentID() != sr.typeResolutionDbID {
		return nil, pgerror.Newf(
			pgcode.FeatureNotSupported, "cross database type references are not supported: %s", tn.String())
	}

	// Ensure that the user can access the target schema.
	if err := sr.canResolveDescUnderSchema(ctx, prefix.Schema, tdesc); err != nil {
		return nil, err
	}

	return tdesc.MakeTypesT(ctx, &tn, sr)
}

// ResolveTypeByOID implements the tree.TypeReferenceResolver interface.
func (sr *schemaResolver) ResolveTypeByOID(ctx context.Context, oid oid.Oid) (*types.T, error) {
	id, err := typedesc.UserDefinedTypeOIDToID(oid)
	if err != nil {
		return nil, err
	}
	name, desc, err := sr.GetTypeDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	return desc.MakeTypesT(ctx, &name, sr)
}

// GetTypeDescriptor implements the catalog.TypeDescriptorResolver interface.
func (sr *schemaResolver) GetTypeDescriptor(
	ctx context.Context, id descpb.ID,
) (tree.TypeName, catalog.TypeDescriptor, error) {
	desc, err := sr.descCollection.GetImmutableTypeByID(ctx, sr.txn, id, tree.ObjectLookupFlags{})
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	// Note that the value of required doesn't matter for lookups by ID.
	_, dbDesc, err := sr.descCollection.GetImmutableDatabaseByID(ctx, sr.txn, desc.GetParentID(), sr.CommonLookupFlags(true /* required */))
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	sc, err := sr.descCollection.GetImmutableSchemaByID(
		ctx, sr.txn, desc.GetParentSchemaID(), tree.SchemaLookupFlags{Required: true})
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	name := tree.MakeQualifiedTypeName(dbDesc.GetName(), sc.GetName(), desc.GetName())
	return name, desc, nil
}

func (sr *schemaResolver) canResolveDescUnderSchema(
	ctx context.Context, scDesc catalog.SchemaDescriptor, desc catalog.Descriptor,
) error {
	// We can't always resolve temporary schemas by ID (for example in the temporary
	// object cleaner which accesses temporary schemas not in the current session).
	// To avoid an internal error, we just don't check usage on temporary tables.
	if tbl, ok := desc.(catalog.TableDescriptor); ok && tbl.IsTemporary() {
		return nil
	}

	switch kind := scDesc.SchemaKind(); kind {
	case catalog.SchemaPublic, catalog.SchemaTemporary, catalog.SchemaVirtual:
		// Anyone can resolve under temporary, public or virtual schemas.
		return nil
	case catalog.SchemaUserDefined:
		return sr.authAccessor.CheckPrivilegeForUser(ctx, scDesc, privilege.USAGE, sr.sessionDataStack.Top().User())
	default:
		panic(errors.AssertionFailedf("unknown schema kind %d", kind))
	}
}

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
func (sr *schemaResolver) runWithOptions(flags resolveFlags, fn func()) {
	if flags.skipCache {
		defer func(prev bool) { sr.skipDescriptorCache = prev }(sr.skipDescriptorCache)
		sr.skipDescriptorCache = true
	}
	if flags.contextDatabaseID != descpb.InvalidID {
		defer func(prev descpb.ID) { sr.typeResolutionDbID = prev }(sr.typeResolutionDbID)
		sr.typeResolutionDbID = flags.contextDatabaseID
	}
	fn()
}

func (sr *schemaResolver) ResolveFunction(
	ctx context.Context, name *tree.UnresolvedName, path tree.SearchPath,
) (*tree.ResolvedFunctionDefinition, error) {
	if name.NumParts > 3 || len(name.Parts[0]) == 0 || name.Star {
		return nil, pgerror.Newf(pgcode.InvalidName, "invalid function name: %s", name)
	}

	fn, err := name.ToFunctionName()
	if err != nil {
		return nil, err
	}

	if fn.ExplicitCatalog && fn.Catalog() != sr.CurrentDatabase() {
		return nil, pgerror.New(pgcode.FeatureNotSupported, "cross-database function references not allowed")
	}

	// Get builtin functions if there is any match.
	builtinDef, err := tree.GetBuiltinFuncDefinition(fn, path)
	if err != nil {
		return nil, err
	}

	var udfDef *tree.ResolvedFunctionDefinition
	if fn.ExplicitSchema && fn.Schema() != catconstants.CRDBInternalSchemaName {
		found, prefix, err := sr.LookupSchema(ctx, sr.CurrentDatabase(), fn.Schema())
		if err != nil {
			return nil, err
		}

		if !found {
			return nil, pgerror.Newf(pgcode.UndefinedSchema, "schema %q does not exist", fn.Schema())
		}

		sc := prefix.Schema
		udfDef, _ = sc.GetResolvedFuncDefinition(fn.Object())
	} else {
		if err := path.IterateSearchPath(func(schema string) error {
			found, prefix, err := sr.LookupSchema(ctx, sr.CurrentDatabase(), schema)
			if err != nil {
				return err
			}
			if !found {
				return nil
			}
			curUdfDef, found := prefix.Schema.GetResolvedFuncDefinition(fn.Object())
			if !found {
				return nil
			}

			udfDef, err = udfDef.MergeWith(curUdfDef)
			return err
		}); err != nil {
			return nil, err
		}
	}

	if builtinDef == nil && udfDef == nil {
		// If nothing found, there is a chance that user typed in a quoted function
		// name which is not lowercase. So here we try to lowercase the given
		// function name and find a suggested function name if possible.
		extraMsg := ""
		lowerName := tree.MakeUnresolvedName(strings.ToLower(name.Parts[0]))
		if lowerName != *name {
			alternative, err := sr.ResolveFunction(ctx, &lowerName, path)
			if err == nil && alternative != nil {
				extraMsg = fmt.Sprintf(", but %s() exists", alternative.Name)
			}
		}
		return nil, pgerror.Newf(pgcode.UndefinedFunction, "unknown function: %s()%s", tree.ErrString(name), extraMsg)
	}
	if builtinDef == nil {
		return udfDef, nil
	}
	if udfDef == nil {
		props, _ := builtinsregistry.GetBuiltinProperties(builtinDef.Name)
		if props.UnsupportedWithIssue != 0 {
			// Note: no need to embed the function name in the message; the
			// caller will add the function name as prefix.
			const msg = "this function is not yet supported"
			var unImplErr error
			if props.UnsupportedWithIssue < 0 {
				unImplErr = unimplemented.New(builtinDef.Name+"()", msg)
			} else {
				unImplErr = unimplemented.NewWithIssueDetail(props.UnsupportedWithIssue, builtinDef.Name, msg)
			}
			return nil, pgerror.Wrapf(unImplErr, pgcode.InvalidParameterValue, "%s()", builtinDef.Name)
		}
		return builtinDef, nil
	}

	return builtinDef.MergeWith(udfDef)
}

func (sr *schemaResolver) ResolveFunctionByOID(
	ctx context.Context, oid oid.Oid,
) (name string, fn *tree.Overload, err error) {
	if !funcdesc.IsOIDUserDefinedFunc(oid) {
		name, ok := tree.OidToBuiltinName[oid]
		if !ok {
			return "", nil, pgerror.Newf(pgcode.UndefinedFunction, "function %d not found", oid)
		}
		funcDef := tree.FunDefs[name]
		for _, o := range funcDef.Definition {
			if o.Oid == oid {
				return funcDef.Name, o, nil
			}
		}
	}

	flags := tree.ObjectLookupFlagsWithRequired()
	flags.AvoidLeased = sr.skipDescriptorCache
	descID, err := funcdesc.UserDefinedFunctionOIDToID(oid)
	if err != nil {
		return "", nil, err
	}
	funcDesc, err := sr.descCollection.GetImmutableFunctionByID(ctx, sr.txn, descID, flags)
	if err != nil {
		return "", nil, err
	}
	ret, err := funcDesc.ToOverload()
	if err != nil {
		return "", nil, err
	}
	return funcDesc.GetName(), ret, nil
}

// NewSkippingCacheSchemaResolver constructs a schemaResolver which always skip
// descriptor cache.
func NewSkippingCacheSchemaResolver(
	descCollection *descs.Collection,
	sessionDataStack *sessiondata.Stack,
	txn *kv.Txn,
	authAccessor scbuild.AuthorizationAccessor,
) resolver.SchemaResolver {
	return &schemaResolver{
		descCollection:      descCollection,
		sessionDataStack:    sessionDataStack,
		txn:                 txn,
		authAccessor:        authAccessor,
		skipDescriptorCache: true,
	}
}
