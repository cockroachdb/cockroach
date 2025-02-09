// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

var _ resolver.SchemaResolver = &schemaResolver{}

// schemaResolver implements the resolver.SchemaResolver interface.
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

// GetObjectNamesAndIDs implements the resolver.SchemaResolver interface.
func (sr *schemaResolver) GetObjectNamesAndIDs(
	ctx context.Context, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor,
) (objectNames tree.TableNames, objectIDs descpb.IDs, _ error) {
	c, err := sr.descCollection.GetAllObjectsInSchema(ctx, sr.txn, db, sc)
	if err != nil {
		return nil, nil, err
	}
	var mc nstree.MutableCatalog
	_ = c.ForEachDescriptor(func(desc catalog.Descriptor) error {
		if !desc.SkipNamespace() && !desc.Dropped() {
			mc.UpsertNamespaceEntry(desc, desc.GetID(), hlc.Timestamp{})
		}
		return nil
	})
	_ = mc.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
		tn := tree.MakeTableNameWithSchema(
			tree.Name(db.GetName()), tree.Name(sc.GetName()), tree.Name(e.GetName()),
		)
		objectNames = append(objectNames, tn)
		objectIDs = append(objectIDs, e.GetID())
		return nil
	})
	return objectNames, objectIDs, nil
}

// MustGetCurrentSessionDatabase implements the resolver.SchemaResolver interface.
func (sr *schemaResolver) MustGetCurrentSessionDatabase(
	ctx context.Context,
) (catalog.DatabaseDescriptor, error) {
	if sr.skipDescriptorCache {
		return sr.descCollection.ByName(sr.txn).Get().Database(ctx, sr.CurrentDatabase())
	}
	return sr.descCollection.ByNameWithLeased(sr.txn).Get().Database(ctx, sr.CurrentDatabase())
}

// CurrentSearchPath implements the resolver.SchemaResolver interface.
func (sr *schemaResolver) CurrentSearchPath() sessiondata.SearchPath {
	return sr.sessionDataStack.Top().SearchPath
}

func (sr *schemaResolver) byIDGetterBuilder() descs.ByIDGetterBuilder {
	if sr.skipDescriptorCache {
		return sr.descCollection.ByIDWithoutLeased(sr.txn)
	}
	return sr.descCollection.ByIDWithLeased(sr.txn)
}

func (sr *schemaResolver) byNameGetterBuilder() descs.ByNameGetterBuilder {
	if sr.skipDescriptorCache {
		return sr.descCollection.ByName(sr.txn)
	}
	return sr.descCollection.ByNameWithLeased(sr.txn)
}

// LookupObject implements the tree.ObjectNameExistingResolver interface.
func (sr *schemaResolver) LookupObject(
	ctx context.Context, flags tree.ObjectLookupFlags, dbName, scName, obName string,
) (found bool, prefix catalog.ResolvedObjectPrefix, desc catalog.Descriptor, err error) {
	// Check if we are looking up a type which matches a built-in type in
	// CockroachDB but is an extension type on the public schema in PostgreSQL.
	if flags.DesiredObjectKind == tree.TypeObject && scName == catconstants.PublicSchemaName {
		if alias, ok := types.PublicSchemaAliases[obName]; ok {
			if flags.RequireMutable {
				return true, catalog.ResolvedObjectPrefix{}, nil, pgerror.Newf(pgcode.WrongObjectType, "type %q is a built-in type", obName)
			}

			found, prefix, err = sr.LookupSchema(ctx, dbName, scName)
			if err != nil || !found {
				return found, prefix, nil, err
			}
			dbDesc, err := sr.byNameGetterBuilder().MaybeGet().Database(ctx, dbName)
			if err != nil {
				return found, prefix, nil, err
			}
			if dbDesc.HasPublicSchemaWithDescriptor() {
				publicSchemaID := dbDesc.GetSchemaID(catconstants.PublicSchemaName)
				return true, prefix, typedesc.MakeSimpleAlias(alias, publicSchemaID), nil
			}
			return true, prefix, typedesc.MakeSimpleAlias(alias, keys.PublicSchemaID), nil
		}
	}

	b := sr.descCollection.ByName(sr.txn)
	if !sr.skipDescriptorCache && !flags.RequireMutable {
		// The caller requires this descriptor to *not* be leased,
		// so lets assert this here. Normally the planner / resolver
		// will propagate flags so we don' need to check on a
		// look up level.
		if flags.AssertNotLeased {
			return false, prefix, nil,
				errors.AssertionFailedf("unable to get leased descriptor for (%q), resolver was not configured properly", obName)
		}
		b = sr.descCollection.ByNameWithLeased(sr.txn)
	}
	if flags.IncludeOffline {
		b = b.WithOffline()
	}
	g := b.MaybeGet()
	switch flags.DesiredObjectKind {
	case tree.TableObject:
		tn := tree.NewTableNameWithSchema(tree.Name(dbName), tree.Name(scName), tree.Name(obName))
		prefix, desc, err = descs.PrefixAndTable(ctx, g, tn)
	case tree.TypeObject:
		tn := tree.NewQualifiedTypeName(dbName, scName, obName)
		prefix, desc, err = descs.PrefixAndType(ctx, g, tn)
	case tree.AnyObject:
		tn := tree.NewTableNameWithSchema(tree.Name(dbName), tree.Name(scName), tree.Name(obName))
		prefix, desc, err = descs.PrefixAndTable(ctx, g, tn)
		if err != nil {
			if sqlerrors.IsUndefinedRelationError(err) || errors.Is(err, catalog.ErrDescriptorWrongType) {
				tn := tree.NewQualifiedTypeName(dbName, scName, obName)
				prefix, desc, err = descs.PrefixAndType(ctx, g, tn)
			}
		}
	default:
		return false, prefix, nil, errors.AssertionFailedf(
			"unknown desired object kind %v", flags.DesiredObjectKind,
		)
	}
	if errors.Is(err, catalog.ErrDescriptorWrongType) {
		return false, prefix, nil, nil
	}
	if flags.RequireMutable && desc != nil {
		switch flags.DesiredObjectKind {
		case tree.TableObject:
			desc, err = sr.descCollection.MutableByID(sr.txn).Table(ctx, desc.GetID())
		case tree.TypeObject:
			desc, err = sr.descCollection.MutableByID(sr.txn).Type(ctx, desc.GetID())
		case tree.AnyObject:
			desc, err = sr.descCollection.MutableByID(sr.txn).Desc(ctx, desc.GetID())
		}
	}
	return desc != nil, prefix, desc, err
}

// LookupSchema implements the resolver.ObjectNameTargetResolver interface.
func (sr *schemaResolver) LookupSchema(
	ctx context.Context, dbName, scName string,
) (found bool, scMeta catalog.ResolvedObjectPrefix, err error) {
	g := sr.byNameGetterBuilder().MaybeGet()
	db, err := g.Database(ctx, dbName)
	if err != nil || db == nil {
		return false, catalog.ResolvedObjectPrefix{}, err
	}
	sc, err := g.Schema(ctx, db, scName)
	if err != nil || sc == nil {
		return false, catalog.ResolvedObjectPrefix{}, err
	}
	return true, catalog.ResolvedObjectPrefix{Database: db, Schema: sc}, nil
}

func (sr *schemaResolver) LookupDatabase(ctx context.Context, dbName string) error {
	g := sr.byNameGetterBuilder().Get()
	_, err := g.Database(ctx, dbName)
	if err != nil {
		return err
	}
	return nil
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
	table, err := sr.descCollection.ByIDWithLeased(sr.txn).WithoutNonPublic().Get().Table(ctx, descpb.ID(id))
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
	// When getting the fully qualified name allow use of leased descriptors,
	// since these will not involve any round trip.
	descGetter := sr.descCollection.ByIDWithLeased(sr.txn)
	dbDesc, err := descGetter.Get().Database(ctx, desc.GetParentID())
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
	scDesc, err := descGetter.Get().Schema(ctx, schemaID)
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

// GetQualifiedFunctionNameByID returns the qualified name of the table,
// view or sequence represented by the provided ID and table kind.
func (sr *schemaResolver) GetQualifiedFunctionNameByID(
	ctx context.Context, id int64,
) (*tree.RoutineName, error) {
	fn, err := sr.descCollection.ByIDWithLeased(sr.txn).WithoutNonPublic().Get().Function(ctx, descpb.ID(id))
	if err != nil {
		return nil, err
	}
	return sr.getQualifiedFunctionName(ctx, fn)
}

func (sr *schemaResolver) getQualifiedFunctionName(
	ctx context.Context, fnDesc catalog.FunctionDescriptor,
) (*tree.RoutineName, error) {
	dbDesc, err := sr.descCollection.ByIDWithLeased(sr.txn).Get().Database(ctx, fnDesc.GetParentID())
	if err != nil {
		return nil, err
	}
	scDesc, err := sr.descCollection.ByIDWithLeased(sr.txn).Get().Schema(ctx, fnDesc.GetParentSchemaID())
	if err != nil {
		return nil, err
	}

	fnName := tree.MakeQualifiedRoutineName(dbDesc.GetName(), scDesc.GetName(), fnDesc.GetName())
	return &fnName, nil
}

// ResolveType implements the tree.TypeReferenceResolver interface.
func (sr *schemaResolver) ResolveType(
	ctx context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	lookupFlags := tree.ObjectLookupFlags{
		Required:          true,
		RequireMutable:    false,
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

	// Disallow cross-database type resolution. Note that we check
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

	return typedesc.HydratedTFromDesc(ctx, &tn, tdesc, sr)
}

// ResolveTypeByOID implements the tree.TypeReferenceResolver interface.
// Note: Type resolution only works for OIDs of user-defined types. Builtin
// types do not need to be hydrated.
func (sr *schemaResolver) ResolveTypeByOID(ctx context.Context, oid oid.Oid) (*types.T, error) {
	return typedesc.ResolveHydratedTByOID(ctx, oid, sr)
}

// GetTypeDescriptor implements the catalog.TypeDescriptorResolver interface.
func (sr *schemaResolver) GetTypeDescriptor(
	ctx context.Context, id descpb.ID,
) (tree.TypeName, catalog.TypeDescriptor, error) {
	g := sr.byIDGetterBuilder().WithoutNonPublic().WithoutOtherParent(sr.typeResolutionDbID).Get()
	desc, err := g.Type(ctx, id)
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	dbName := sr.CurrentDatabase()
	if !descpb.IsVirtualTable(desc.GetID()) {
		db, err := g.Database(ctx, desc.GetParentID())
		if err != nil {
			return tree.TypeName{}, nil, err
		}
		dbName = db.GetName()
	}
	sc, err := g.Schema(ctx, desc.GetParentSchemaID())
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	name := tree.MakeQualifiedTypeName(dbName, sc.GetName(), desc.GetName())
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
		forLog := kind // prevents kind from escaping
		panic(errors.AssertionFailedf("unknown schema kind %d", forLog))
	}
}

// runWithOptions sets the provided resolution flags for the
// duration of the call of the passed argument fn.
//
// This is meant to be used like this (for example):
//
// var someVar T
// var err error
//
//	p.runWithOptions(resolveFlags{skipCache: true}, func() {
//	   someVar, err = ResolveExistingTableObject(ctx, p, ...)
//	})
//
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
	ctx context.Context, name tree.UnresolvedRoutineName, path tree.SearchPath,
) (*tree.ResolvedFunctionDefinition, error) {
	uname := name.UnresolvedName()
	if uname.NumParts > 3 || len(uname.Parts[0]) == 0 || uname.Star {
		return nil, pgerror.Newf(pgcode.InvalidName, "invalid function name: %s", name)
	}

	fn, err := uname.ToRoutineName()
	if err != nil {
		return nil, err
	}

	if fn.ExplicitCatalog && fn.Catalog() != sr.CurrentDatabase() {
		return nil, pgerror.New(pgcode.FeatureNotSupported, "cross-database function references not allowed")
	}

	// Get builtin and udf functions if there is any match.
	builtinDef, err := tree.GetBuiltinFuncDefinition(fn, path)
	if err != nil {
		return nil, err
	}
	routine, err := maybeLookupRoutine(ctx, sr, path, fn)
	if err != nil {
		return nil, err
	}

	switch {
	case builtinDef != nil && routine != nil:
		return builtinDef.MergeWith(routine, path)
	case builtinDef != nil:
		if builtinDef.UnsupportedWithIssue != 0 {
			return nil, builtinDef.MakeUnsupportedError()
		}
		return builtinDef, nil
	case routine != nil:
		return routine, nil
	default:
		return nil, makeFunctionUndefinedError(ctx, name, path, fn, sr)
	}
}

// If nothing found, there is a chance that user typed in a quoted function
// name which is not lowercase. So here we try to lowercase the given
// function name and find a suggested function name if possible.
func makeFunctionUndefinedError(
	ctx context.Context,
	name tree.UnresolvedRoutineName,
	path tree.SearchPath,
	fn tree.RoutineName,
	sr *schemaResolver,
) error {
	uname := name.UnresolvedName()
	makeRoutineName := func(n *tree.UnresolvedName) (tree.UnresolvedRoutineName, error) {
		switch name.(type) {
		case tree.UnresolvedFunctionName:
			return tree.MakeUnresolvedFunctionName(n), nil
		case tree.UnresolvedProcedureName:
			return tree.MakeUnresolvedProcedureName(n), nil
		default:
			return nil, errors.AssertionFailedf("unexpected routine name type")
		}
	}
	var lowerName tree.UnresolvedName
	if fn.ExplicitSchema {
		lowerName = tree.MakeUnresolvedName(strings.ToLower(uname.Parts[1]), strings.ToLower(uname.Parts[0]))
	} else {
		lowerName = tree.MakeUnresolvedName(strings.ToLower(uname.Parts[0]))
	}
	lowerRoutineName, err := makeRoutineName(&lowerName)
	if err != nil {
		return err
	}
	wrap := func(err error) error { return err }
	if lowerName != *uname {
		alternative, err := sr.ResolveFunction(ctx, lowerRoutineName, path)
		if err != nil {
			switch pgerror.GetPGCode(err) {
			case pgcode.UndefinedFunction, pgcode.UndefinedSchema:
				// Lower-case function does not exist.
			default:
				return errors.Wrapf(err,
					"failed to look up alternative name for %v which does not exist", &fn,
				)
			}
		} else if alternative != nil {
			wrap = func(err error) error {
				return errors.WithHintf(
					err, "lower-case alternative %s exists", &lowerName,
				)
			}
		}
	}
	if _, ok := name.(tree.UnresolvedProcedureName); ok {
		err = errors.Mark(
			pgerror.Newf(pgcode.UndefinedFunction, "unknown procedure: %s()", tree.ErrString(uname)),
			tree.ErrRoutineUndefined,
		)
	} else {
		err = errors.Mark(
			pgerror.Newf(pgcode.UndefinedFunction, "unknown function: %s()", tree.ErrString(uname)),
			tree.ErrRoutineUndefined,
		)
	}
	return wrap(err)
}

func maybeLookupRoutine(
	ctx context.Context, sr *schemaResolver, path tree.SearchPath, fn tree.RoutineName,
) (*tree.ResolvedFunctionDefinition, error) {
	if sr.txn == nil {
		return nil, nil
	}

	db := sr.CurrentDatabase()
	if db == "" {
		// The database is empty for queries run in the internal executor. None
		// of the lookups below will succeed, so we can return early.
		return nil, nil
	}

	if fn.ExplicitSchema && fn.Schema() != catconstants.CRDBInternalSchemaName {
		found, prefix, err := sr.LookupSchema(ctx, db, fn.Schema())
		if err != nil {
			return nil, err
		}

		if !found {
			return nil, pgerror.Newf(pgcode.UndefinedSchema, "schema %q does not exist", fn.Schema())
		}

		udfDef, _ := prefix.Schema.GetResolvedFuncDefinition(ctx, fn.Object())
		return udfDef, nil
	}

	var udfDef *tree.ResolvedFunctionDefinition
	for i, n := 0, path.NumElements(); i < n; i++ {
		schema := path.GetSchema(i)
		found, prefix, err := sr.LookupSchema(ctx, db, schema)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		curUdfDef, found := prefix.Schema.GetResolvedFuncDefinition(ctx, fn.Object())
		if !found {
			continue
		}
		udfDef, err = udfDef.MergeWith(curUdfDef, path)
		if err != nil {
			return nil, err
		}
	}
	return udfDef, nil
}

func (sr *schemaResolver) ResolveFunctionByOID(
	ctx context.Context, oid oid.Oid,
) (name *tree.RoutineName, fn *tree.Overload, err error) {
	if !funcdesc.IsOIDUserDefinedFunc(oid) {
		qol, ok := tree.OidToQualifiedBuiltinOverload[oid]
		if !ok {
			return nil, nil, errors.Mark(
				pgerror.Newf(pgcode.UndefinedFunction, "function %d not found", oid),
				tree.ErrRoutineUndefined,
			)
		}
		fnName := tree.MakeQualifiedRoutineName(sr.CurrentDatabase(), qol.Schema, tree.OidToBuiltinName[oid])
		return &fnName, qol.Overload, nil
	}

	g := sr.byIDGetterBuilder().WithoutNonPublic().WithoutOtherParent(sr.typeResolutionDbID).Get()
	descID := funcdesc.UserDefinedFunctionOIDToID(oid)
	funcDesc, err := g.Function(ctx, descID)
	if err != nil {
		return nil, nil, err
	}
	ret, err := funcDesc.ToOverload()
	if err != nil {
		return nil, nil, err
	}
	fnName, err := sr.getQualifiedFunctionName(ctx, funcDesc)
	if err != nil {
		return nil, nil, err
	}
	return fnName, ret, nil
}

// FunctionDesc returns the descriptor for the function with the given OID.
func (sr *schemaResolver) FunctionDesc(
	ctx context.Context, oid oid.Oid,
) (catalog.FunctionDescriptor, error) {
	if !funcdesc.IsOIDUserDefinedFunc(oid) {
		return nil, errors.Mark(
			pgerror.Newf(pgcode.UndefinedFunction, "function %d  not user-defined", oid),
			tree.ErrRoutineUndefined,
		)
	}
	g := sr.byIDGetterBuilder().WithoutNonPublic().WithoutOtherParent(sr.typeResolutionDbID).Get()
	descID := funcdesc.UserDefinedFunctionOIDToID(oid)
	return g.Function(ctx, descID)
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
