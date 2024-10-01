// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package resolver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// SchemaResolver abstracts the interfaces needed from the logical
// planner to perform name resolution below.
//
// We use an interface instead of passing *planner directly to make
// the resolution methods able to work even when we evolve the code to
// use a different plan builder.
// TODO(rytaft,andyk): study and reuse this.
type SchemaResolver interface {
	ObjectNameExistingResolver
	ObjectNameTargetResolver
	tree.QualifiedNameResolver
	tree.TypeReferenceResolver
	tree.FunctionReferenceResolver

	// GetObjectNamesAndIDs returns the names and IDs of the objects in the
	// schema.
	GetObjectNamesAndIDs(
		ctx context.Context, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor,
	) (tree.TableNames, descpb.IDs, error)

	// MustGetCurrentSessionDatabase returns the database descriptor for the
	// current session database.
	MustGetCurrentSessionDatabase(ctx context.Context) (catalog.DatabaseDescriptor, error)

	// CurrentSearchPath returns the current search path.
	CurrentSearchPath() sessiondata.SearchPath
}

// ObjectNameExistingResolver is the helper interface to resolve table
// names when the object is expected to exist already. The boolean passed
// is used to specify if a MutableTableDescriptor is to be returned in the
// result. ResolvedObjectPrefix should always be populated by implementors
// to allows us to generate errors at higher level layers, since it allows
// us to know if the schema and database were found.
type ObjectNameExistingResolver interface {
	LookupObject(
		ctx context.Context, flags tree.ObjectLookupFlags,
		dbName, scName, obName string,
	) (
		found bool,
		prefix catalog.ResolvedObjectPrefix,
		objMeta catalog.Descriptor,
		err error,
	)
}

// ObjectNameTargetResolver is the helper interface to resolve object
// names when the object is not expected to exist. The planner implements
// LookupSchema to return an object consisting of the parent database and
// resolved target schema.
type ObjectNameTargetResolver interface {
	LookupSchema(
		ctx context.Context, dbName, scName string,
	) (found bool, scMeta catalog.ResolvedObjectPrefix, err error)
}

// ErrNoPrimaryKey is returned when resolving a table object and the
// AllowWithoutPrimaryKey flag is not set.
var ErrNoPrimaryKey = pgerror.Newf(pgcode.NoPrimaryKey,
	"requested table does not have a primary key")

// ResolveExistingTableObject looks up an existing object.
// If required is true, an error is returned if the object does not exist.
// Optionally, if a desired descriptor type is specified, that type is checked.
//
// The object name is modified in-place with the result of the name
// resolution, if successful. It is not modified in case of error or
// if no object is found.
//
// TODO(ajwerner): Remove this function. It mutates a table name in place,
// which is lame but a common pattern and it throws away all the work of
// resolving the prefix.
func ResolveExistingTableObject(
	ctx context.Context, sc SchemaResolver, tn *tree.TableName, lookupFlags tree.ObjectLookupFlags,
) (prefix catalog.ResolvedObjectPrefix, res catalog.TableDescriptor, err error) {
	// TODO: As part of work for #34240, an UnresolvedObjectName should be
	//  passed as an argument to this function.
	un := tn.ToUnresolvedObjectName()
	desc, prefix, err := ResolveExistingObject(ctx, sc, un, lookupFlags)
	if err != nil || desc == nil {
		return prefix, nil, err
	}
	tn.ObjectNamePrefix = prefix.NamePrefix()
	return prefix, desc.(catalog.TableDescriptor), nil
}

// ResolveMutableExistingTableObject looks up an existing mutable object.
// If required is true, an error is returned if the object does not exist.
// Optionally, if a desired descriptor type is specified, that type is checked.
//
// The object name is modified in-place with the result of the name
// resolution, if successful. It is not modified in case of error or
// if no object is found.
func ResolveMutableExistingTableObject(
	ctx context.Context,
	sc SchemaResolver,
	tn *tree.TableName,
	required bool,
	requiredType tree.RequiredTableKind,
) (prefix catalog.ResolvedObjectPrefix, res *tabledesc.Mutable, err error) {
	lookupFlags := tree.ObjectLookupFlags{
		Required:             required,
		RequireMutable:       true,
		DesiredObjectKind:    tree.TableObject,
		DesiredTableDescKind: requiredType,
	}
	// TODO: As part of work for #34240, an UnresolvedObjectName should be
	// passed as an argument to this function.
	un := tn.ToUnresolvedObjectName()
	var desc catalog.Descriptor
	desc, prefix, err = ResolveExistingObject(ctx, sc, un, lookupFlags)
	if err != nil || desc == nil {
		return prefix, nil, err
	}
	if tn.ObjectNamePrefix.SchemaName == "" || tn.ObjectNamePrefix.CatalogName == "" {
		tn.ObjectNamePrefix = prefix.NamePrefix()
	}
	return prefix, desc.(*tabledesc.Mutable), nil
}

// ResolveMutableType resolves a type descriptor for mutable access. It
// returns the resolved descriptor, as well as the fully qualified resolved
// object name.
func ResolveMutableType(
	ctx context.Context, sc SchemaResolver, un *tree.UnresolvedObjectName, required bool,
) (catalog.ResolvedObjectPrefix, *typedesc.Mutable, error) {
	lookupFlags := tree.ObjectLookupFlags{
		Required:          required,
		RequireMutable:    true,
		DesiredObjectKind: tree.TypeObject,
	}
	desc, prefix, err := ResolveExistingObject(ctx, sc, un, lookupFlags)
	if err != nil || desc == nil {
		return catalog.ResolvedObjectPrefix{}, nil, err
	}
	switch t := desc.(type) {
	case *typedesc.Mutable:
		return prefix, t, nil
	case catalog.TableImplicitRecordTypeDescriptor:
		return catalog.ResolvedObjectPrefix{}, nil, pgerror.Newf(pgcode.DependentObjectsStillExist,
			"cannot modify table record type %q", desc.GetName())
	default:
		return catalog.ResolvedObjectPrefix{}, nil,
			errors.AssertionFailedf("unhandled type descriptor type %T during resolve mutable desc", t)
	}
}

// ResolveExistingObject resolves an object with the given flags.
func ResolveExistingObject(
	ctx context.Context,
	sc SchemaResolver,
	un *tree.UnresolvedObjectName,
	lookupFlags tree.ObjectLookupFlags,
) (res catalog.Descriptor, _ catalog.ResolvedObjectPrefix, err error) {
	found, prefix, obj, err := ResolveExisting(ctx, un, sc, lookupFlags, sc.CurrentDatabase(), sc.CurrentSearchPath())
	if err != nil {
		return nil, prefix, err
	}
	// Construct the resolved table name for use in error messages.
	if !found {
		if lookupFlags.Required {
			// The contract coming out of ResolveExisting is that if the database
			// or schema exist then they will be populated in the prefix even if
			// the object does not exist. In the case where we have explicit names
			// we can populate a more appropriate error regarding what part of the
			// name does not exist. If we're searching the empty database explicitly,
			// then all bets are off and just return an undefined object error.
			if un.HasExplicitCatalog() && un.Catalog() != "" {
				if prefix.Database == nil {
					return nil, prefix, sqlerrors.NewUndefinedDatabaseError(un.Catalog())
				}
				if un.HasExplicitSchema() && prefix.Schema == nil {
					return nil, prefix, sqlerrors.NewUndefinedSchemaError(un.Schema())
				}
			}
			switch lookupFlags.DesiredObjectKind {
			case tree.TableObject:
				return nil, prefix, sqlerrors.NewUndefinedRelationError(un)
			case tree.TypeObject:
				return nil, prefix, sqlerrors.NewUndefinedTypeError(un)
			case tree.AnyObject:
				return nil, prefix, sqlerrors.NewUndefinedObjectError(un)
			default:
				return nil, prefix, errors.AssertionFailedf("unknown object kind %d", lookupFlags.DesiredObjectKind)
			}
		}
		return nil, prefix, nil
	}
	getResolvedTn := func() *tree.TableName {
		tn := tree.MakeTableNameFromPrefix(prefix.NamePrefix(), tree.Name(un.Object()))
		return &tn
	}

	switch lookupFlags.DesiredObjectKind {
	case tree.TypeObject:
		typ, isType := obj.(catalog.TypeDescriptor)
		if !isType {
			return nil, prefix, sqlerrors.NewUndefinedTypeError(getResolvedTn())
		}
		return typ, prefix, nil
	case tree.TableObject:
		table, ok := obj.(catalog.TableDescriptor)
		if !ok {
			return nil, prefix, sqlerrors.NewUndefinedRelationError(getResolvedTn())
		}
		goodType := true
		switch lookupFlags.DesiredTableDescKind {
		case tree.ResolveRequireTableDesc:
			goodType = table.IsTable()
		case tree.ResolveRequireViewDesc:
			goodType = table.IsView()
		case tree.ResolveRequireTableOrViewDesc:
			goodType = table.IsTable() || table.IsView()
		case tree.ResolveRequireSequenceDesc:
			goodType = table.IsSequence()
		}
		if !goodType {
			return nil, prefix, sqlerrors.NewWrongObjectTypeError(getResolvedTn(), lookupFlags.DesiredTableDescKind.String())
		}

		// If the table does not have a primary key, return an error
		// that the requested descriptor is invalid for use.
		if !lookupFlags.AllowWithoutPrimaryKey &&
			table.IsTable() &&
			!table.HasPrimaryKey() {
			return nil, prefix, ErrNoPrimaryKey
		}

		return obj.(catalog.TableDescriptor), prefix, nil
	case tree.AnyObject:
		return obj, prefix, nil
	default:
		return nil, prefix, errors.AssertionFailedf(
			"unknown desired object kind %d", lookupFlags.DesiredObjectKind)
	}
}

// ResolveTargetObject determines a valid target path for an object
// that may not exist yet. It returns the descriptor for the database
// where the target object lives. It also returns the resolved name
// prefix for the input object.
func ResolveTargetObject(
	ctx context.Context, sc SchemaResolver, un *tree.UnresolvedObjectName,
) (catalog.ResolvedObjectPrefix, tree.ObjectNamePrefix, error) {
	found, prefix, scInfo, err := ResolveTarget(ctx, un, sc, sc.CurrentDatabase(), sc.CurrentSearchPath())
	if err != nil {
		return catalog.ResolvedObjectPrefix{}, prefix, err
	}
	if !found {
		if !un.HasExplicitSchema() && !un.HasExplicitCatalog() {
			return catalog.ResolvedObjectPrefix{}, prefix,
				pgerror.New(pgcode.InvalidName, "no database specified")
		}
		err = pgerror.Newf(pgcode.InvalidSchemaName,
			"cannot create %q because the target database or schema does not exist",
			tree.ErrString(un))
		err = errors.WithHint(err, "verify that the current database and search_path are valid and/or the target database exists")
		return catalog.ResolvedObjectPrefix{}, prefix, err
	}
	if scInfo.Schema.SchemaKind() == catalog.SchemaVirtual {
		return catalog.ResolvedObjectPrefix{}, prefix, pgerror.Newf(pgcode.InsufficientPrivilege,
			"schema cannot be modified: %q", tree.ErrString(&prefix))
	}
	return scInfo, prefix, nil
}

// SchemaEntryForDB entry for an individual schema,
// which includes the name and modification timestamp.
type SchemaEntryForDB struct {
	Name      string
	Timestamp hlc.Timestamp
}

// ResolveExisting performs name resolution for an object name when
// the target object is expected to exist already. It does not
// mutate the input name. It additionally returns the resolved
// prefix qualification for the object. For example, if the unresolved
// name was "a.b" and the name was resolved to "a.public.b", the
// prefix "a.public" is returned.
//
// Note that the returned prefix will be populated with the relevant found
// components because LookupObject retains those components. This is error
// prone and exists only for backwards compatibility with certain error
// reporting behaviors.
//
// Note also that if the implied current database does not exist and the name
// is either unqualified or qualified by a virtual schema, an error will be
// returned to indicate that the database does not exist. This error will be
// returned regardless of the value set on the Required flag.
func ResolveExisting(
	ctx context.Context,
	u *tree.UnresolvedObjectName,
	r ObjectNameExistingResolver,
	lookupFlags tree.ObjectLookupFlags,
	curDb string,
	searchPath sessiondata.SearchPath,
) (found bool, prefix catalog.ResolvedObjectPrefix, result catalog.Descriptor, err error) {
	if u.HasExplicitSchema() {
		if u.HasExplicitCatalog() {
			// Already 3 parts: nothing to search. Delegate to the resolver.
			found, prefix, result, err = r.LookupObject(ctx, lookupFlags, u.Catalog(), u.Schema(), u.Object())
			prefix.ExplicitDatabase, prefix.ExplicitSchema = true, true
			return found, prefix, result, err
		}

		// Two parts: D.T.
		// Try to use the current database, and be satisfied if it's sufficient to find the object.
		//
		// Note: CockroachDB supports querying virtual schemas even when the current
		// database is not set. For example, `select * from pg_catalog.pg_tables` is
		// meant to show all tables across all databases when there is no current
		// database set. Therefore, we test this even if curDb == "", as long as the
		// schema name is for a virtual schema.
		_, isVirtualSchema := catconstants.VirtualSchemaNames[u.Schema()]
		if isVirtualSchema || curDb != "" {
			if found, prefix, result, err = r.LookupObject(
				ctx, lookupFlags, curDb, u.Schema(), u.Object(),
			); found || err != nil || isVirtualSchema {
				if !found && err == nil && prefix.Database == nil { // && isVirtualSchema
					// If the database was not found during the lookup for a virtual schema
					// we should return a database not found error. We will use the prefix
					// information to confirm this, since its possible that someone might
					// be selecting a non-existent table or type. While normally we generate
					// errors above this layer, we have no way of flagging if the looked up object
					// was virtual schema. The Required flag is never set above when doing
					// the object look up, so no errors will be generated for missing objects
					// or databases.
					err = sqlerrors.NewUndefinedDatabaseError(curDb)
				}
				// Special case the qualification of virtual schema accesses for
				// backwards compatibility.
				prefix.ExplicitDatabase = false
				prefix.ExplicitSchema = true
				return found, prefix, result, err
			}
		}

		// No luck so far. Compatibility with CockroachDB v1.1: try D.public.T instead.
		found, prefix, result, err = r.LookupObject(ctx, lookupFlags, u.Schema(), catconstants.PublicSchemaName, u.Object())
		if found && err == nil {
			prefix.ExplicitSchema = true
			prefix.ExplicitDatabase = true
		}
		return found, prefix, result, err
	}

	// This is a naked object name. Use the search path.
	iter := searchPath.Iter()
	foundDatabase := false
	for next, ok := iter.Next(); ok; next, ok = iter.Next() {
		if found, prefix, result, err = r.LookupObject(
			ctx, lookupFlags, curDb, next, u.Object(),
		); found || err != nil {
			return found, prefix, result, err
		}
		foundDatabase = foundDatabase || prefix.Database != nil
	}

	// If we have a database, and we didn't find it, then we're never going to
	// find it because it must not exist. This error return path is a bit of
	// a rough edge, but it preserves backwards compatibility and makes sure
	// we return a database does not exist error in cases where the current
	// database definitely does not exist.
	if curDb != "" && !foundDatabase {
		return false, prefix, nil, sqlerrors.NewUndefinedDatabaseError(curDb)
	}
	return false, prefix, nil, nil
}

// ResolveTarget performs name resolution for an object name when
// the target object is not expected to exist already. It does not
// mutate the input name. It additionally returns the resolved
// prefix qualification for the object. For example, if the unresolved
// name was "a.b" and the name was resolved to "a.public.b", the
// prefix "a.public" is returned.
func ResolveTarget(
	ctx context.Context,
	u *tree.UnresolvedObjectName,
	r ObjectNameTargetResolver,
	curDb string,
	searchPath sessiondata.SearchPath,
) (found bool, _ tree.ObjectNamePrefix, scMeta catalog.ResolvedObjectPrefix, err error) {
	if u.HasExplicitSchema() {
		if u.HasExplicitCatalog() {
			// Already 3 parts: nothing to do.
			found, scMeta, err = r.LookupSchema(ctx, u.Catalog(), u.Schema())
			scMeta.ExplicitDatabase, scMeta.ExplicitSchema = true, true
			return found, scMeta.NamePrefix(), scMeta, err
		}
		// Two parts: D.T.
		// Try to use the current database, and be satisfied if it's sufficient to find the object.
		if found, scMeta, err = r.LookupSchema(ctx, curDb, u.Schema()); found || err != nil {
			if err == nil {
				scMeta.ExplicitDatabase, scMeta.ExplicitSchema = false, true
			}
			return found, scMeta.NamePrefix(), scMeta, err
		}
		// No luck so far. Compatibility with CockroachDB v1.1: use D.public.T instead.
		if found, scMeta, err = r.LookupSchema(ctx, u.Schema(), catconstants.PublicSchemaName); found || err != nil {
			if err == nil {
				scMeta.ExplicitDatabase, scMeta.ExplicitSchema = true, true
			}
			return found, scMeta.NamePrefix(), scMeta, err
		}
		// Welp, really haven't found anything.
		return false, tree.ObjectNamePrefix{}, catalog.ResolvedObjectPrefix{}, nil
	}

	// This is a naked table name. Use the current schema = the first
	// valid item in the search path.
	iter := searchPath.IterWithoutImplicitPGSchemas()
	for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
		if found, scMeta, err = r.LookupSchema(ctx, curDb, scName); found || err != nil {
			if err == nil {
				scMeta.ExplicitDatabase, scMeta.ExplicitSchema = false, false
			}
			break
		}
	}
	return found, scMeta.NamePrefix(), scMeta, err
}

// ResolveObjectNamePrefix is used for table prefixes. This is adequate for table
// patterns with stars, e.g. AllTablesSelector.
func ResolveObjectNamePrefix(
	ctx context.Context,
	r ObjectNameTargetResolver,
	curDb string,
	searchPath sessiondata.SearchPath,
	tp *tree.ObjectNamePrefix,
) (found bool, scMeta catalog.ResolvedObjectPrefix, err error) {
	if tp.ExplicitSchema {
		return resolvePrefixWithExplicitSchema(ctx, r, curDb, searchPath, tp)
	}
	// This is a naked table name. Use the current schema = the first
	// valid item in the search path.
	iter := searchPath.IterWithoutImplicitPGSchemas()
	for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
		if found, scMeta, err = r.LookupSchema(ctx, curDb, scName); found || err != nil {
			if err == nil {
				tp.CatalogName = tree.Name(curDb)
				tp.SchemaName = tree.Name(scName)
			}
			break
		}
	}
	return found, scMeta, err
}

// ResolveIndex tries to find an index with a TableIndexName.
// (1) If table name is provided, table is resolved first, and it looks for a
// non-dropped index from the table. Primary index is returned if index name is
// empty or LegacyPrimaryKeyIndexName,
// (2) If table name is not provided:
// a: If schema name is provided, schema name is first resolved, and then all
// tables are looped to search for the index.
// b: If schema name is not provided, all tables within all schemas	on the
// search path are looped for the index.
//
// Error is returned when any of the these happened:
// (1) "require=true" but index does not exist.
// (2) index name is ambiguous, namely more than one table in a same schema
// contain indexes with same name.
// (3) table name is present from input, but the resolved object is not a table
// or materialized view.
// (4) table name is not present, the given schema or schemas on search path can
// not be found.
func ResolveIndex(
	ctx context.Context,
	schemaResolver SchemaResolver,
	tableIndexName *tree.TableIndexName,
	flag tree.IndexLookupFlags,
) (
	found bool,
	prefix catalog.ResolvedObjectPrefix,
	tblDesc catalog.TableDescriptor,
	idxDesc catalog.Index,
	err error,
) {
	if tableIndexName.Table.ObjectName != "" {
		lflags := tree.ObjectLookupFlags{
			Required:             flag.Required,
			IncludeOffline:       flag.IncludeOfflineTable,
			DesiredObjectKind:    tree.TableObject,
			DesiredTableDescKind: tree.ResolveRequireTableOrViewDesc,
		}

		resolvedPrefix, candidateTbl, err := ResolveExistingTableObject(ctx, schemaResolver, &tableIndexName.Table, lflags)
		if err != nil {
			return false, catalog.ResolvedObjectPrefix{}, nil, nil, err
		}
		if candidateTbl == nil {
			if flag.Required {
				err = pgerror.Newf(
					pgcode.UndefinedObject, "index %q does not exist", tableIndexName.Index,
				)
			}
			return false, catalog.ResolvedObjectPrefix{}, nil, nil, err
		}
		if candidateTbl.IsView() && !candidateTbl.MaterializedView() {
			return false, catalog.ResolvedObjectPrefix{}, nil, nil, pgerror.Newf(
				pgcode.WrongObjectType, "%q is not a table or materialized view", tableIndexName.Table.ObjectName,
			)
		}

		if tableIndexName.Index == "" {
			// Return primary index.
			return true, resolvedPrefix, candidateTbl, candidateTbl.GetPrimaryIndex(), nil
		}

		idx := catalog.FindIndex(
			candidateTbl,
			catalog.IndexOpts{AddMutations: true},
			func(idx catalog.Index) bool {
				return idx.GetName() == string(tableIndexName.Index)
			},
		)
		if idx != nil && (flag.IncludeNonActiveIndex || idx.Public()) {
			return true, resolvedPrefix, candidateTbl, idx, nil
		}

		// Fallback to referencing @primary as the PRIMARY KEY.
		// Note that indexes with "primary" as their name takes precedence above.
		if tableIndexName.Index == tabledesc.LegacyPrimaryKeyIndexName {
			return true, resolvedPrefix, candidateTbl, candidateTbl.GetPrimaryIndex(), nil
		}

		if flag.Required {
			return false, catalog.ResolvedObjectPrefix{}, nil, nil, pgerror.Newf(
				pgcode.UndefinedObject, "index %q does not exist", tableIndexName.Index,
			)
		}
		return false, catalog.ResolvedObjectPrefix{}, nil, nil, nil
	}

	if tableIndexName.Table.ExplicitSchema {
		resolvedPrefix, err := resolveSchemaByName(ctx, schemaResolver, &tableIndexName.Table.ObjectNamePrefix, schemaResolver.CurrentSearchPath())
		if err != nil {
			return false, catalog.ResolvedObjectPrefix{}, nil, nil, err
		}

		tblFound, tbl, idx, err := findTableContainingIndex(
			ctx, tree.Name(tableIndexName.Index), resolvedPrefix, schemaResolver, flag.IncludeNonActiveIndex, flag.IncludeOfflineTable,
		)
		if err != nil {
			return false, catalog.ResolvedObjectPrefix{}, nil, nil, err
		}
		if tblFound {
			return true, resolvedPrefix, tbl, idx, nil
		}
		if flag.Required {
			return false, catalog.ResolvedObjectPrefix{}, nil, nil, pgerror.Newf(
				pgcode.UndefinedObject, "index %q does not exist", tableIndexName.Index,
			)
		}
		return false, catalog.ResolvedObjectPrefix{}, nil, nil, nil
	}

	var schemaFound bool
	catalogName := schemaResolver.CurrentDatabase()
	if tableIndexName.Table.ExplicitCatalog {
		catalogName = tableIndexName.Table.Catalog()
	}
	// We need to iterate through all the schemas in the search path.
	iter := schemaResolver.CurrentSearchPath().IterWithoutImplicitPGSchemas()
	for path, ok := iter.Next(); ok; path, ok = iter.Next() {
		curPrefix := tree.ObjectNamePrefix{
			CatalogName:     tree.Name(catalogName),
			ExplicitCatalog: true,
			SchemaName:      tree.Name(path),
			ExplicitSchema:  true,
		}

		found, candidateResolvedPrefix, curErr := resolvePrefixWithExplicitSchema(
			ctx, schemaResolver, schemaResolver.CurrentDatabase(), schemaResolver.CurrentSearchPath(), &curPrefix,
		)
		if curErr != nil {
			return false, catalog.ResolvedObjectPrefix{}, nil, nil, curErr
		}
		if !found {
			continue
		}
		schemaFound = true

		candidateFound, tbl, idx, curErr := findTableContainingIndex(
			ctx, tree.Name(tableIndexName.Index), candidateResolvedPrefix, schemaResolver, flag.IncludeNonActiveIndex, flag.IncludeOfflineTable,
		)
		if curErr != nil {
			return false, catalog.ResolvedObjectPrefix{}, nil, nil, curErr
		}

		if candidateFound {
			return true, candidateResolvedPrefix, tbl, idx, nil
		}
	}

	if !schemaFound {
		return false, catalog.ResolvedObjectPrefix{}, nil, nil, pgerror.Newf(
			pgcode.UndefinedObject, "schema or database was not found while searching index: %q", tableIndexName.Index,
		)
	}

	if flag.Required {
		return false, catalog.ResolvedObjectPrefix{}, nil, nil, pgerror.Newf(
			pgcode.UndefinedObject, "index %q does not exist", tableIndexName.Index,
		)
	}
	return false, catalog.ResolvedObjectPrefix{}, nil, nil, nil
}

// resolvePrefixWithExplicitSchema resolves a name prefix with explicit schema.
// Error won't be return if schema is not found. Instead, found=false is return.
// (1) If db(catalog) name is given, it looks up schema within the db.
// (2) If db(catalog) name is not given, it first looks at current db to look up
// the schema. If a schema cannot be found given the name, but there is a
// database with the same name, public schema under this db is returned.
func resolvePrefixWithExplicitSchema(
	ctx context.Context,
	r ObjectNameTargetResolver,
	curDb string,
	searchPath sessiondata.SearchPath,
	tp *tree.ObjectNamePrefix,
) (found bool, scMeta catalog.ResolvedObjectPrefix, err error) {
	// pg_temp can be used as an alias for the current sessions temporary schema.
	// We must perform this resolution before looking up the object. This
	// resolution only succeeds if the session already has a temporary schema.
	scName, err := searchPath.MaybeResolveTemporarySchema(tp.Schema())
	if err != nil {
		return false, catalog.ResolvedObjectPrefix{}, err
	}
	if tp.ExplicitCatalog {
		// Catalog name is explicit; nothing to do.
		tp.SchemaName = tree.Name(scName)
		return r.LookupSchema(ctx, tp.Catalog(), scName)
	}
	// Try with the current database. This may be empty, because
	// virtual schemas exist even when the db name is empty
	// (CockroachDB extension).
	if found, scMeta, err = r.LookupSchema(ctx, curDb, scName); found || err != nil {
		if err == nil {
			tp.CatalogName = tree.Name(curDb)
			tp.SchemaName = tree.Name(scName)
		}
		return found, scMeta, err
	}
	// No luck so far. Compatibility with CockroachDB v1.1: use D.public.T instead.
	if found, scMeta, err = r.LookupSchema(ctx, tp.Schema(), catconstants.PublicSchemaName); found || err != nil {
		if err == nil {
			tp.CatalogName = tp.SchemaName
			tp.SchemaName = catconstants.PublicSchemaName
			tp.ExplicitCatalog = true
		}
		return found, scMeta, err
	}
	// No luck.
	return false, catalog.ResolvedObjectPrefix{}, nil
}

// resolveSchemaByName is similar to resolvePrefixWithExplicitSchema, but
// returns error if schema is not found.
func resolveSchemaByName(
	ctx context.Context,
	schemaResolver SchemaResolver,
	prefix *tree.ObjectNamePrefix,
	searchPath sessiondata.SearchPath,
) (catalog.ResolvedObjectPrefix, error) {
	if !prefix.ExplicitSchema {
		return catalog.ResolvedObjectPrefix{}, pgerror.New(
			pgcode.InvalidName, "no schema specified",
		)
	}

	found, resolvedPrefix, err := resolvePrefixWithExplicitSchema(
		ctx, schemaResolver, schemaResolver.CurrentDatabase(), searchPath, prefix,
	)
	if err != nil {
		return catalog.ResolvedObjectPrefix{}, err
	}
	if !found {
		return catalog.ResolvedObjectPrefix{}, pgerror.Newf(
			pgcode.InvalidSchemaName, "target database or schema does not exist",
		)
	}

	return resolvedPrefix, nil
}

// findTableContainingIndex tries to find and index in a schema by iterating
// through all tables.
// Error is returned if Index name is ambiguous between tables. No error is
// returned if no table is found.
func findTableContainingIndex(
	ctx context.Context,
	indexName tree.Name,
	resolvedPrefix catalog.ResolvedObjectPrefix,
	schemaResolver SchemaResolver,
	includeNonActiveIndex bool,
	includeOfflineTable bool,
) (found bool, tblDesc catalog.TableDescriptor, idxDesc catalog.Index, err error) {
	dsNames, _, err := schemaResolver.GetObjectNamesAndIDs(ctx, resolvedPrefix.Database, resolvedPrefix.Schema)

	if err != nil {
		return false, nil, nil, err
	}

	lflags := tree.ObjectLookupFlags{
		IncludeOffline:       true,
		DesiredObjectKind:    tree.TableObject,
		DesiredTableDescKind: tree.ResolveAnyTableKind,
	}
	for _, dsName := range dsNames {
		_, candidateTbl, err := ResolveExistingTableObject(ctx, schemaResolver, &dsName, lflags)
		if err != nil {
			return false, nil, nil, err
		}
		if candidateTbl == nil ||
			!(candidateTbl.IsTable() ||
				candidateTbl.MaterializedView()) ||
			(!includeOfflineTable && candidateTbl.Offline()) {
			continue
		}

		candidateIdx := catalog.FindIndex(
			candidateTbl,
			catalog.IndexOpts{AddMutations: true},
			func(idx catalog.Index) bool {
				return idx.GetName() == string(indexName)
			},
		)
		if candidateIdx == nil {
			continue
		}
		if !includeNonActiveIndex && !candidateIdx.Public() {
			continue
		}
		if found {
			prefix := resolvedPrefix.NamePrefix()
			tn1 := tree.MakeTableNameWithSchema(prefix.CatalogName, prefix.SchemaName, tree.Name(candidateTbl.GetName()))
			tn2 := tree.MakeTableNameWithSchema(prefix.CatalogName, prefix.SchemaName, tree.Name(tblDesc.GetName()))
			return false, nil, nil, pgerror.Newf(
				pgcode.AmbiguousParameter, "index name %q is ambiguous (found in %s and %s)",
				indexName,
				tn1.String(),
				tn2.String(),
			)
		}
		found = true
		tblDesc = candidateTbl
		idxDesc = candidateIdx
	}

	return found, tblDesc, idxDesc, nil
}
