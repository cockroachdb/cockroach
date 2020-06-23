// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// This file contains the two major components to name resolution:
//
// - classification algorithms. These are used when two different
//   semantic constructs can appear in the same position in the SQL grammar.
//   For example, table patterns and table names in GRANT.
//
// - resolution algorithms. These are used to map an unresolved name
//   or pattern to something looked up from the database.
//

// classifyTablePattern distinguishes between a TableName (last name
// part is a table name) and an AllTablesSelector.
// Used e.g. for GRANT.
func classifyTablePattern(n *UnresolvedName) (TablePattern, error) {
	if n.NumParts < 1 || n.NumParts > 3 {
		return nil, newInvTableNameError(n)
	}
	// Check that all the parts specified are not empty.
	firstCheck := 0
	if n.Star {
		firstCheck = 1
	}
	// It's OK if the catalog name is empty.
	// We allow this in e.g. `select * from "".crdb_internal.tables`.
	lastCheck := n.NumParts
	if lastCheck > 2 {
		lastCheck = 2
	}
	for i := firstCheck; i < lastCheck; i++ {
		if len(n.Parts[i]) == 0 {
			return nil, newInvTableNameError(n)
		}
	}

	// Construct the result.
	if n.Star {
		return &AllTablesSelector{makeObjectNamePrefixFromUnresolvedName(n)}, nil
	}
	tb := makeTableNameFromUnresolvedName(n)
	return &tb, nil
}

// classifyColumnItem distinguishes between a ColumnItem (last name
// part is a column name) and an AllColumnsSelector.
//
// Used e.g. in SELECT clauses.
func classifyColumnItem(n *UnresolvedName) (VarName, error) {
	if n.NumParts < 1 || n.NumParts > 4 {
		return nil, newInvColRef(n)
	}

	// Check that all the parts specified are not empty.
	firstCheck := 0
	if n.Star {
		firstCheck = 1
	}
	// It's OK if the catalog name is empty.
	// We allow this in e.g.
	// `select "".crdb_internal.tables.table_id from "".crdb_internal.tables`.
	lastCheck := n.NumParts
	if lastCheck > 3 {
		lastCheck = 3
	}
	for i := firstCheck; i < lastCheck; i++ {
		if len(n.Parts[i]) == 0 {
			return nil, newInvColRef(n)
		}
	}

	// Construct the result.
	var tn *UnresolvedObjectName
	if n.NumParts > 1 {
		var err error
		tn, err = NewUnresolvedObjectName(
			n.NumParts-1,
			[3]string{n.Parts[1], n.Parts[2], n.Parts[3]},
			NoAnnotation,
		)
		if err != nil {
			return nil, err
		}
	}
	if n.Star {
		return &AllColumnsSelector{TableName: tn}, nil
	}
	return &ColumnItem{TableName: tn, ColumnName: Name(n.Parts[0])}, nil
}

// Resolution algorithms follow.

const (
	// PublicSchema is the name of the physical schema in every
	// database/catalog.
	PublicSchema string = sessiondata.PublicSchemaName
	// PublicSchemaName is the same, typed as Name.
	PublicSchemaName Name = Name(PublicSchema)
)

// NumResolutionResults represents the number of results in the lookup
// of data sources matching a given prefix.
type NumResolutionResults int

const (
	// NoResults for when there is no result.
	NoResults NumResolutionResults = iota
	// ExactlyOne indicates just one source matching the requested name.
	ExactlyOne
	// MoreThanOne signals an ambiguous match.
	MoreThanOne
)

// ColumnItemResolver is the helper interface to resolve column items.
type ColumnItemResolver interface {
	// FindSourceMatchingName searches for a data source with name tn.
	//
	// This must error out with "ambiguous table name" if there is more
	// than one data source matching tn. The srcMeta is subsequently
	// passed to Resolve() if resolution succeeds. The prefix will not be
	// modified.
	FindSourceMatchingName(ctx context.Context, tn TableName) (res NumResolutionResults, prefix *TableName, srcMeta ColumnSourceMeta, err error)

	// FindSourceProvidingColumn searches for a data source providing
	// a column with the name given.
	//
	// This must error out with "ambiguous column name" if there is more
	// than one data source matching tn, "column not found" if there is
	// none. The srcMeta and colHints are subsequently passed to
	// Resolve() if resolution succeeds. The prefix will not be
	// modified.
	FindSourceProvidingColumn(ctx context.Context, col Name) (prefix *TableName, srcMeta ColumnSourceMeta, colHint int, err error)

	// Resolve() is called if resolution succeeds.
	Resolve(ctx context.Context, prefix *TableName, srcMeta ColumnSourceMeta, colHint int, col Name) (ColumnResolutionResult, error)
}

// ColumnSourceMeta is an opaque reference passed through column item resolution.
type ColumnSourceMeta interface {
	// ColumnSourcMeta is the interface anchor.
	ColumnSourceMeta()
}

// ColumnResolutionResult is an opaque reference returned by ColumnItemResolver.Resolve().
type ColumnResolutionResult interface {
	// ColumnResolutionResult is the interface anchor.
	ColumnResolutionResult()
}

// Resolve performs name resolution for a qualified star using a resolver.
func (a *AllColumnsSelector) Resolve(
	ctx context.Context, r ColumnItemResolver,
) (srcName *TableName, srcMeta ColumnSourceMeta, err error) {
	prefix := a.TableName.ToTableName()

	// Is there a data source with this prefix?
	var res NumResolutionResults
	res, srcName, srcMeta, err = r.FindSourceMatchingName(ctx, prefix)
	if err != nil {
		return nil, nil, err
	}
	if res == NoResults && a.TableName.NumParts == 2 {
		// No, but name of form db.tbl.*?
		// Special rule for compatibility with CockroachDB v1.x:
		// search name db.public.tbl.* instead.
		prefix.ExplicitCatalog = true
		prefix.CatalogName = prefix.SchemaName
		prefix.SchemaName = PublicSchemaName
		res, srcName, srcMeta, err = r.FindSourceMatchingName(ctx, prefix)
		if err != nil {
			return nil, nil, err
		}
	}
	if res == NoResults {
		return nil, nil, newSourceNotFoundError("no data source matches pattern: %s", a)
	}
	return srcName, srcMeta, nil
}

// Resolve performs name resolution for a column item using a resolver.
func (c *ColumnItem) Resolve(
	ctx context.Context, r ColumnItemResolver,
) (ColumnResolutionResult, error) {
	colName := c.ColumnName
	if c.TableName == nil {
		// Naked column name: simple case.
		srcName, srcMeta, cHint, err := r.FindSourceProvidingColumn(ctx, colName)
		if err != nil {
			return nil, err
		}
		return r.Resolve(ctx, srcName, srcMeta, cHint, colName)
	}

	// There is a prefix. We need to search for it.
	prefix := c.TableName.ToTableName()

	// Is there a data source with this prefix?
	res, srcName, srcMeta, err := r.FindSourceMatchingName(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if res == NoResults && c.TableName.NumParts == 2 {
		// No, but name of form db.tbl.x?
		// Special rule for compatibility with CockroachDB v1.x:
		// search name db.public.tbl.x instead.
		prefix.ExplicitCatalog = true
		prefix.CatalogName = prefix.SchemaName
		prefix.SchemaName = PublicSchemaName
		res, srcName, srcMeta, err = r.FindSourceMatchingName(ctx, prefix)
		if err != nil {
			return nil, err
		}
	}
	if res == NoResults {
		return nil, newSourceNotFoundError("no data source matches prefix: %s in this context", c.TableName)
	}
	return r.Resolve(ctx, srcName, srcMeta, -1, colName)
}

// ObjectNameTargetResolver is the helper interface to resolve object
// names when the object is not expected to exist.
//
// TODO(ajwerner): figure out what scMeta is supposed to be. Currently it's
// the database but with User-defined schemas, should it be the schema?
// Should it be both?
type ObjectNameTargetResolver interface {
	LookupSchema(ctx context.Context, dbName, scName string) (found bool, scMeta SchemaMeta, err error)
}

// SchemaMeta is an opaque reference returned by LookupSchema().
type SchemaMeta interface {
	// SchemaMeta is the interface anchor.
	SchemaMeta()
}

// ObjectNameExistingResolver is the helper interface to resolve table
// names when the object is expected to exist already. The boolean passed
// is used to specify if a MutableTableDescriptor is to be returned in the
// result.
type ObjectNameExistingResolver interface {
	LookupObject(ctx context.Context, flags ObjectLookupFlags, dbName, scName, obName string) (
		found bool, objMeta NameResolutionResult, err error,
	)
}

// NameResolutionResult is an opaque reference returned by LookupObject().
type NameResolutionResult interface {
	// NameResolutionResult is the interface anchor.
	NameResolutionResult()
}

// ResolveExisting performs name resolution for an object name when
// the target object is expected to exist already. It does not
// mutate the input name. It additionally returns the resolved
// prefix qualification for the object. For example, if the unresolved
// name was "a.b" and the name was resolved to "a.public.b", the
// prefix "a.public" is returned.
func ResolveExisting(
	ctx context.Context,
	u *UnresolvedObjectName,
	r ObjectNameExistingResolver,
	lookupFlags ObjectLookupFlags,
	curDb string,
	searchPath sessiondata.SearchPath,
) (bool, ObjectNamePrefix, NameResolutionResult, error) {
	namePrefix := ObjectNamePrefix{
		SchemaName:      Name(u.Schema()),
		ExplicitSchema:  u.HasExplicitSchema(),
		CatalogName:     Name(u.Catalog()),
		ExplicitCatalog: u.HasExplicitCatalog(),
	}
	if u.HasExplicitSchema() {
		// pg_temp can be used as an alias for the current sessions temporary schema.
		// We must perform this resolution before looking up the object. This
		// resolution only succeeds if the session already has a temporary schema.
		scName, err := searchPath.MaybeResolveTemporarySchema(u.Schema())
		if err != nil {
			return false, namePrefix, nil, err
		}
		if u.HasExplicitCatalog() {
			// Already 3 parts: nothing to search. Delegate to the resolver.
			namePrefix.CatalogName = Name(u.Catalog())
			namePrefix.SchemaName = Name(u.Schema())
			found, result, err := r.LookupObject(ctx, lookupFlags, u.Catalog(), scName, u.Object())
			return found, namePrefix, result, err
		}
		// Two parts: D.T.
		// Try to use the current database, and be satisfied if it's sufficient to find the object.
		//
		// Note: we test this even if curDb == "", because CockroachDB
		// supports querying virtual schemas even when the current
		// database is not set. For example, `select * from
		// pg_catalog.pg_tables` is meant to show all tables across all
		// databases when there is no current database set.

		if found, objMeta, err := r.LookupObject(ctx, lookupFlags, curDb, scName, u.Object()); found || err != nil {
			if err == nil {
				namePrefix.CatalogName = Name(curDb)
			}
			return found, namePrefix, objMeta, err
		}
		// No luck so far. Compatibility with CockroachDB v1.1: try D.public.T instead.
		if found, objMeta, err := r.LookupObject(ctx, lookupFlags, u.Schema(), PublicSchema, u.Object()); found || err != nil {
			if err == nil {
				namePrefix.CatalogName = Name(u.Schema())
				namePrefix.SchemaName = PublicSchemaName
				namePrefix.ExplicitCatalog = true
			}
			return found, namePrefix, objMeta, err
		}
		// Welp, really haven't found anything.
		return false, namePrefix, nil, nil
	}

	// This is a naked table name. Use the search path.
	iter := searchPath.Iter()
	for next, ok := iter.Next(); ok; next, ok = iter.Next() {
		if found, objMeta, err := r.LookupObject(ctx, lookupFlags, curDb, next, u.Object()); found || err != nil {
			if err == nil {
				namePrefix.CatalogName = Name(curDb)
				namePrefix.SchemaName = Name(next)
			}
			return found, namePrefix, objMeta, err
		}
	}
	return false, namePrefix, nil, nil
}

// ResolveTarget performs name resolution for an object name when
// the target object is not expected to exist already. It does not
// mutate the input name. It additionally returns the resolved
// prefix qualification for the object. For example, if the unresolved
// name was "a.b" and the name was resolved to "a.public.b", the
// prefix "a.public" is returned.
func ResolveTarget(
	ctx context.Context,
	u *UnresolvedObjectName,
	r ObjectNameTargetResolver,
	curDb string,
	searchPath sessiondata.SearchPath,
) (found bool, namePrefix ObjectNamePrefix, scMeta SchemaMeta, err error) {
	namePrefix = ObjectNamePrefix{
		SchemaName:      Name(u.Schema()),
		ExplicitSchema:  u.HasExplicitSchema(),
		CatalogName:     Name(u.Catalog()),
		ExplicitCatalog: u.HasExplicitCatalog(),
	}
	if u.HasExplicitSchema() {
		// pg_temp can be used as an alias for the current sessions temporary schema.
		// We must perform this resolution before looking up the object. This
		// resolution only succeeds if the session already has a temporary schema.
		scName, err := searchPath.MaybeResolveTemporarySchema(u.Schema())
		if err != nil {
			return false, namePrefix, nil, err
		}
		if u.HasExplicitCatalog() {
			// Already 3 parts: nothing to do.
			found, scMeta, err = r.LookupSchema(ctx, u.Catalog(), scName)
			return found, namePrefix, scMeta, err
		}
		// Two parts: D.T.
		// Try to use the current database, and be satisfied if it's sufficient to find the object.
		if found, scMeta, err = r.LookupSchema(ctx, curDb, scName); found || err != nil {
			if err == nil {
				namePrefix.CatalogName = Name(curDb)
			}
			return found, namePrefix, scMeta, err
		}
		// No luck so far. Compatibility with CockroachDB v1.1: use D.public.T instead.
		if found, scMeta, err = r.LookupSchema(ctx, u.Schema(), PublicSchema); found || err != nil {
			if err == nil {
				namePrefix.CatalogName = Name(u.Schema())
				namePrefix.SchemaName = PublicSchemaName
				namePrefix.ExplicitCatalog = true
			}
			return found, namePrefix, scMeta, err
		}
		// Welp, really haven't found anything.
		return false, namePrefix, nil, nil
	}

	// This is a naked table name. Use the current schema = the first
	// valid item in the search path.
	iter := searchPath.IterWithoutImplicitPGSchemas()
	for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
		if found, scMeta, err = r.LookupSchema(ctx, curDb, scName); found || err != nil {
			if err == nil {
				namePrefix.CatalogName = Name(curDb)
				namePrefix.SchemaName = Name(scName)
			}
			break
		}
	}
	return found, namePrefix, scMeta, err
}

// Resolve is used for table prefixes. This is adequate for table
// patterns with stars, e.g. AllTablesSelector.
func (tp *ObjectNamePrefix) Resolve(
	ctx context.Context, r ObjectNameTargetResolver, curDb string, searchPath sessiondata.SearchPath,
) (found bool, scMeta SchemaMeta, err error) {
	if tp.ExplicitSchema {
		// pg_temp can be used as an alias for the current sessions temporary schema.
		// We must perform this resolution before looking up the object. This
		// resolution only succeeds if the session already has a temporary schema.
		scName, err := searchPath.MaybeResolveTemporarySchema(tp.Schema())
		if err != nil {
			return false, nil, err
		}
		if tp.ExplicitCatalog {
			// Catalog name is explicit; nothing to do.
			return r.LookupSchema(ctx, tp.Catalog(), scName)
		}
		// Try with the current database. This may be empty, because
		// virtual schemas exist even when the db name is empty
		// (CockroachDB extension).
		if found, scMeta, err = r.LookupSchema(ctx, curDb, scName); found || err != nil {
			if err == nil {
				tp.CatalogName = Name(curDb)
			}
			return found, scMeta, err
		}
		// No luck so far. Compatibility with CockroachDB v1.1: use D.public.T instead.
		if found, scMeta, err = r.LookupSchema(ctx, tp.Schema(), PublicSchema); found || err != nil {
			if err == nil {
				tp.CatalogName = tp.SchemaName
				tp.SchemaName = PublicSchemaName
				tp.ExplicitCatalog = true
			}
			return found, scMeta, err
		}
		// No luck.
		return false, nil, nil
	}
	// This is a naked table name. Use the current schema = the first
	// valid item in the search path.
	iter := searchPath.IterWithoutImplicitPGSchemas()
	for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
		if found, scMeta, err = r.LookupSchema(ctx, curDb, scName); found || err != nil {
			if err == nil {
				tp.CatalogName = Name(curDb)
				tp.SchemaName = Name(scName)
			}
			break
		}
	}
	return found, scMeta, err
}

// ResolveFunction transforms an UnresolvedName to a FunctionDefinition.
//
// Function resolution currently takes a "short path" using the
// assumption that there are no stored functions in the database. That
// is, only functions in the (virtual) global namespace and virtual
// schemas can be used. This in turn implies that the current
// database does not matter and no resolver is needed.
//
// TODO(whoever): this needs to be revisited when there can be stored functions.
// When that is the case, function names must be first normalized to e.g.
// TableName (or whatever an object name will be called by then)
// and then undergo regular name resolution via ResolveExisting(). When
// that happens, the following function can be removed.
func (n *UnresolvedName) ResolveFunction(
	searchPath sessiondata.SearchPath,
) (*FunctionDefinition, error) {
	if n.NumParts > 3 || len(n.Parts[0]) == 0 || n.Star {
		// The Star part of the condition is really an assertion. The
		// parser should not have let this star propagate to a point where
		// this method is called.
		return nil, pgerror.Newf(pgcode.InvalidName,
			"invalid function name: %s", n)
	}

	// We ignore the catalog part. Like explained above, we currently
	// only support functions in virtual schemas, which always exist
	// independently of the database/catalog prefix.
	function, prefix := n.Parts[0], n.Parts[1]

	if d, ok := FunDefs[function]; ok && prefix == "" {
		// Fast path: return early.
		return d, nil
	}

	fullName := function

	if prefix == sessiondata.PgCatalogName {
		// If the user specified e.g. `pg_catalog.max()` we want to find
		// it in the global namespace.
		prefix = ""
	}

	if prefix != "" {
		fullName = prefix + "." + function
	}
	def, ok := FunDefs[fullName]
	if !ok {
		found := false
		if prefix == "" {
			// The function wasn't qualified, so we must search for it via
			// the search path first.
			iter := searchPath.Iter()
			for alt, ok := iter.Next(); ok; alt, ok = iter.Next() {
				fullName = alt + "." + function
				if def, ok = FunDefs[fullName]; ok {
					found = true
					break
				}
			}
		}
		if !found {
			extraMsg := ""
			// Try a little harder.
			if rdef, ok := FunDefs[strings.ToLower(function)]; ok {
				extraMsg = fmt.Sprintf(", but %s() exists", rdef.Name)
			}
			return nil, pgerror.Newf(
				pgcode.UndefinedFunction, "unknown function: %s()%s", ErrString(n), extraMsg)
		}
	}

	return def, nil
}

func newInvColRef(n *UnresolvedName) error {
	return pgerror.NewWithDepthf(1, pgcode.InvalidColumnReference,
		"invalid column name: %s", n)
}

func newInvTableNameError(n fmt.Stringer) error {
	return pgerror.NewWithDepthf(1, pgcode.InvalidName,
		"invalid table name: %s", n)
}

func newSourceNotFoundError(fmt string, args ...interface{}) error {
	return pgerror.NewWithDepthf(1, pgcode.UndefinedTable, fmt, args...)
}

// CommonLookupFlags is the common set of flags for the various accessor interfaces.
type CommonLookupFlags struct {
	// if required is set, lookup will return an error if the item is not found.
	Required bool
	// if AvoidCached is set, lookup will avoid the cache (if any).
	AvoidCached bool
}

// DatabaseLookupFlags is the flag struct suitable for GetDatabaseDesc().
type DatabaseLookupFlags = CommonLookupFlags

// DatabaseListFlags is the flag struct suitable for GetObjectNames().
type DatabaseListFlags struct {
	CommonLookupFlags
	// ExplicitPrefix, when set, will cause the returned table names to
	// have an explicit schema and catalog part.
	ExplicitPrefix bool
}

// DesiredObjectKind represents what kind of object is desired in a name
// resolution attempt.
type DesiredObjectKind int

const (
	// TableObject is used when a table-like object is desired from resolution.
	TableObject DesiredObjectKind = iota
	// TypeObject is used when a type-like object is desired from resolution.
	TypeObject
)

// NewQualifiedObjectName returns an ObjectName of the corresponding kind.
// It is used mainly for constructing appropriate error messages depending
// on what kind of object was requested.
func NewQualifiedObjectName(catalog, schema, object string, kind DesiredObjectKind) ObjectName {
	switch kind {
	case TableObject:
		name := MakeTableNameWithSchema(Name(catalog), Name(schema), Name(object))
		return &name
	case TypeObject:
		name := MakeNewQualifiedTypeName(catalog, schema, object)
		return &name
	}
	return nil
}

// ObjectLookupFlags is the flag struct suitable for GetObjectDesc().
type ObjectLookupFlags struct {
	CommonLookupFlags
	// return a MutableTableDescriptor
	RequireMutable         bool
	IncludeOffline         bool
	AllowWithoutPrimaryKey bool
	// Control what type of object is being requested.
	DesiredObjectKind DesiredObjectKind
}

// ObjectLookupFlagsWithRequired returns a default ObjectLookupFlags object
// with just the Required flag true. This is a common configuration of the
// flags.
func ObjectLookupFlagsWithRequired() ObjectLookupFlags {
	return ObjectLookupFlags{
		CommonLookupFlags: CommonLookupFlags{Required: true},
	}
}
