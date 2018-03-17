// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tree

import (
	"context"

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

// NormalizeTableName transforms an UnresolvedName into a TableName.
// This does not perform name resolution.
func NormalizeTableName(n *UnresolvedName) (res TableName, err error) {
	if n.NumParts < 1 || n.NumParts > 3 || n.Star {
		// The Star part of the condition is really an assertion. The
		// parser should not have let this star propagate to a point where
		// this method is called.
		return res, newInvTableNameError(n)
	}

	// Check that all the parts specified are not empty.
	// It's OK if the catalog name is empty.
	// We allow this in e.g. `select * from "".crdb_internal.tables`.
	lastCheck := n.NumParts
	if lastCheck > 2 {
		lastCheck = 2
	}
	for i := 0; i < lastCheck; i++ {
		if len(n.Parts[i]) == 0 {
			return res, newInvTableNameError(n)
		}
	}

	return makeTableNameFromUnresolvedName(n), nil
}

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
		return &AllTablesSelector{makeTableNamePrefixFromUnresolvedName(n)}, nil
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
		return nil, newInvColRef("invalid column name: %s", n)
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
			return nil, newInvColRef("invalid column name: %s", n)
		}
	}

	// Construct the result.
	tn := UnresolvedName{
		NumParts: n.NumParts - 1,
		Parts:    NameParts{n.Parts[1], n.Parts[2], n.Parts[3]},
	}
	if n.Star {
		return &AllColumnsSelector{tn}, nil
	}
	return &ColumnItem{TableName: tn, ColumnName: Name(n.Parts[0])}, nil
}

// Resolution algorithms follow.

const (
	// PublicSchema is the name of the physical schema in every
	// database/catalog.
	PublicSchema string = "public"
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

// Resolve performs name resolution for a column item using a resolver.
func (c *ColumnItem) Resolve(
	ctx context.Context, r ColumnItemResolver,
) (ColumnResolutionResult, error) {
	colName := c.ColumnName
	if c.TableName.NumParts == 0 {
		// Naked column name: simple case.
		srcName, srcMeta, cHint, err := r.FindSourceProvidingColumn(ctx, colName)
		if err != nil {
			return nil, err
		}
		return r.Resolve(ctx, srcName, srcMeta, cHint, colName)
	}

	// There is a prefix. We need to search for it.
	prefix := makeTableNameFromUnresolvedName(&c.TableName)

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
		return nil, newSourceNotFoundError("no data source matches prefix: %s", &c.TableName)
	}
	return r.Resolve(ctx, srcName, srcMeta, -1, colName)
}

// TableNameTargetResolver is the helper interface to resolve table
// names when the object is not expected to exist.
type TableNameTargetResolver interface {
	LookupSchema(ctx context.Context, dbName, scName string) (found bool, scMeta SchemaMeta, err error)
}

// SchemaMeta is an opaque reference returned by LookupSchema().
type SchemaMeta interface {
	// SchemaMeta is the interface anchor.
	SchemaMeta()
}

// TableNameExistingResolver is the helper interface to resolve table
// names when the object is expected to exist already.
type TableNameExistingResolver interface {
	LookupObject(ctx context.Context, dbName, scName, obName string) (found bool, objMeta NameResolutionResult, err error)
}

// NameResolutionResult is an opaque reference returned by LookupObject().
type NameResolutionResult interface {
	// NameResolutionResult is the interface anchor.
	NameResolutionResult()
}

// ResolveExisting performs name resolution for a table name when
// the target object is expected to exist already.
func (t *TableName) ResolveExisting(
	ctx context.Context, r TableNameExistingResolver, curDb string, searchPath sessiondata.SearchPath,
) (bool, NameResolutionResult, error) {
	if t.ExplicitSchema {
		if t.ExplicitCatalog {
			// Already 3 parts: nothing to search. Delegate to the resolver.
			return r.LookupObject(ctx, t.Catalog(), t.Schema(), t.Table())
		}
		// Two parts: D.T.
		// Try to use the current database, and be satisfied if it's sufficient to find the object.
		//
		// Note: we test this even if curDb == "", because CockroachDB
		// supports querying virtual schemas even when the current
		// database is not set. For example, `select * from
		// pg_catalog.pg_tables` is meant to show all tables across all
		// databases when there is no current database set.
		if found, objMeta, err := r.LookupObject(ctx, curDb, t.Schema(), t.Table()); found || err != nil {
			if err == nil {
				t.CatalogName = Name(curDb)
			}
			return found, objMeta, err
		}
		// No luck so far. Compatibility with CockroachDB v1.1: try D.public.T instead.
		if found, objMeta, err := r.LookupObject(ctx, t.Schema(), PublicSchema, t.Table()); found || err != nil {
			if err == nil {
				t.CatalogName = t.SchemaName
				t.SchemaName = PublicSchemaName
				t.ExplicitCatalog = true
			}
			return found, objMeta, err
		}
		// Welp, really haven't found anything.
		return false, nil, nil
	}

	// This is a naked table name. Use the search path.
	iter := searchPath.Iter()
	for next, ok := iter(); ok; next, ok = iter() {
		if found, objMeta, err := r.LookupObject(ctx, curDb, next, t.Table()); found || err != nil {
			if err == nil {
				t.CatalogName = Name(curDb)
				t.SchemaName = Name(next)
			}
			return found, objMeta, err
		}
	}
	return false, nil, nil
}

// ResolveTarget performs name resolution for a table name when
// the target object is not expected to exist already.
func (t *TableName) ResolveTarget(
	ctx context.Context, r TableNameTargetResolver, curDb string, searchPath sessiondata.SearchPath,
) (found bool, scMeta SchemaMeta, err error) {
	if t.ExplicitSchema {
		if t.ExplicitCatalog {
			// Already 3 parts: nothing to do.
			return r.LookupSchema(ctx, t.Catalog(), t.Schema())
		}
		// Two parts: D.T.
		// Try to use the current database, and be satisfied if it's sufficient to find the object.
		if found, scMeta, err = r.LookupSchema(ctx, curDb, t.Schema()); found || err != nil {
			if err == nil {
				t.CatalogName = Name(curDb)
			}
			return found, scMeta, err
		}
		// No luck so far. Compatibility with CockroachDB v1.1: use D.public.T instead.
		if found, scMeta, err = r.LookupSchema(ctx, t.Schema(), PublicSchema); found || err != nil {
			if err == nil {
				t.CatalogName = t.SchemaName
				t.SchemaName = PublicSchemaName
				t.ExplicitCatalog = true
			}
			return found, scMeta, err
		}
		// Welp, really haven't found anything.
		return false, nil, nil
	}

	// This is a naked table name. Use the current schema = the first item in the search path.
	hasFirst, firstSchema := searchPath.FirstSpecified()
	if hasFirst {
		if found, scMeta, err = r.LookupSchema(ctx, curDb, firstSchema); found || err != nil {
			if err == nil {
				t.CatalogName = Name(curDb)
				t.SchemaName = Name(firstSchema)
			}
		}
	}
	return found, scMeta, err
}

// Resolve is used for table prefixes. This is adequate for table
// patterns with stars, e.g. AllTablesSelector.
func (tp *TableNamePrefix) Resolve(
	ctx context.Context, r TableNameTargetResolver, curDb string, searchPath sessiondata.SearchPath,
) (found bool, scMeta SchemaMeta, err error) {
	if tp.ExplicitSchema {
		if tp.ExplicitCatalog {
			// Catalog name is explicit; nothing to do.
			return r.LookupSchema(ctx, tp.Catalog(), tp.Schema())
		}
		// Try with the current database. This may be empty, because
		// virtual schemas exist even when the db name is empty
		// (CockroachDB extension).
		if found, scMeta, err = r.LookupSchema(ctx, curDb, tp.Schema()); found || err != nil {
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
	// This is a naked table name. Use the current schema = the first item in the search path.
	hasFirst, firstSchema := searchPath.FirstSpecified()
	if hasFirst {
		if found, scMeta, err = r.LookupSchema(ctx, curDb, firstSchema); found || err != nil {
			if err == nil {
				tp.CatalogName = Name(curDb)
				tp.SchemaName = Name(firstSchema)
			}
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
		return nil, pgerror.NewErrorf(pgerror.CodeInvalidNameError,
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
			for alt, ok := iter(); ok; alt, ok = iter() {
				fullName = alt + "." + function
				if def, ok = FunDefs[fullName]; ok {
					found = true
					break
				}
			}
		}
		if !found {
			return nil, pgerror.NewErrorf(
				pgerror.CodeUndefinedFunctionError, "unknown function: %s()", ErrString(n))
		}
	}

	return def, nil
}

func newInvColRef(fmt string, n *UnresolvedName) error {
	return pgerror.NewErrorWithDepthf(1, pgerror.CodeInvalidColumnReferenceError, fmt, n)
}

func newInvTableNameError(n *UnresolvedName) error {
	return pgerror.NewErrorWithDepthf(1, pgerror.CodeInvalidNameError,
		"invalid table name: %s", n)
}

func newSourceNotFoundError(fmt string, args ...interface{}) error {
	return pgerror.NewErrorWithDepthf(1, pgerror.CodeUndefinedTableError, fmt, args...)
}
