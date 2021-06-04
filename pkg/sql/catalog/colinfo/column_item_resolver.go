// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colinfo

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	FindSourceMatchingName(
		ctx context.Context, tn tree.TableName,
	) (res NumResolutionResults, prefix *tree.TableName, srcMeta ColumnSourceMeta, err error)

	// FindSourceProvidingColumn searches for a data source providing
	// a column with the name given.
	//
	// This must error out with "ambiguous column name" if there is more
	// than one data source matching tn, "column not found" if there is
	// none. The srcMeta and colHints are subsequently passed to
	// Resolve() if resolution succeeds. The prefix will not be
	// modified.
	FindSourceProvidingColumn(
		ctx context.Context, col tree.Name,
	) (prefix *tree.TableName, srcMeta ColumnSourceMeta, colHint int, err error)

	// Resolve() is called if resolution succeeds.
	Resolve(
		ctx context.Context, prefix *tree.TableName, srcMeta ColumnSourceMeta, colHint int, col tree.Name,
	) (ColumnResolutionResult, error)
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

// ResolveAllColumnsSelector performs name resolution for a qualified star using a resolver.
func ResolveAllColumnsSelector(
	ctx context.Context, r ColumnItemResolver, a *tree.AllColumnsSelector,
) (srcName *tree.TableName, srcMeta ColumnSourceMeta, err error) {
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
		prefix.SchemaName = tree.PublicSchemaName
		res, srcName, srcMeta, err = r.FindSourceMatchingName(ctx, prefix)
		if err != nil {
			return nil, nil, err
		}
	}
	if res == NoResults {
		return nil, nil, pgerror.Newf(pgcode.UndefinedTable,
			"no data source matches pattern: %s", a)
	}
	return srcName, srcMeta, nil
}

// ResolveColumnItem performs name resolution for a column item using a resolver.
func ResolveColumnItem(
	ctx context.Context, r ColumnItemResolver, c *tree.ColumnItem,
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
		prefix.SchemaName = tree.PublicSchemaName
		res, srcName, srcMeta, err = r.FindSourceMatchingName(ctx, prefix)
		if err != nil {
			return nil, err
		}
	}
	if res == NoResults {
		return nil, pgerror.Newf(pgcode.UndefinedTable,
			"no data source matches prefix: %s in this context", c.TableName)
	}
	return r.Resolve(ctx, srcName, srcMeta, -1, colName)
}
