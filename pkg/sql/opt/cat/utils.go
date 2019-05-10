// Copyright 2019 The Cockroach Authors.
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

package cat

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

// ExpandDataSourceGlob is a utility function that expands a tree.TablePattern
// into a list of object names.
func ExpandDataSourceGlob(
	ctx context.Context, catalog Catalog, flags Flags, pattern tree.TablePattern,
) ([]DataSourceName, error) {

	switch p := pattern.(type) {
	case *tree.TableName:
		_, name, err := catalog.ResolveDataSource(ctx, flags, p)
		if err != nil {
			return nil, err
		}
		return []DataSourceName{name}, nil

	case *tree.AllTablesSelector:
		schema, _, err := catalog.ResolveSchema(ctx, flags, &p.TableNamePrefix)
		if err != nil {
			return nil, err
		}

		return schema.GetDataSourceNames(ctx)

	default:
		return nil, errors.Errorf("invalid TablePattern type %T", p)
	}
}

// ResolveTableIndex resolves a TableIndexName.
func ResolveTableIndex(
	ctx context.Context, catalog Catalog, flags Flags, name *tree.TableIndexName,
) (Index, error) {
	if name.Table.TableName != "" {
		ds, _, err := catalog.ResolveDataSource(ctx, flags, &name.Table)
		if err != nil {
			return nil, err
		}
		table, ok := ds.(Table)
		if !ok {
			return nil, pgerror.Newf(
				pgerror.CodeWrongObjectTypeError, "%q is not a table", name.Table.TableName,
			)
		}
		if name.Index == "" {
			// Return primary index.
			return table.Index(0), nil
		}
		for i := 0; i < table.IndexCount(); i++ {
			if idx := table.Index(i); idx.Name() == tree.Name(name.Index) {
				return idx, nil
			}
		}
		return nil, pgerror.Newf(
			pgerror.CodeUndefinedObjectError, "index %q does not exist", name.Index,
		)
	}

	// We have to search for a table that has an index with the given name.
	schema, _, err := catalog.ResolveSchema(ctx, flags, &name.Table.TableNamePrefix)
	if err != nil {
		return nil, err
	}
	dsNames, err := schema.GetDataSourceNames(ctx)
	if err != nil {
		return nil, err
	}
	var found Index
	for i := range dsNames {
		ds, _, err := catalog.ResolveDataSource(ctx, flags, &dsNames[i])
		if err != nil {
			return nil, err
		}
		table, ok := ds.(Table)
		if !ok {
			// Not a table, ignore.
			continue
		}
		for i := 0; i < table.IndexCount(); i++ {
			if idx := table.Index(i); idx.Name() == tree.Name(name.Index) {
				if found != nil {
					return nil, pgerror.Newf(pgerror.CodeAmbiguousParameterError,
						"index name %q is ambiguous (found in %s and %s)",
						name.Index, table.Name().String(), found.Table().Name().String())
				}
				found = idx
				break
			}
		}
	}
	if found == nil {
		return nil, pgerror.Newf(
			pgerror.CodeUndefinedObjectError, "index %q does not exist", name.Index,
		)
	}
	return found, nil
}
