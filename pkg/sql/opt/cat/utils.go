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

package cat

import (
	"context"

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
