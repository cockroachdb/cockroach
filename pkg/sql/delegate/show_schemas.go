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

package delegate

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// delegateShowSchemas implements SHOW SCHEMAS which returns all the schemas in
// the given or current database.
// Privileges: None.
func delegateShowSchemas(
	ctx context.Context, catalog cat.Catalog, evalCtx *tree.EvalContext, n *tree.ShowSchemas,
) (tree.Statement, error) {
	name := cat.SchemaName{
		SchemaName:      tree.Name(tree.PublicSchema),
		ExplicitCatalog: true,
		ExplicitSchema:  true,
	}
	if n.Database != "" {
		name.CatalogName = n.Database
	} else {
		name.CatalogName = tree.Name(evalCtx.SessionData.Database)
		if name.CatalogName == "" {
			return nil, pgerror.NewError(pgerror.CodeInvalidNameError, "no database specified")
		}
	}

	flags := cat.Flags{AvoidDescriptorCaches: true}
	if _, _, err := catalog.ResolveSchema(ctx, flags, &name); err != nil {
		return nil, err
	}

	getSchemasQuery := fmt.Sprintf(`
			SELECT schema_name
			FROM %[1]s.information_schema.schemata
			WHERE catalog_name = %[2]s
			ORDER BY schema_name`,
		name.CatalogName.String(), // note: CatalogName.String() != Catalog()
		lex.EscapeSQLString(name.Catalog()),
	)

	return parse(getSchemasQuery)
}
