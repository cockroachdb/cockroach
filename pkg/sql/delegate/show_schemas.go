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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// delegateShowSchemas implements SHOW SCHEMAS which returns all the schemas in
// the given or current database.
// Privileges: None.
func (d *delegator) delegateShowSchemas(n *tree.ShowSchemas) (tree.Statement, error) {
	name, err := d.getSpecifiedOrCurrentDatabase(n.Database)
	if err != nil {
		return nil, err
	}
	getSchemasQuery := fmt.Sprintf(`
			SELECT schema_name
			FROM %[1]s.information_schema.schemata
			WHERE catalog_name = %[2]s
			ORDER BY schema_name`,
		name.String(), // note: (tree.Name).String() != string(name)
		lex.EscapeSQLString(string(name)),
	)

	return parse(getSchemasQuery)
}

// getSpecifiedOrCurrentDatabase returns the name of the specified database, or
// of the current database if the specified name is empty.
//
// Returns an error if there is no current database, or if the specified
// database doesn't exist.
func (d *delegator) getSpecifiedOrCurrentDatabase(specifiedDB tree.Name) (tree.Name, error) {
	name := cat.SchemaName{
		CatalogName:     specifiedDB,
		SchemaName:      tree.Name(tree.PublicSchema),
		ExplicitCatalog: true,
		ExplicitSchema:  true,
	}
	if name.CatalogName == "" {
		name.CatalogName = tree.Name(d.evalCtx.SessionData.Database)
		if name.CatalogName == "" {
			return "", pgerror.NewError(pgerror.CodeInvalidNameError, "no database specified")
		}
	}

	flags := cat.Flags{AvoidDescriptorCaches: true}
	if _, _, err := d.catalog.ResolveSchema(d.ctx, flags, &name); err != nil {
		return "", err
	}
	return name.CatalogName, nil
}
