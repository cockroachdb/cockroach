// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func (d *delegator) delegateShowCreateFunction(n *tree.ShowCreateFunction) (tree.Statement, error) {
	// We don't need to filter by db since we don't allow cross-database
	// references.
	query := `
SELECT function_name, create_statement
FROM crdb_internal.create_function_statements
WHERE schema_name = %[1]s
AND function_name = %[2]s
`
	un, ok := n.Name.FunctionReference.(*tree.UnresolvedName)
	if !ok {
		return nil, errors.AssertionFailedf("not a valid function name")
	}

	fn, err := d.catalog.ResolveFunction(d.ctx, un, &d.evalCtx.SessionData().SearchPath)
	if err != nil {
		return nil, err
	}

	var udfSchema string
	for _, o := range fn.Overloads {
		if o.IsUDF {
			udfSchema = o.Schema
		}
	}
	if udfSchema == "" {
		return nil, errors.Errorf("function %s does not exist", tree.AsString(un))
	}

	fullQuery := fmt.Sprintf(query, lexbase.EscapeSQLString(udfSchema), lexbase.EscapeSQLString(un.Parts[0]))
	return d.parse(fullQuery)
}
