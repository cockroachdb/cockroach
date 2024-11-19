// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func (d *delegator) delegateShowCreateFunction(n *tree.ShowCreateRoutine) (tree.Statement, error) {
	// We don't need to filter by db since we don't allow cross-database
	// references.
	query := `
SELECT %[1]s, create_statement
FROM crdb_internal.%[2]s
WHERE schema_name = %[3]s
AND %[1]s = %[4]s
`
	resolvableFunctionReference := &n.Name
	un, ok := resolvableFunctionReference.FunctionReference.(*tree.UnresolvedName)
	if !ok {
		return nil, errors.AssertionFailedf("not a valid function name")
	}

	searchPath := &d.evalCtx.SessionData().SearchPath
	var fn *tree.ResolvedFunctionDefinition
	var err error
	if d.qualifyDataSourceNamesInAST {
		fn, err = resolvableFunctionReference.Resolve(d.ctx, searchPath, d.catalog)
	} else {
		// TODO(mgartner): We can probably make the distinction between the two
		// types of unresolved routine names at parsing-time (or shortly after),
		// rather than here. Ideally, the UnresolvedRoutineName interface can be
		// incorporated with ResolvableFunctionReference.
		if n.Procedure {
			fn, err = d.catalog.ResolveFunction(d.ctx, tree.MakeUnresolvedProcedureName(un), searchPath)
		} else {
			fn, err = d.catalog.ResolveFunction(d.ctx, tree.MakeUnresolvedFunctionName(un), searchPath)
		}
	}
	if err != nil {
		return nil, err
	}

	routineType := tree.UDFRoutine
	tab := "create_function_statements"
	nameCol := "function_name"
	if n.Procedure {
		routineType = tree.ProcedureRoutine
		tab = "create_procedure_statements"
		nameCol = "procedure_name"
	}

	var udfSchema string
	for _, o := range fn.Overloads {
		if o.Type == routineType {
			udfSchema = o.Schema
			break
		}
	}
	if udfSchema == "" {
		return nil, errors.Errorf("function %s does not exist", tree.AsString(un))
	}

	if d.qualifyDataSourceNamesInAST {
		referenceByName := resolvableFunctionReference.ReferenceByName
		if !referenceByName.HasExplicitSchema() {
			referenceByName.Parts[1] = udfSchema
		}
		if !referenceByName.HasExplicitCatalog() {
			referenceByName.Parts[2] = d.evalCtx.SessionData().Database
		}
		if referenceByName.NumParts < 3 {
			referenceByName.NumParts = 3
		}
	}

	fullQuery := fmt.Sprintf(query,
		nameCol, tab, lexbase.EscapeSQLString(udfSchema), lexbase.EscapeSQLString(un.Parts[0]))
	return d.parse(fullQuery)
}
