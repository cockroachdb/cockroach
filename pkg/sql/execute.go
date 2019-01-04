// Copyright 2017 The Cockroach Authors.
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

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// fillInPlaceholder helps with the EXECUTE foo(args) SQL statement: it takes in
// a prepared statement returning
// the referenced prepared statement and correctly updated placeholder info.
// See https://www.postgresql.org/docs/current/static/sql-execute.html for details.
func fillInPlaceholders(
	ps *PreparedStatement, name string, params tree.Exprs, searchPath sessiondata.SearchPath,
) (*tree.PlaceholderInfo, error) {
	if len(ps.TypeHints) != len(params) {
		return nil, pgerror.NewErrorf(pgerror.CodeSyntaxError,
			"wrong number of parameters for prepared statement %q: expected %d, got %d",
			name, len(ps.TypeHints), len(params))
	}

	qArgs := make(tree.QueryArguments, len(params))
	var semaCtx tree.SemaContext
	for i, e := range params {
		idx := types.PlaceholderIdx(i)

		typedExpr, err := sqlbase.SanitizeVarFreeExpr(
			e, ps.TypeHints[idx], "EXECUTE parameter", /* context */
			&semaCtx, nil /* evalCtx */, true /* allowImpure */)
		if err != nil {
			return nil, pgerror.NewError(pgerror.CodeWrongObjectTypeError, err.Error())
		}

		qArgs[idx] = typedExpr
	}
	return &tree.PlaceholderInfo{
			Values:    qArgs,
			TypeHints: ps.TypeHints,
			Types:     ps.Types,
		},
		nil
}
