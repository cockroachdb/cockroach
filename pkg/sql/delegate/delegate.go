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

package delegate

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// Certain statements (most SHOW variants) are just syntactic sugar for a more
// complicated underlying query.
//
// This package contains the logic to convert the AST of such a statement to the
// AST of the equivalent query to be planned.

// TryDelegate takes a statement and checks if it is one of the statements that
// can be rewritten as a lower level query. If it can, returns a new AST which
// is equivalent to the original statement. Otherwise, returns nil.
func TryDelegate(
	ctx context.Context, catalog cat.Catalog, evalCtx *tree.EvalContext, stmt tree.Statement,
) (tree.Statement, error) {
	d := delegator{
		ctx:     ctx,
		catalog: catalog,
		evalCtx: evalCtx,
	}
	switch t := stmt.(type) {
	case *tree.ShowAllClusterSettings:
		return d.delegateShowAllClusterSettings(t)

	case *tree.ShowDatabases:
		return d.delegateShowDatabases(t)

	case *tree.ShowCreate:
		return d.delegateShowCreate(t)

	case *tree.ShowIndex:
		return d.delegateShowIndex(t)

	case *tree.ShowColumns:
		return d.delegateShowColumns(t)

	case *tree.ShowConstraints:
		return d.delegateShowConstraints(t)

	case *tree.ShowGrants:
		return d.delegateShowGrants(t)

	case *tree.ShowJobs:
		return d.delegateShowJobs(t)

	case *tree.ShowQueries:
		return d.delegateShowQueries(t)

	case *tree.ShowRoleGrants:
		return d.delegateShowRoleGrants(t)

	case *tree.ShowSchemas:
		return d.delegateShowSchemas(t)

	case *tree.ShowSequences:
		return d.delegateShowSequences(t)

	case *tree.ShowSessions:
		return d.delegateShowSessions(t)

	case *tree.ShowSyntax:
		return d.delegateShowSyntax(t)

	case *tree.ShowUsers:
		return d.delegateShowUsers(t)

	case *tree.ShowVar:
		return d.delegateShowVar(t)

	case *tree.ShowTransactionStatus:
		return d.delegateShowVar(&tree.ShowVar{Name: "transaction_status"})

	default:
		return nil, nil
	}
}

type delegator struct {
	ctx     context.Context
	catalog cat.Catalog
	evalCtx *tree.EvalContext
}

func parse(sql string) (tree.Statement, error) {
	s, err := parser.ParseOne(sql)
	return s.AST, err
}
