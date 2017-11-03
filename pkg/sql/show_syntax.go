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
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ShowSyntax implements the plan for SHOW SYNTAX. This statement is
// usually handled as a special case in Executor, but for
// FROM [SHOW SYNTAX ...] we will arrive here too.
func (p *planner) ShowSyntax(ctx context.Context, n *tree.ShowSyntax) (planNode, error) {
	var query bytes.Buffer
	query.WriteString("SELECT @1 AS field, @2 AS text FROM (VALUES ")

	stmts, err := parser.Parse(n.Statement)
	if err != nil {
		pqErr, _ := pgerror.GetPGCause(err)
		fmt.Fprintf(&query, "('error', %s), ('code', %s)",
			lex.EscapeSQLString(pqErr.Message),
			lex.EscapeSQLString(pqErr.Code))
		if pqErr.Source != nil {
			if pqErr.Source.File != "" {
				fmt.Fprintf(&query, ", ('file', %s)", lex.EscapeSQLString(pqErr.Source.File))
			}
			if pqErr.Source.Line > 0 {
				fmt.Fprintf(&query, ", ('line', '%d')", pqErr.Source.Line)
			}
			if pqErr.Source.Function != "" {
				fmt.Fprintf(&query, ", ('function', %s)", lex.EscapeSQLString(pqErr.Source.Function))
			}
		}
		if pqErr.Detail != "" {
			fmt.Fprintf(&query, ", ('detail', %s)", lex.EscapeSQLString(pqErr.Detail))
		}
		if pqErr.Hint != "" {
			fmt.Fprintf(&query, ", ('hint', %s)", lex.EscapeSQLString(pqErr.Hint))
		}
	} else {
		for i, stmt := range stmts {
			if i > 0 {
				query.WriteString(", ")
			}
			fmt.Fprintf(&query, "('sql', %s)",
				lex.EscapeSQLString(tree.AsStringWithFlags(stmt, tree.FmtParsable)))
		}
	}
	query.WriteByte(')')

	return p.delegateQuery(ctx, "SHOW SYNTAX", query.String(), nil, nil)
}
