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
	// Construct an equivalent SELECT query that produces the results:
	//
	// SELECT @1 AS field, @2 AS text
	//   FROM (VALUES
	//           ('file',     'foo.go'),
	//           ('line',     '123'),
	//           ('function', 'blix()'),
	//           ('detail',   'some details'),
	//           ('hint',     'some hints'))
	//
	var query bytes.Buffer
	query.WriteString("SELECT @1 AS field, @2 AS text FROM (VALUES ")

	comma := ""
	if err := runShowSyntax(ctx, n.Statement, func(ctx context.Context, field, msg string) error {
		fmt.Fprintf(&query, "%s('%s', ", comma, field)
		lex.EncodeSQLString(&query, msg)
		query.WriteByte(')')
		comma = ", "
		return nil
	}); err != nil {
		return nil, err
	}
	query.WriteByte(')')
	return p.delegateQuery(ctx, "SHOW SYNTAX", query.String(), nil, nil)
}

func runShowSyntax(
	ctx context.Context, stmt string, report func(ctx context.Context, field, msg string) error,
) error {
	stmts, err := parser.Parse(stmt)
	if err != nil {
		pqErr, ok := pgerror.GetPGCause(err)
		if !ok {
			return pgerror.NewErrorf(pgerror.CodeInternalError, "unknown parser error: %v", err)
		}
		if err := report(ctx, "error", pqErr.Message); err != nil {
			return err
		}
		if err := report(ctx, "code", pqErr.Code); err != nil {
			return err
		}
		if pqErr.Source != nil {
			if pqErr.Source.File != "" {
				if err := report(ctx, "file", pqErr.Source.File); err != nil {
					return err
				}
			}
			if pqErr.Source.Line > 0 {
				if err := report(ctx, "line", fmt.Sprintf("%d", pqErr.Source.Line)); err != nil {
					return err
				}
			}
			if pqErr.Source.Function != "" {
				if err := report(ctx, "function", pqErr.Source.Function); err != nil {
					return err
				}
			}
		}
		if pqErr.Detail != "" {
			if err := report(ctx, "detail", pqErr.Detail); err != nil {
				return err
			}
		}
		if pqErr.Hint != "" {
			if err := report(ctx, "hint", pqErr.Hint); err != nil {
				return err
			}
		}
	} else {
		for _, stmt := range stmts {
			if err := report(ctx, "sql", tree.AsStringWithFlags(stmt, tree.FmtParsable)); err != nil {
				return err
			}
		}
	}
	return nil
}
