// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package funcdesc

import (
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ParseUserDefinedFuncDef takes a string, the user-defined function's
// definition, and parses it. It performs validation, ensuring that the
// definition contains only things that the user-defined function implementation
// can handle.
// It returns the fully-parsed statement, as well as the render expression in
// the SELECT (which is currently the only thing handled by UDFs).
func ParseUserDefinedFuncDef(funcDef string) (tree.Statement, tree.Expr, error) {
	funcAst, err := parser.ParseOne(funcDef)
	if err != nil {
		return nil, nil, pgerror.Wrapf(err, pgcode.Syntax, "invalid function definition: %s", funcDef)
	}
	s, ok := funcAst.AST.(*tree.Select)
	if !ok {
		return nil, nil, pgerror.Newf(pgcode.Syntax, "only SELECTs are allowed as UDFs")
	}

	if s.Limit != nil || s.OrderBy != nil || s.Locking != nil || s.With != nil {
		return nil, nil, pgerror.Newf(pgcode.Syntax, "only simple scalar SELECTs are allowed as UDFs")
	}
	switch t := s.Select.(type) {
	case *tree.SelectClause:
		if len(t.Exprs) != 1 {
			return nil, nil, pgerror.New(pgcode.Syntax, "only single-projection scalar SELECTs are allowed as UDFs")
		}
		if t.Distinct || t.TableSelect || t.From.Tables != nil || t.DistinctOn != nil || t.GroupBy != nil ||
			t.Having != nil || t.Window != nil || t.Where != nil {
			return nil, nil, pgerror.Newf(pgcode.Syntax, "only simple scalar SELECTs are allowed as UDFs")
		}
		return funcAst.AST, t.Exprs[0].Expr, nil
	default:
		return nil, nil, pgerror.Newf(pgcode.Syntax, "only simple scalar SELECTs are allowed as UDFs")
	}
}
