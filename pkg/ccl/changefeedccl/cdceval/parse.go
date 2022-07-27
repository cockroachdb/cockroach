// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdceval

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// AsStringUnredacted returns unredacted string representation.
// Method should be used when serializing node formatter to be stored in
// protocol messages.
func AsStringUnredacted(n tree.NodeFormatter) string {
	return tree.AsStringWithFlags(n, tree.FmtParsable|tree.FmtShowPasswords)
}

// ParseChangefeedExpression is a helper to parse changefeed "select clause",
// and perform minimal validation checks.
func ParseChangefeedExpression(selectClause string) (*tree.SelectClause, error) {
	sc, err := parseChangefeedExpression(selectClause)
	if err != nil {
		return nil, changefeedbase.WithTerminalError(err)
	}
	return sc, nil
}

func parseChangefeedExpression(selectClause string) (*tree.SelectClause, error) {
	stmt, err := parser.ParseOne(selectClause)
	if err != nil {
		return nil, err
	}
	if slct, ok := stmt.AST.(*tree.Select); ok {
		if sc, ok := slct.Select.(*tree.SelectClause); ok {
			// This really shouldn't happen as it's enforced by sql.y.
			if len(sc.From.Tables) != 1 {
				return nil, pgerror.Newf(pgcode.Syntax, "expected 1 table, found %d", len(sc.From.Tables))
			}
			return sc, nil
		}
	}

	return nil, errors.AssertionFailedf("expected select clause, found %T", stmt.AST)
}
