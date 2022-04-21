// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdceval

import (
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// AsStringUnredacted returns unredacted string representation.
func AsStringUnredacted(n tree.NodeFormatter) string {
	return tree.AsStringWithFlags(n, tree.FmtParsable|tree.FmtShowPasswords)
}

// ParseChangefeedExpression is a helper to parse changefeed "select clause".
func ParseChangefeedExpression(selectClause string) (*tree.SelectClause, error) {
	stmt, err := parser.ParseOne(selectClause)
	if err != nil {
		return nil, err
	}

	return stmt.AST.(*tree.Select).Select.(*tree.SelectClause), nil
}
