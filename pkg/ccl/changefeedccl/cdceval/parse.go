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
	"github.com/cockroachdb/errors"
)

// AsStringUnredacted returns unredacted string representation.
// Method is intended to be used when serializing node formatter to be stored
// in protocol messages.
func AsStringUnredacted(n tree.NodeFormatter) string {
	return tree.AsStringWithFlags(n, tree.FmtParsable|tree.FmtShowPasswords)
}

// ParseChangefeedExpression is a helper to parse changefeed "select clause".
func ParseChangefeedExpression(selectClause string) (*tree.SelectClause, error) {
	stmt, err := parser.ParseOne(selectClause)
	if err != nil {
		return nil, err
	}
	if slct, ok := stmt.AST.(*tree.Select); ok {
		if sc, ok := slct.Select.(*tree.SelectClause); ok {
			return sc, nil
		}
	}
	return nil, errors.AssertionFailedf("expected select clause, found %T", stmt.AST)
}

// tableNameOrAlias returns tree.TableName for the table expression.
func tableNameOrAlias(name string, expr tree.TableExpr) *tree.TableName {
	switch t := expr.(type) {
	case *tree.AliasedTableExpr:
		return tree.NewUnqualifiedTableName(t.As.Alias)
	case *tree.TableRef:
		return tree.NewUnqualifiedTableName(t.As.Alias)
	}
	return tree.NewUnqualifiedTableName(tree.Name(name))
}
