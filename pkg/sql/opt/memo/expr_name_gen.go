// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memo

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

// ExprNameGenerator is used to generate a unique name for each relational
// expression in a query tree. See GenerateName for details.
type ExprNameGenerator struct {
	prefix    string
	exprCount int
}

// NewExprNameGenerator creates a new instance of ExprNameGenerator,
// initialized with the given prefix.
func NewExprNameGenerator(prefix string) *ExprNameGenerator {
	return &ExprNameGenerator{prefix: prefix}
}

// GenerateName generates a name for a relational expression with the given
// operator. It is used to generate names for each relational expression
// in a query tree, corresponding to the tables that will be created if the
// session variable `save_tables_prefix` is non-empty.
//
// Each invocation of GenerateName is guaranteed to produce a unique name for
// a given instance of ExprNameGenerator. This works because each name is
// appended with a unique, auto-incrementing number. For readability, the
// generated names also contain a common prefix and the name of the relational
// operator separated with underscores. For example: my_query_scan_2.
//
// Since the names are generated with an auto-incrementing number, the order
// of invocation is important. For a given query, the number assigned to each
// relational subexpression corresponds to the order in which the expression
// was encountered during tree traversal. Thus, in order to generate a
// consistent name, always call GenerateName in a pre-order traversal of the
// expression tree.
//
func (g *ExprNameGenerator) GenerateName(op opt.Operator) string {
	// Replace all instances of "-" in the operator name with "_" in order to
	// create a legal table name.
	operator := strings.Replace(op.String(), "-", "_", -1)
	g.exprCount++
	return fmt.Sprintf("%s_%s_%d", g.prefix, operator, g.exprCount)
}

// ColumnNameGenerator is used to generate a unique name for each column of a
// relational expression. See GenerateName for details.
type ColumnNameGenerator struct {
	e    RelExpr
	pres physical.Presentation
	seen map[string]int
}

// NewColumnNameGenerator creates a new instance of ColumnNameGenerator,
// initialized with the given relational expression.
func NewColumnNameGenerator(e RelExpr) *ColumnNameGenerator {
	return &ColumnNameGenerator{
		e:    e,
		pres: e.RequiredPhysical().Presentation,
		seen: make(map[string]int, e.Relational().OutputCols.Len()),
	}
}

// GenerateName generates a unique name for each column in a relational
// expression. This function is used to generate consistent, unique names
// for the columns in the table that will be created if the session
// variable `save_tables_prefix` is non-empty.
func (g *ColumnNameGenerator) GenerateName(col opt.ColumnID) string {
	colMeta := g.e.Memo().Metadata().ColumnMeta(col)
	colName := colMeta.Alias

	// Check whether the presentation has a different name for this column, and
	// use it if available.
	for i := range g.pres {
		if g.pres[i].ID == col {
			colName = g.pres[i].Alias
			break
		}
	}

	// Every column name must be unique.
	if cnt, ok := g.seen[colName]; ok {
		g.seen[colName]++
		colName = fmt.Sprintf("%s_%d", colName, cnt)
	} else {
		g.seen[colName] = 1
	}

	return colName
}
