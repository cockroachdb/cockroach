// Copyright 2018 The Cockroach Authors.
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

package opt

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

type tableName string
type columnName string
type columnMap = util.FastIntMap
type columnIndex = int

// columnProps holds properties about each column in a relational expression.
type columnProps struct {
	// name is the column name.
	name columnName

	// table is the name of the table.
	table tableName

	// typ contains the datum type held by the column.
	typ types.T

	// index is the index for this column, which is unique across all the
	// columns in the expression.
	index columnIndex
}

func (c columnProps) String() string {
	if c.name == "" {
		return fmt.Sprintf("@%d", c.index+1)
	}
	if c.table == "" {
		return tree.NameString(string(c.name))
	}
	return fmt.Sprintf("%s.%s",
		tree.NameString(string(c.table)), tree.NameString(string(c.name)))
}

var _ tree.Expr = &columnProps{}

// Format is part of the tree.Expr interface.
func (c *columnProps) Format(ctx *tree.FmtCtx) {
	ctx.Printf("@%d", c.index+1)
}

// Walk is part of the tree.Expr interface.
func (c *columnProps) Walk(v tree.Visitor) tree.Expr {
	return c
}

// TypeCheck is part of the tree.Expr interface.
func (c *columnProps) TypeCheck(_ *tree.SemaContext, desired types.T) (tree.TypedExpr, error) {
	return c, nil
}

// ResolvedType is part of the tree.TypedExpr interface.
func (c *columnProps) ResolvedType() types.T {
	return c.typ
}

// Variable is part of the tree.VariableExpr interface. This prevents the
// column from being evaluated during normalization.
func (*columnProps) Variable() {}

// Eval is part of the tree.TypedExpr interface.
func (*columnProps) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	panic(fmt.Errorf("columnProps must be replaced before evaluation"))
}
