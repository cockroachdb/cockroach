// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser2

import (
	"bytes"
	"fmt"
)

// Update represents an UPDATE statement.
type Update struct {
	Table TableExpr
	Exprs UpdateExprs
	Where *Where
}

func (node *Update) String() string {
	return fmt.Sprintf("UPDATE %v SET %v%v",
		node.Table, node.Exprs, node.Where)
}

// UpdateExprs represents a list of update expressions.
type UpdateExprs []*UpdateExpr

func (node UpdateExprs) String() string {
	var prefix string
	var buf bytes.Buffer
	for _, n := range node {
		fmt.Fprintf(&buf, "%s%v", prefix, n)
		prefix = ", "
	}
	return buf.String()
}

// UpdateExpr represents an update expression.
type UpdateExpr struct {
	Name QualifiedName
	Expr Expr
}

func (node *UpdateExpr) String() string {
	return fmt.Sprintf("%v = %v", node.Name, node.Expr)
}
