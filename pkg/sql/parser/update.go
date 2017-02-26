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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

// This code was derived from https://github.com/youtube/vitess.
//
// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package parser

import "bytes"

// Update represents an UPDATE statement.
type Update struct {
	Table     TableExpr
	Exprs     UpdateExprs
	Where     *Where
	Returning ReturningClause
}

// Format implements the NodeFormatter interface.
func (node *Update) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("UPDATE ")
	FormatNode(buf, f, node.Table)
	buf.WriteString(" SET ")
	FormatNode(buf, f, node.Exprs)
	FormatNode(buf, f, node.Where)
	FormatNode(buf, f, node.Returning)
}

// UpdateExprs represents a list of update expressions.
type UpdateExprs []*UpdateExpr

// Format implements the NodeFormatter interface.
func (node UpdateExprs) Format(buf *bytes.Buffer, f FmtFlags) {
	for i, n := range node {
		if i > 0 {
			buf.WriteString(", ")
		}
		FormatNode(buf, f, n)
	}
}

// UpdateExpr represents an update expression.
type UpdateExpr struct {
	Tuple bool
	Names UnresolvedNames
	Expr  Expr
}

// Format implements the NodeFormatter interface.
func (node *UpdateExpr) Format(buf *bytes.Buffer, f FmtFlags) {
	open, close := "", ""
	if node.Tuple {
		open, close = "(", ")"
	}
	buf.WriteString(open)
	FormatNode(buf, f, node.Names)
	buf.WriteString(close)
	buf.WriteString(" = ")
	FormatNode(buf, f, node.Expr)
}
