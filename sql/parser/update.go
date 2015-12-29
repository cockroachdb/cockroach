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

import (
	"bytes"
	"fmt"
	"strings"
)

// Update represents an UPDATE statement.
type Update struct {
	Table TableExpr
	Exprs UpdateExprs
	Where *Where
}

func (node *Update) String() string {
	var buf bytes.Buffer
	buf.WriteString("UPDATE " + node.Table.String() + " SET " + node.Exprs.String())
	if node.Where != nil {
		buf.WriteString(" " + node.Where.String())
	}
	return buf.String()
}

// UpdateExprs represents a list of update expressions.
type UpdateExprs []*UpdateExpr

func (node UpdateExprs) String() string {
	strs := make([]string, len(node))
	for i := range node {
		strs[i] = node[i].String()
	}

	return strings.Join(strs, ", ")
}

// UpdateExpr represents an update expression.
type UpdateExpr struct {
	Tuple bool
	Names QualifiedNames
	Expr  Expr
}

func (node *UpdateExpr) String() string {
	open, close := "", ""
	if node.Tuple {
		open, close = "(", ")"
	}
	return fmt.Sprintf("%s%s%s = %s", open, node.Names, close, node.Expr)
}
