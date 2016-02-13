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
)

// Update represents an UPDATE statement.
type Update struct {
	Table     TableExpr
	Exprs     UpdateExprs
	Where     *Where
	Returning ReturningExprs
}

func (node *Update) String() string {
	return fmt.Sprintf("UPDATE %s SET %s%s%s",
		node.Table, node.Exprs, node.Where, node.Returning)
}

// UpdateExprs represents a list of update expressions.
type UpdateExprs []*UpdateExpr

func (node UpdateExprs) String() string {
	var prefix string
	var buf bytes.Buffer
	for _, n := range node {
		fmt.Fprintf(&buf, "%s%s", prefix, n)
		prefix = ", "
	}
	return buf.String()
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
