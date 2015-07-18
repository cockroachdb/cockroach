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

// Insert represents an INSERT statement.
type Insert struct {
	Table   QualifiedName
	Columns Columns
	Rows    SelectStatement
}

func (node *Insert) String() string {
	if node.Rows == nil {
		return fmt.Sprintf("INSERT INTO %s%s DEFAULT VALUES",
			node.Table, node.Columns)
	}
	return fmt.Sprintf("INSERT INTO %s%s %s",
		node.Table, node.Columns, node.Rows)
}

// Columns represents an insert column list. The syntax for Columns is a subset
// of SelectExprs. So, it's castable to a SelectExprs and can be analyzed as
// such.
type Columns []SelectExpr

func (node Columns) String() string {
	if node == nil {
		return ""
	}

	var prefix string
	var buf bytes.Buffer
	_, _ = buf.WriteString("(")
	for _, n := range node {
		fmt.Fprintf(&buf, "%s%s", prefix, n)
		prefix = ", "
	}
	_, _ = buf.WriteString(")")
	return buf.String()
}
