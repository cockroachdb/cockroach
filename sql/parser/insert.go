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

// Insert represents an INSERT statement.
type Insert struct {
	Table     *QualifiedName
	Columns   QualifiedNames
	Rows      *Select
	Returning ReturningExprs
}

func (node *Insert) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "INSERT INTO %s", node.Table)
	if node.Columns != nil {
		fmt.Fprintf(&buf, "(%s)", node.Columns)
	}
	if node.DefaultValues() {
		buf.WriteString(" DEFAULT VALUES")
	} else {
		fmt.Fprintf(&buf, " %s", node.Rows)
	}
	buf.WriteString(node.Returning.String())
	return buf.String()
}

// DefaultValues returns true iff only default values are being inserted.
func (node *Insert) DefaultValues() bool {
	return node.Rows.Select == nil
}
