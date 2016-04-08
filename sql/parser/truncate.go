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

// Truncate represents a TRUNCATE statement.
type Truncate struct {
	Tables       QualifiedNames
	DropBehavior DropBehavior
}

func (node *Truncate) String() string {
	var buf bytes.Buffer
	buf.WriteString("TRUNCATE TABLE ")
	for i, n := range node.Tables {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(n.String())
	}
	if node.DropBehavior != DropDefault {
		fmt.Fprintf(&buf, " %s", node.DropBehavior)
	}
	return buf.String()
}
