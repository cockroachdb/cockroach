// Copyright 2017 The Cockroach Authors.
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

package tree

import (
	"bytes"
)

// With represents a WITH statement.
type With struct {
	CTEList []*CTE
}

// CTE represents a common table expression inside of a WITH clause.
type CTE struct {
	Name AliasClause
	Stmt Statement
}

// Format implements the NodeFormatter interface.
func (node *With) Format(buf *bytes.Buffer, f FmtFlags) {
	if node == nil {
		return
	}
	buf.WriteString("WITH ")
	for i, cte := range node.CTEList {
		if i != 0 {
			buf.WriteString(", ")
		}
		cte.Name.Format(buf, f)
		buf.WriteString(" AS (")
		cte.Stmt.Format(buf, f)
		buf.WriteString(") ")
	}
}
