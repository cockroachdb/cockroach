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

// ID is a custom type for {Database,Table}Descriptor IDs.
type ID uint32

// ColumnID is a custom type for ColumnDescriptor IDs.
type ColumnID uint32

// TableRef represents a numeric table reference.
// (Syntax !NNN in SQL.)
type TableRef struct {
	// TableID is the descriptor ID of the requested table.
	TableID int64

	// ColumnIDs is the list of column IDs requested in the table.
	// Note that a nil array here means "unspecified" (all columns)
	// whereas an array of length 0 means "zero columns".
	// Lists of zero columns are not supported and will throw an error.
	Columns []ColumnID

	// As determines the names that can be used in the surrounding query
	// to refer to this source.
	As AliasClause
}

// Format implements the NodeFormatter interface.
func (n *TableRef) Format(ctx *FmtCtx) {
	ctx.Printf("[%d", n.TableID)
	if n.Columns != nil {
		ctx.WriteByte('(')
		for i, c := range n.Columns {
			if i > 0 {
				ctx.WriteString(", ")
			}
			ctx.Printf("%d", c)
		}
		ctx.WriteByte(')')
	}
	if n.As.Alias != "" {
		ctx.WriteString(" AS ")
		ctx.FormatNode(&n.As)
	}
	ctx.WriteByte(']')
}
func (n *TableRef) String() string { return AsString(n) }

// tableExpr implements the TableExpr interface.
func (n *TableRef) tableExpr() {}
