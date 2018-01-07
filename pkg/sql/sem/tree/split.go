// Copyright 2016 The Cockroach Authors.
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

// Split represents an `ALTER TABLE/INDEX .. SPLIT AT ..` statement.
type Split struct {
	// Only one of Table and Index can be set.
	Table *NormalizableTableName
	Index *TableNameWithIndex
	// Each row contains values for the columns in the PK or index (or a prefix
	// of the columns).
	Rows *Select
}

// Format implements the NodeFormatter interface.
func (node *Split) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER ")
	if node.Index != nil {
		ctx.WriteString("INDEX ")
		ctx.FormatNode(node.Index)
	} else {
		ctx.WriteString("TABLE ")
		ctx.FormatNode(node.Table)
	}
	ctx.WriteString(" SPLIT AT ")
	ctx.FormatNode(node.Rows)
}

// TestingRelocate represents an `ALTER TABLE/INDEX .. TESTING_RELOCATE ..`
// statement.
type TestingRelocate struct {
	// Only one of Table and Index can be set.
	Table *NormalizableTableName
	Index *TableNameWithIndex
	// Each row contains an array with store ids and values for the columns in the
	// PK or index (or a prefix of the columns).
	// See docs/RFCS/sql_split_syntax.md.
	Rows *Select
}

// Format implements the NodeFormatter interface.
func (node *TestingRelocate) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER ")
	if node.Index != nil {
		ctx.WriteString("INDEX ")
		ctx.FormatNode(node.Index)
	} else {
		ctx.WriteString("TABLE ")
		ctx.FormatNode(node.Table)
	}
	ctx.WriteString(" TESTING_RELOCATE ")
	ctx.FormatNode(node.Rows)
}

// Scatter represents an `ALTER TABLE/INDEX .. SCATTER ..`
// statement.
type Scatter struct {
	// Only one of Table and Index can be set.
	Table *NormalizableTableName
	Index *TableNameWithIndex
	// Optional from and to values for the columns in the PK or index (or a prefix
	// of the columns).
	// See docs/RFCS/sql_split_syntax.md.
	From, To Exprs
}

// Format implements the NodeFormatter interface.
func (node *Scatter) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER ")
	if node.Index != nil {
		ctx.WriteString("INDEX ")
		ctx.FormatNode(node.Index)
	} else {
		ctx.WriteString("TABLE ")
		ctx.FormatNode(node.Table)
	}
	ctx.WriteString(" SCATTER")
	if node.From != nil {
		ctx.WriteString(" FROM (")
		ctx.FormatNode(&node.From)
		ctx.WriteString(") TO (")
		ctx.FormatNode(&node.To)
		ctx.WriteString(")")
	}
}
