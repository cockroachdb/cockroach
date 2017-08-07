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

package parser

import "bytes"

// Backup represents a BACKUP statement.
type Backup struct {
	Targets         TargetList
	To              Expr
	IncrementalFrom Exprs
	AsOf            AsOfClause
	Options         KVOptions
}

var _ Statement = &Backup{}

// Format implements the NodeFormatter interface.
func (node *Backup) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("BACKUP ")
	FormatNode(buf, f, node.Targets)
	buf.WriteString(" TO ")
	FormatNode(buf, f, node.To)
	if node.AsOf.Expr != nil {
		buf.WriteString(" ")
		FormatNode(buf, f, node.AsOf)
	}
	if node.IncrementalFrom != nil {
		buf.WriteString(" INCREMENTAL FROM ")
		FormatNode(buf, f, node.IncrementalFrom)
	}
	if node.Options != nil {
		buf.WriteString(" WITH ")
		FormatNode(buf, f, node.Options)
	}
}

// Restore represents a RESTORE statement.
type Restore struct {
	Targets TargetList
	From    Exprs
	AsOf    AsOfClause
	Options KVOptions
}

var _ Statement = &Restore{}

// Format implements the NodeFormatter interface.
func (node *Restore) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("RESTORE ")
	FormatNode(buf, f, node.Targets)
	buf.WriteString(" FROM ")
	FormatNode(buf, f, node.From)
	if node.AsOf.Expr != nil {
		buf.WriteString(" ")
		FormatNode(buf, f, node.AsOf)
	}
	if node.Options != nil {
		buf.WriteString(" WITH ")
		FormatNode(buf, f, node.Options)
	}
}

// KVOption is a key-value option.
type KVOption struct {
	Key   Name
	Value Expr
}

// KVOptions is a list of KVOptions.
type KVOptions []KVOption

// Format implements the NodeFormatter interface.
func (o KVOptions) Format(buf *bytes.Buffer, f FmtFlags) {
	for i, n := range o {
		if i > 0 {
			buf.WriteString(", ")
		}
		FormatNode(buf, f, n.Key)
		if n.Value != nil {
			buf.WriteString(` = `)
			FormatNode(buf, f, n.Value)
		}
	}
}
