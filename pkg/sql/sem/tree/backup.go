// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// Backup represents a BACKUP statement.
type Backup struct {
	Targets         TargetList
	To              PartitionedBackup
	IncrementalFrom Exprs
	AsOf            AsOfClause
	Options         KVOptions
}

var _ Statement = &Backup{}

// Format implements the NodeFormatter interface.
func (node *Backup) Format(ctx *FmtCtx) {
	ctx.WriteString("BACKUP ")
	ctx.FormatNode(&node.Targets)
	ctx.WriteString(" TO ")
	ctx.FormatNode(&node.To)
	if node.AsOf.Expr != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(&node.AsOf)
	}
	if node.IncrementalFrom != nil {
		ctx.WriteString(" INCREMENTAL FROM ")
		ctx.FormatNode(&node.IncrementalFrom)
	}
	if node.Options != nil {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
	}
}

// Restore represents a RESTORE statement.
type Restore struct {
	Targets TargetList
	From    []PartitionedBackup
	AsOf    AsOfClause
	Options KVOptions
}

var _ Statement = &Restore{}

// Format implements the NodeFormatter interface.
func (node *Restore) Format(ctx *FmtCtx) {
	ctx.WriteString("RESTORE ")
	ctx.FormatNode(&node.Targets)
	ctx.WriteString(" FROM ")
	for i := range node.From {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&node.From[i])
	}
	if node.AsOf.Expr != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(&node.AsOf)
	}
	if node.Options != nil {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.Options)
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
func (o *KVOptions) Format(ctx *FmtCtx) {
	for i := range *o {
		n := &(*o)[i]
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&n.Key)
		if n.Value != nil {
			ctx.WriteString(` = `)
			ctx.FormatNode(n.Value)
		}
	}
}

// PartitionedBackup is a list of destination URIs for a single BACKUP. A single
// URI corresponds to the special case of a regular backup, and multiple URIs
// correspond to a partitioned backup whose locality configuration is
// specified by LOCALITY url params.
type PartitionedBackup []Expr

// Format implements the NodeFormatter interface.
func (node *PartitionedBackup) Format(ctx *FmtCtx) {
	if len(*node) > 1 {
		ctx.WriteString("(")
	}
	ctx.FormatNode((*Exprs)(node))
	if len(*node) > 1 {
		ctx.WriteString(")")
	}
}
