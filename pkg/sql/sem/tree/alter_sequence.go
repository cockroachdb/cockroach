// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// AlterSequence represents an ALTER SEQUENCE statement, except in the case of
// ALTER SEQUENCE <seqName> RENAME TO <newSeqName>, which is represented by a
// RenameTable node.
type AlterSequence struct {
	IfExists bool
	Name     *UnresolvedObjectName
	Options  SequenceOptions
}

// Format implements the NodeFormatter interface.
func (node *AlterSequence) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER SEQUENCE ")
	if node.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(node.Name)
	ctx.FormatNode(&node.Options)
}
