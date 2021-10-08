// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// StreamIngestion represents a RESTORE FROM REPLICATION STREAM statement.
type StreamIngestion struct {
	Targets TargetList
	From    StringOrPlaceholderOptList
	AsOf    AsOfClause
}

var _ Statement = &StreamIngestion{}

// Format implements the NodeFormatter interface.
func (node *StreamIngestion) Format(ctx *FmtCtx) {
	ctx.WriteString("RESTORE ")
	ctx.FormatNode(&node.Targets)
	ctx.WriteString(" ")
	ctx.WriteString("FROM REPLICATION STREAM FROM ")
	ctx.FormatNode(&node.From)
	if node.AsOf.Expr != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(&node.AsOf)
	}
}
