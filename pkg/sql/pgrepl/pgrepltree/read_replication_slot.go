// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgrepltree

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

type ReadReplicationSlot struct {
	Slot tree.Name
}

var _ tree.Statement = (*ReadReplicationSlot)(nil)

func (rrs *ReadReplicationSlot) String() string {
	return tree.AsString(rrs)
}

func (rrs *ReadReplicationSlot) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("READ_REPLICATION_SLOT ")
	ctx.FormatNode(&rrs.Slot)
}

func (rrs *ReadReplicationSlot) StatementReturnType() tree.StatementReturnType {
	return tree.Replication
}

func (rrs *ReadReplicationSlot) StatementType() tree.StatementType {
	return tree.TypeDDL
}

func (rrs *ReadReplicationSlot) StatementTag() string {
	return "READ_REPLICATION_SLOT"
}

func (rrs *ReadReplicationSlot) replicationStatement() {
}

var _ tree.Statement = (*ReadReplicationSlot)(nil)
