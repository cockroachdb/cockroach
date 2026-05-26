// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgrepltree

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

type DropReplicationSlot struct {
	Slot tree.Name
	Wait bool
}

var _ tree.Statement = (*DropReplicationSlot)(nil)

func (drs *DropReplicationSlot) String() string {
	return tree.AsString(drs)
}

func (drs *DropReplicationSlot) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("DROP_REPLICATION_SLOT ")
	ctx.FormatNode(&drs.Slot)
	if drs.Wait {
		ctx.WriteString(" WAIT")
	}
}

func (drs *DropReplicationSlot) StatementReturnType() tree.StatementReturnType {
	return tree.Replication
}

func (drs *DropReplicationSlot) StatementType() tree.StatementType {
	return tree.TypeDDL
}

func (drs *DropReplicationSlot) StatementTag() string {
	return "DROP_REPLICATION_SLOT"
}

func (drs *DropReplicationSlot) replicationStatement() {
}

var _ tree.Statement = (*DropReplicationSlot)(nil)
