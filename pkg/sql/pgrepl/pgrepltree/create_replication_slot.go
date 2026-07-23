// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgrepltree

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type CreateReplicationSlot struct {
	Slot      tree.Name
	Temporary bool
	Kind      SlotKind
	Plugin    tree.Name
	Options   Options
}

var _ tree.Statement = (*CreateReplicationSlot)(nil)

func (crs *CreateReplicationSlot) String() string {
	return tree.AsString(crs)
}

func (crs *CreateReplicationSlot) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("CREATE_REPLICATION_SLOT ")
	ctx.FormatNode(&crs.Slot)
	if crs.Temporary {
		ctx.WriteString(" TEMPORARY")
	}
	switch crs.Kind {
	case PhysicalReplication:
		ctx.WriteString(" PHYSICAL")
	case LogicalReplication:
		ctx.WriteString(" LOGICAL ")
		ctx.FormatNode(&crs.Plugin)
	default:
		panic(fmt.Sprintf("unknown replication slot kind: %d", crs.Kind))
	}
	if len(crs.Options) > 0 {
		ctx.WriteString(" (")
		ctx.FormatNode(crs.Options)
		ctx.WriteString(")")
	}
}

func (crs *CreateReplicationSlot) StatementReturnType() tree.StatementReturnType {
	return tree.Replication
}

func (crs *CreateReplicationSlot) StatementType() tree.StatementType {
	return tree.TypeDDL
}

func (crs *CreateReplicationSlot) StatementTag() string {
	return "CREATE_REPLICATION_SLOT"
}

func (crs *CreateReplicationSlot) replicationStatement() {
}

var _ tree.Statement = (*CreateReplicationSlot)(nil)
