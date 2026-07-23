// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgrepltree

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgrepl/lsn"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type StartReplication struct {
	Slot      tree.Name
	Temporary bool
	Kind      SlotKind
	LSN       lsn.LSN
	Options   Options
	Timeline  *tree.NumVal
}

var _ tree.Statement = (*StartReplication)(nil)

func (srs *StartReplication) String() string {
	return tree.AsString(srs)
}

func (srs *StartReplication) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("START_REPLICATION")
	if srs.Slot != "" {
		ctx.WriteString(" SLOT ")
		ctx.FormatNode(&srs.Slot)
	}
	switch srs.Kind {
	case PhysicalReplication:
		ctx.WriteString(" PHYSICAL ")
	case LogicalReplication:
		ctx.WriteString(" LOGICAL ")
	default:
		panic(fmt.Sprintf("unknown replication slot kind: %d", srs.Kind))
	}
	ctx.WriteString(srs.LSN.String())
	if srs.Timeline != nil {
		ctx.WriteString(" TIMELINE ")
		ctx.FormatNode(srs.Timeline)
	}
	if len(srs.Options) > 0 {
		ctx.WriteString(" (")
		ctx.FormatNode(srs.Options)
		ctx.WriteString(")")
	}
}

func (srs *StartReplication) StatementReturnType() tree.StatementReturnType {
	return tree.Replication
}

func (srs *StartReplication) StatementType() tree.StatementType {
	return tree.TypeDDL
}

func (srs *StartReplication) StatementTag() string {
	return "START_REPLICATION"
}

func (srs *StartReplication) replicationStatement() {
}

var _ tree.Statement = (*StartReplication)(nil)
