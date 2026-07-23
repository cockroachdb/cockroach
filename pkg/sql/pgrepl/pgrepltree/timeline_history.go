// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgrepltree

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

type TimelineHistory struct {
	Timeline *tree.NumVal
}

var _ tree.Statement = (*TimelineHistory)(nil)

func (th *TimelineHistory) String() string {
	return tree.AsString(th)
}

func (th *TimelineHistory) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("TIMELINE_HISTORY ")
	ctx.FormatNode(th.Timeline)
}

func (th *TimelineHistory) StatementReturnType() tree.StatementReturnType {
	return tree.Replication
}

func (th *TimelineHistory) StatementType() tree.StatementType {
	return tree.TypeDDL
}

func (th *TimelineHistory) StatementTag() string {
	return "TIMELINE_HISTORY"
}

func (th *TimelineHistory) replicationStatement() {
}

var _ tree.Statement = (*TimelineHistory)(nil)
