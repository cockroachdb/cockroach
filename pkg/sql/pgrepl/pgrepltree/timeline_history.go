// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
