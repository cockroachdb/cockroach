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

type BaseBackup struct {
	Options Options
}

var _ tree.Statement = (*BaseBackup)(nil)

func (bb *BaseBackup) String() string {
	return tree.AsString(bb)
}

func (bb *BaseBackup) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("BASE_BACKUP")
	if len(bb.Options) > 0 {
		ctx.WriteString(" (")
		ctx.FormatNode(bb.Options)
		ctx.WriteString(")")
	}
}

func (bb *BaseBackup) StatementReturnType() tree.StatementReturnType {
	return tree.Replication
}

func (bb *BaseBackup) StatementType() tree.StatementType {
	return tree.TypeDDL
}

func (bb *BaseBackup) StatementTag() string {
	return "BASE_BACKUP"
}

func (bb *BaseBackup) replicationStatement() {
}

var _ tree.Statement = (*BaseBackup)(nil)
