// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgrepltree

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

type IdentifySystem struct {
}

var _ tree.Statement = (*IdentifySystem)(nil)

func (i *IdentifySystem) String() string {
	return tree.AsString(i)
}

func (i *IdentifySystem) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("IDENTIFY_SYSTEM")
}

func (i *IdentifySystem) StatementReturnType() tree.StatementReturnType {
	return tree.Rows
}

func (i *IdentifySystem) StatementType() tree.StatementType {
	return tree.TypeDML
}

func (i *IdentifySystem) StatementTag() string {
	return "IDENTIFY_SYSTEM"
}

func (i *IdentifySystem) replicationStatement() {
}

var _ tree.Statement = (*IdentifySystem)(nil)
