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
