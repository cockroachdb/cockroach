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

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type SlotKind int

const (
	PhysicalReplication SlotKind = iota + 1
	LogicalReplication
)

type LSN struct {
	Hi uint32
	Lo uint32
}

func (lsn LSN) String() string {
	return tree.AsString(&lsn)
}

func (lsn LSN) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(fmt.Sprintf("%X/%X", lsn.Hi, lsn.Lo))
}
