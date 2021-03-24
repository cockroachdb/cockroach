// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type ShowLogs struct {
	Nodes   []roachpb.NodeID
	Pattern Expr
	With    KVOptions
}

func (l *ShowLogs) Format(ctx *FmtCtx) {
	if l.Pattern == nil {
		ctx.WriteString("SHOW ALL LOGS")
	} else {
		ctx.WriteString("SHOW LOGS")

	}

	for i, n := range l.Nodes {
		if i == 0 {
			ctx.WriteString(" FROM NODES ")
		} else {
			ctx.WriteString(", ")
		}
		ctx.WriteString(strconv.Itoa(int(n)))
	}

	if l.Pattern != nil {
		ctx.WriteString(" MATCHING ")
		ctx.FormatNode(l.Pattern)
	}

	if l.With != nil {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&l.With)
	}
}

var _ NodeFormatter = (*ShowLogs)(nil)
var _ Statement = (*ShowLogs)(nil)
