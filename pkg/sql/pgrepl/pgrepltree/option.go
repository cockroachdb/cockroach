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

type Options []Option

func (ol Options) Format(ctx *tree.FmtCtx) {
	for i, o := range ol {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(o)
	}
}

type Option struct {
	Key   tree.Name
	Value tree.Expr
}

func (o Option) Format(ctx *tree.FmtCtx) {
	ctx.FormatNode(&o.Key)
	if o.Value != nil {
		ctx.WriteRune(' ')
		ctx.FormatNode(o.Value)
	}
}
