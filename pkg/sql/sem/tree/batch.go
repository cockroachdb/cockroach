// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// Batch represents a BATCH clause.
type Batch struct {
	Params []BatchParam
}

var _ NodeFormatter = &Batch{}

type BatchParam interface {
	NodeFormatter
}

// SizeBatchParam represents a BATCH (SIZE size) parameter.
type SizeBatchParam struct {
	// Size is the expression specified by SIZE <size>.
	// It must be positive.
	Size Expr
}

// BatchParam represents a BATCH (param) parameter.
var _ BatchParam = &SizeBatchParam{}

// Format implements NodeFormatter.
func (p *SizeBatchParam) Format(ctx *FmtCtx) {
	ctx.WriteString("SIZE ")
	p.Size.Format(ctx)
}

// Format implements the NodeFormatter interface.
func (b *Batch) Format(ctx *FmtCtx) {
	if b == nil {
		return
	}
	ctx.WriteString("BATCH ")
	params := b.Params
	if len(params) > 0 {
		ctx.WriteString("(")
		for i, param := range params {
			if i > 0 {
				ctx.WriteString(",")
			}
			param.Format(ctx)
		}
		ctx.WriteString(") ")
	}
}
