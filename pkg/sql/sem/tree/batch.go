// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import "github.com/cockroachdb/errors"

// Batch represents a BATCH clause.
type Batch struct {
	Params []BatchParam
}

var _ NodeFormatter = &Batch{}

// NormalizedBatch is a Batch with Batch.Params flattened into struct fields.
type NormalizedBatch struct {
	HasSize bool
	Size    Expr
}

// Normalize converts a Batch to a NormalizedBatch.
func (b *Batch) Normalize() (normalized NormalizedBatch, _ error) {
	for i, param := range b.Params {
		if err := param.Normalize(&normalized); err != nil {
			return normalized, errors.Wrapf(err, "normalizing param index %d", i)
		}
	}
	return normalized, nil
}

// BatchParam represents a BATCH (param) parameter.
type BatchParam interface {
	NodeFormatter
	Normalize(*NormalizedBatch) error
}

var _ BatchParam = &SizeBatchParam{}
var _ NodeFormatter = &SizeBatchParam{}

// SizeBatchParam represents a BATCH (SIZE size) parameter.
type SizeBatchParam struct {
	// Size is the expression specified by SIZE <size>.
	// It must be positive.
	Size Expr
}

// Format implements NodeFormatter.
func (p *SizeBatchParam) Format(ctx *FmtCtx) {
	ctx.WriteString("SIZE ")
	p.Size.Format(ctx)
}

// Normalize implements BatchParam.
func (p *SizeBatchParam) Normalize(batch *NormalizedBatch) error {
	if batch.HasSize {
		return errors.Newf("SIZE already specified")
	}
	batch.HasSize = true
	batch.Size = p.Size
	return nil
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
