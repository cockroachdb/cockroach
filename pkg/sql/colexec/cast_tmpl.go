// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// {{/*
// +build execgen_template
//
// This file is the execgen template for cast.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"context"
	"fmt"
	"math"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// {{/*

type _R_GO_TYPE interface{}

var _ apd.Decimal
var _ = math.MaxInt8
var _ tree.Datum

// _LEFT_CANONICAL_TYPE_FAMILY is the template variable.
const _LEFT_CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _LEFT_TYPE_WIDTH is the template variable.
const _LEFT_TYPE_WIDTH = 0

// _RIGHT_CANONICAL_TYPE_FAMILY is the template variable.
const _RIGHT_CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _RIGHT_TYPE_WIDTH is the template variable.
const _RIGHT_TYPE_WIDTH = 0

func _CAST(to, from, fromCol interface{}) {
	colexecerror.InternalError("")
}

// This will be replaced with execgen.UNSAFEGET.
func _L_UNSAFEGET(to, from interface{}) interface{} {
	colexecerror.InternalError("")
}

// This will be replaced with execgen.UNSAFEGET.
func _R_UNSAFEGET(to, from interface{}) interface{} {
	colexecerror.InternalError("")
}

// This will be replaced with execgen.SET.
func _R_SET(to, from interface{}) {
	colexecerror.InternalError("")
}

// This will be replaced with execgen.SLICE.
func _L_SLICE(col, i, j interface{}) interface{} {
	colexecerror.InternalError("")
}

// */}}

func cast(inputVec, outputVec coldata.Vec, n int, sel []int) {
	castPerformed := false
	switch inputVec.CanonicalTypeFamily() {
	// {{range .LeftFamilies}}
	case _LEFT_CANONICAL_TYPE_FAMILY:
		switch inputVec.Type().Width() {
		// {{range .LeftWidths}}
		case _LEFT_TYPE_WIDTH:
			switch outputVec.CanonicalTypeFamily() {
			// {{range .RightFamilies}}
			case _RIGHT_CANONICAL_TYPE_FAMILY:
				switch outputVec.Type().Width() {
				// {{range .RightWidths}}
				case _RIGHT_TYPE_WIDTH:
					inputCol := inputVec._L_TYP()
					outputCol := outputVec._R_TYP()
					if inputVec.MaybeHasNulls() {
						inputNulls := inputVec.Nulls()
						outputNulls := outputVec.Nulls()
						if sel != nil {
							sel = sel[:n]
							for _, i := range sel {
								if inputNulls.NullAt(i) {
									outputNulls.SetNull(i)
								} else {
									v := _L_UNSAFEGET(inputCol, i)
									var r _R_GO_TYPE
									_CAST(r, v, inputCol)
									_R_SET(outputCol, i, r)
								}
							}
						} else {
							// Remove bounds checks for inputCol[i] and outputCol[i].
							inputCol = _L_SLICE(inputCol, 0, n)
							_ = _L_UNSAFEGET(inputCol, n-1)
							_ = _R_UNSAFEGET(outputCol, n-1)
							for i := 0; i < n; i++ {
								if inputNulls.NullAt(i) {
									outputNulls.SetNull(i)
								} else {
									v := _L_UNSAFEGET(inputCol, i)
									var r _R_GO_TYPE
									_CAST(r, v, inputCol)
									_R_SET(outputCol, i, r)
								}
							}
						}
					} else {
						if sel != nil {
							sel = sel[:n]
							for _, i := range sel {
								v := _L_UNSAFEGET(inputCol, i)
								var r _R_GO_TYPE
								_CAST(r, v, inputCol)
								_R_SET(outputCol, i, r)
							}
						} else {
							// Remove bounds checks for inputCol[i] and outputCol[i].
							inputCol = _L_SLICE(inputCol, 0, n)
							_ = _L_UNSAFEGET(inputCol, n-1)
							_ = _R_UNSAFEGET(outputCol, n-1)
							for i := 0; i < n; i++ {
								v := _L_UNSAFEGET(inputCol, i)
								var r _R_GO_TYPE
								_CAST(r, v, inputCol)
								_R_SET(outputCol, i, r)
							}
						}
					}
					castPerformed = true
					// {{end}}
				}
				// {{end}}
			}
			// {{end}}
		}
		// {{end}}
	}
	if !castPerformed {
		colexecerror.InternalError(fmt.Sprintf("unhandled cast %s -> %s", inputVec.Type(), outputVec.Type()))
	}
}

func GetCastOperator(
	allocator *colmem.Allocator,
	input colexecbase.Operator,
	colIdx int,
	resultIdx int,
	fromType *types.T,
	toType *types.T,
) (colexecbase.Operator, error) {
	input = newVectorTypeEnforcer(allocator, input, toType, resultIdx)
	if fromType.Family() == types.UnknownFamily {
		return &castOpNullAny{
			OneInputNode: NewOneInputNode(input),
			allocator:    allocator,
			colIdx:       colIdx,
			outputIdx:    resultIdx,
		}, nil
	}
	leftType, rightType := fromType, toType
	switch typeconv.TypeFamilyToCanonicalTypeFamily(leftType.Family()) {
	// {{range .LeftFamilies}}
	case _LEFT_CANONICAL_TYPE_FAMILY:
		switch leftType.Width() {
		// {{range .LeftWidths}}
		case _LEFT_TYPE_WIDTH:
			switch typeconv.TypeFamilyToCanonicalTypeFamily(rightType.Family()) {
			// {{range .RightFamilies}}
			case _RIGHT_CANONICAL_TYPE_FAMILY:
				switch rightType.Width() {
				// {{range .RightWidths}}
				case _RIGHT_TYPE_WIDTH:
					return &castOp{
						OneInputNode: NewOneInputNode(input),
						allocator:    allocator,
						colIdx:       colIdx,
						outputIdx:    resultIdx,
					}, nil
					// {{end}}
				}
				// {{end}}
			}
			// {{end}}
		}
		// {{end}}
	}
	return nil, errors.Errorf("unhandled cast %s -> %s", fromType, toType)
}

type castOpNullAny struct {
	OneInputNode
	allocator *colmem.Allocator
	colIdx    int
	outputIdx int
}

var _ colexecbase.Operator = &castOpNullAny{}

func (c *castOpNullAny) Init() {
	c.input.Init()
}

func (c *castOpNullAny) Next(ctx context.Context) coldata.Batch {
	batch := c.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	vec := batch.ColVec(c.colIdx)
	projVec := batch.ColVec(c.outputIdx)
	vecNulls := vec.Nulls()
	projNulls := projVec.Nulls()
	if projVec.MaybeHasNulls() {
		// We need to make sure that there are no left over nulls values in the
		// output vector.
		projNulls.UnsetNulls()
	}
	if sel := batch.Selection(); sel != nil {
		sel = sel[:n]
		for _, i := range sel {
			if vecNulls.NullAt(i) {
				projNulls.SetNull(i)
			} else {
				colexecerror.InternalError(errors.Errorf("unexpected non-null at index %d", i))
			}
		}
	} else {
		for i := 0; i < n; i++ {
			if vecNulls.NullAt(i) {
				projNulls.SetNull(i)
			} else {
				colexecerror.InternalError(fmt.Errorf("unexpected non-null at index %d", i))
			}
		}
	}
	return batch
}

type castOp struct {
	OneInputNode
	allocator *colmem.Allocator
	colIdx    int
	outputIdx int
}

var _ colexecbase.Operator = &castOp{}

func (c *castOp) Init() {
	c.input.Init()
}

func (c *castOp) Next(ctx context.Context) coldata.Batch {
	batch := c.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	vec := batch.ColVec(c.colIdx)
	projVec := batch.ColVec(c.outputIdx)
	if projVec.MaybeHasNulls() {
		// We need to make sure that there are no left over null values in the
		// output vector.
		projVec.Nulls().UnsetNulls()
	}
	c.allocator.PerformOperation(
		[]coldata.Vec{projVec}, func() { cast(vec, projVec, n, batch.Selection()) },
	)
	return batch
}
