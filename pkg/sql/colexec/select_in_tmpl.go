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
// This file is the execgen template for select_in.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"bytes"
	"context"
	"math"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	// {{/*
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	// */}}
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/pkg/errors"
)

// {{/*

type _GOTYPE interface{}
type _TYPE interface{}

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "time" package.
var _ time.Time

// Dummy import to pull in "coltypes" package
var _ coltypes.T

// Dummy import to pull in "bytes" package
var _ bytes.Buffer

// Dummy import to pull in "math" package.
var _ = math.MaxInt64

func _ASSIGN_EQ(_, _, _ interface{}) uint64 {
	execerror.VectorizedInternalPanic("")
}

// */}}

// Use execgen package to remove unused import warning.
var _ interface{} = execgen.UNSAFEGET

// Enum used to represent comparison results
type comparisonResult int

const (
	siTrue comparisonResult = iota
	siFalse
	siNull
)

func GetInProjectionOperator(
	allocator *Allocator,
	ct *types.T,
	input Operator,
	colIdx int,
	resultIdx int,
	datumTuple *tree.DTuple,
	negate bool,
) (Operator, error) {
	var err error
	switch t := typeconv.FromColumnType(ct); t {
	// {{range .}}
	case coltypes._TYPE:
		obj := &projectInOp_TYPE{
			OneInputNode: NewOneInputNode(input),
			allocator:    allocator,
			colIdx:       colIdx,
			outputIdx:    resultIdx,
			negate:       negate,
		}
		obj.filterRow, obj.hasNulls, err = fillDatumRow_TYPE(ct, datumTuple)
		if err != nil {
			return nil, err
		}
		return obj, nil
	// {{end}}
	default:
		return nil, errors.Errorf("unhandled type: %s", t)
	}
}

func GetInOperator(
	ct *types.T, input Operator, colIdx int, datumTuple *tree.DTuple, negate bool,
) (Operator, error) {
	var err error
	switch t := typeconv.FromColumnType(ct); t {
	// {{range .}}
	case coltypes._TYPE:
		obj := &selectInOp_TYPE{
			OneInputNode: NewOneInputNode(input),
			colIdx:       colIdx,
			negate:       negate,
		}
		obj.filterRow, obj.hasNulls, err = fillDatumRow_TYPE(ct, datumTuple)
		if err != nil {
			return nil, err
		}
		return obj, nil
	// {{end}}
	default:
		return nil, errors.Errorf("unhandled type: %s", t)
	}
}

// {{range .}}

type selectInOp_TYPE struct {
	OneInputNode
	colIdx    int
	filterRow []_GOTYPE
	hasNulls  bool
	negate    bool
}

type projectInOp_TYPE struct {
	OneInputNode
	allocator *Allocator
	colIdx    int
	outputIdx int
	filterRow []_GOTYPE
	hasNulls  bool
	negate    bool
}

var _ Operator = &projectInOp_TYPE{}

func fillDatumRow_TYPE(ct *types.T, datumTuple *tree.DTuple) ([]_GOTYPE, bool, error) {
	conv := typeconv.GetDatumToPhysicalFn(ct)
	var result []_GOTYPE
	hasNulls := false
	for _, d := range datumTuple.D {
		if d == tree.DNull {
			hasNulls = true
		} else {
			convRaw, err := conv(d)
			if err != nil {
				return nil, false, err
			}
			converted := convRaw.(_GOTYPE)
			result = append(result, converted)
		}
	}
	return result, hasNulls, nil
}

func cmpIn_TYPE(target _GOTYPE, filterRow []_GOTYPE, hasNulls bool) comparisonResult {
	for i := range filterRow {
		var cmp bool
		_ASSIGN_EQ(cmp, target, filterRow[i])
		if cmp {
			return siTrue
		}
	}
	if hasNulls {
		return siNull
	} else {
		return siFalse
	}
}

func (si *selectInOp_TYPE) Init() {
	si.input.Init()
}

func (pi *projectInOp_TYPE) Init() {
	pi.input.Init()
}

func (si *selectInOp_TYPE) Next(ctx context.Context) coldata.Batch {
	for {
		batch := si.input.Next(ctx)
		if batch.Length() == 0 {
			return batch
		}

		vec := batch.ColVec(si.colIdx)
		col := vec._TemplateType()
		var idx uint16
		n := batch.Length()

		compVal := siTrue
		if si.negate {
			compVal = siFalse
		}

		if vec.MaybeHasNulls() {
			nulls := vec.Nulls()
			if sel := batch.Selection(); sel != nil {
				sel = sel[:n]
				for _, i := range sel {
					v := execgen.UNSAFEGET(col, int(i))
					if !nulls.NullAt(uint16(i)) && cmpIn_TYPE(v, si.filterRow, si.hasNulls) == compVal {
						sel[idx] = uint16(i)
						idx++
					}
				}
			} else {
				batch.SetSelection(true)
				sel := batch.Selection()
				col = execgen.SLICE(col, 0, int(n))
				for execgen.RANGE(i, col) {
					v := execgen.UNSAFEGET(col, i)
					if !nulls.NullAt(uint16(i)) && cmpIn_TYPE(v, si.filterRow, si.hasNulls) == compVal {
						sel[idx] = uint16(i)
						idx++
					}
				}
			}
		} else {
			if sel := batch.Selection(); sel != nil {
				sel = sel[:n]
				for _, i := range sel {
					v := execgen.UNSAFEGET(col, int(i))
					if cmpIn_TYPE(v, si.filterRow, si.hasNulls) == compVal {
						sel[idx] = uint16(i)
						idx++
					}
				}
			} else {
				batch.SetSelection(true)
				sel := batch.Selection()
				col = execgen.SLICE(col, 0, int(n))
				for execgen.RANGE(i, col) {
					v := execgen.UNSAFEGET(col, i)
					if cmpIn_TYPE(v, si.filterRow, si.hasNulls) == compVal {
						sel[idx] = uint16(i)
						idx++
					}
				}
			}
		}

		if idx > 0 {
			batch.SetLength(idx)
			return batch
		}
	}
}

func (pi *projectInOp_TYPE) Next(ctx context.Context) coldata.Batch {
	batch := pi.input.Next(ctx)
	if pi.outputIdx == batch.Width() {
		pi.allocator.AppendColumn(batch, coltypes.Bool)
	}
	if batch.Length() == 0 {
		return batch
	}

	vec := batch.ColVec(pi.colIdx)
	col := vec._TemplateType()

	projVec := batch.ColVec(pi.outputIdx)
	projCol := projVec.Bool()
	projNulls := projVec.Nulls()

	n := batch.Length()

	cmpVal := siTrue
	if pi.negate {
		cmpVal = siFalse
	}

	if vec.MaybeHasNulls() {
		nulls := vec.Nulls()
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			for _, i := range sel {
				if nulls.NullAt(uint16(i)) {
					projNulls.SetNull(uint16(i))
				} else {
					v := execgen.UNSAFEGET(col, int(i))
					cmpRes := cmpIn_TYPE(v, pi.filterRow, pi.hasNulls)
					if cmpRes == siNull {
						projNulls.SetNull(uint16(i))
					} else {
						projCol[i] = cmpRes == cmpVal
					}
				}
			}
		} else {
			col = execgen.SLICE(col, 0, int(n))
			for execgen.RANGE(i, col) {
				if nulls.NullAt(uint16(i)) {
					projNulls.SetNull(uint16(i))
				} else {
					v := execgen.UNSAFEGET(col, i)
					cmpRes := cmpIn_TYPE(v, pi.filterRow, pi.hasNulls)
					if cmpRes == siNull {
						projNulls.SetNull(uint16(i))
					} else {
						projCol[i] = cmpRes == cmpVal
					}
				}
			}
		}
	} else {
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			for _, i := range sel {
				v := execgen.UNSAFEGET(col, int(i))
				cmpRes := cmpIn_TYPE(v, pi.filterRow, pi.hasNulls)
				if cmpRes == siNull {
					projNulls.SetNull(uint16(i))
				} else {
					projCol[i] = cmpRes == cmpVal
				}
			}
		} else {
			col = execgen.SLICE(col, 0, int(n))
			for execgen.RANGE(i, col) {
				v := execgen.UNSAFEGET(col, i)
				cmpRes := cmpIn_TYPE(v, pi.filterRow, pi.hasNulls)
				if cmpRes == siNull {
					projNulls.SetNull(uint16(i))
				} else {
					projCol[i] = cmpRes == cmpVal
				}
			}
		}
	}
	return batch
}

// {{end}}
