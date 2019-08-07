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

package exec

import (
	"bytes"
	"context"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types/conv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/pkg/errors"
)

// {{/*

type _GOTYPE interface{}
type _TYPE interface{}

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "types" package
var _ types.T

// Dummy import to pull in "bytes" package
var _ bytes.Buffer

func _ASSIGN_EQ(_, _, _ interface{}) uint64 {
	panic("")
}

// */}}

// Enum used to represent comparison results
type comparisonResult int

const (
	siTrue comparisonResult = iota
	siFalse
	siNull
)

func GetInProjectionOperator(
	ct *semtypes.T, input Operator, colIdx int, resultIdx int, datumTuple *tree.DTuple, negate bool,
) (Operator, error) {
	var err error
	switch t := conv.FromColumnType(ct); t {
	// {{range .}}
	case types._TYPE:
		obj := &projectInOp_TYPE{
			OneInputNode: NewOneInputNode(input),
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
	ct *semtypes.T, input Operator, colIdx int, datumTuple *tree.DTuple, negate bool,
) (Operator, error) {
	var err error
	switch t := conv.FromColumnType(ct); t {
	// {{range .}}
	case types._TYPE:
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
	colIdx    int
	outputIdx int
	filterRow []_GOTYPE
	hasNulls  bool
	negate    bool
}

var _ StaticMemoryOperator = &projectInOp_TYPE{}

func (p *projectInOp_TYPE) EstimateStaticMemoryUsage() int {
	return EstimateBatchSizeBytes([]types.T{types.Bool}, coldata.BatchSize)
}

func fillDatumRow_TYPE(ct *semtypes.T, datumTuple *tree.DTuple) ([]_GOTYPE, bool, error) {
	conv := conv.GetDatumToPhysicalFn(ct)
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
					if !nulls.NullAt(uint16(i)) && cmpIn_TYPE(col[i], si.filterRow, si.hasNulls) == compVal {
						sel[idx] = uint16(i)
						idx++
					}
				}
			} else {
				batch.SetSelection(true)
				sel := batch.Selection()
				col = col[:n]
				for i := range col {
					if !nulls.NullAt(uint16(i)) && cmpIn_TYPE(col[i], si.filterRow, si.hasNulls) == compVal {
						sel[idx] = uint16(i)
						idx++
					}
				}
			}
		} else {
			if sel := batch.Selection(); sel != nil {
				sel = sel[:n]
				for _, i := range sel {
					if cmpIn_TYPE(col[i], si.filterRow, si.hasNulls) == compVal {
						sel[idx] = uint16(i)
						idx++
					}
				}
			} else {
				batch.SetSelection(true)
				sel := batch.Selection()
				col = col[:n]
				for i := range col {
					if cmpIn_TYPE(col[i], si.filterRow, si.hasNulls) == compVal {
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
	if batch.Length() == 0 {
		return batch
	}

	if pi.outputIdx == batch.Width() {
		batch.AppendCol(types.Bool)
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
					cmpRes := cmpIn_TYPE(col[i], pi.filterRow, pi.hasNulls)
					if cmpRes == siNull {
						projNulls.SetNull(uint16(i))
					} else {
						projCol[i] = cmpRes == cmpVal
					}
				}
			}
		} else {
			col = col[:n]
			for i := range col {
				if nulls.NullAt(uint16(i)) {
					projNulls.SetNull(uint16(i))
				} else {
					cmpRes := cmpIn_TYPE(col[i], pi.filterRow, pi.hasNulls)
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
				cmpRes := cmpIn_TYPE(col[i], pi.filterRow, pi.hasNulls)
				if cmpRes == siNull {
					projNulls.SetNull(uint16(i))
				} else {
					projCol[i] = cmpRes == cmpVal
				}
			}
		} else {
			col = col[:n]
			for i := range col {
				cmpRes := cmpIn_TYPE(col[i], pi.filterRow, pi.hasNulls)
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
