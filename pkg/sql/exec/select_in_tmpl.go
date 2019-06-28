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
// This file is the execgen template for distinct.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package exec

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types/conv"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
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

func GetInOperator(
	ct *semtypes.T,
	input Operator,
	colIdx int,
	datumRow tree.Datum,
	negate bool,
) (Operator, error) {
	var err error
	switch t := conv.FromColumnType(ct); t {
	// {{range .}}
	case types._TYPE:
		obj := &selectInOp_TYPE{
			input:  input,
			colIdx: colIdx,
			negate: negate,
		}
		obj.filterRow, obj.filterRowNulls, obj.hasNulls, err = fillDatumRow_TYPE(ct, datumRow)
		if err != nil {
			return nil, err
		}
		return obj, nil
	// {{end}}
	default:
		return nil, errors.Errorf("unhandled type: %s", t)
	}
}

// {{define "equalityCheck"}}
_ASSIGN_EQ(cmp, target, val)
// {{end}}

// {{range .}}

type selectInOp_TYPE struct {
	input          Operator
	colIdx         int
	filterRow      []_GOTYPE
	filterRowNulls []bool
	hasNulls       bool
	negate         bool
}

func fillDatumRow_TYPE(ct *semtypes.T, row tree.Datum) ([]_GOTYPE, []bool, bool, error) {
	conv := conv.GetDatumToPhysicalFn(ct)
	datumTuple, ok := tree.AsDTuple(row)
	if !ok {
		return nil, nil, false, errors.New("not a constant datum row")
	}
	var result []_GOTYPE
	var resultNulls []bool
	hasNulls := false
	for _, d := range datumTuple.D {
		var converted _GOTYPE
		isNull := false
		if d == tree.DNull {
			isNull = true
			hasNulls = true
		} else {
			convRaw, err := conv(d)
			if err != nil {
				return nil, nil, false, err
			}
			converted = convRaw.(_GOTYPE)
		}
		result = append(result, converted)
		resultNulls = append(resultNulls, isNull)
	}
	return result, resultNulls, hasNulls, nil
}

func (si *selectInOp_TYPE) isIn_TYPE(col []_GOTYPE, sel []uint16, nulls *coldata.Nulls, i uint16, idx *uint16) {
	// if our row element is not in filter row
	// and at least one element in filter row is null
	// return null. else return false
	keep := false
	for fi, val := range si.filterRow {
		// if we have a null row, we keep it immediately
		if nulls.NullAt(i) {
			keep = true
			break
		}
		target := col[i]
		var cmp bool
		// {{template "equalityCheck" buildDict "Global" . }}
		// we found something if we aren't null at this index
		if !si.filterRowNulls[fi] && cmp {
			keep = true
			break
		}
	}
	if !keep && si.hasNulls {
		// write a null into the column
		nulls.SetNull(i)
		sel[*idx] = i
		*idx++
	} else if keep {
		sel[*idx] = i
		*idx++
	}
}

func (si *selectInOp_TYPE) isNotIn_TYPE(col []_GOTYPE, sel []uint16, nulls *coldata.Nulls, i uint16, idx *uint16) {
	// if there are no equal values and at least one null,
	// then we return null, otherwise we return true,
	// and return false if there is a match
	keep := true
	target := col[i]
	for fi, val := range si.filterRow {
		// if we have a null row, keep it immediately
		if nulls.NullAt(i) {
			keep = true
			break
		}
		var cmp bool
		// {{template "equalityCheck" buildDict "Global" . }}
		if !si.filterRowNulls[fi] && cmp {
			keep = false
			break
		}
	}
	if keep && si.hasNulls {
		// write a null into the column
		nulls.SetNull(i)
		sel[*idx] = i
		*idx++
	} else if keep {
		sel[*idx] = i
		*idx++
	}
}

func (si *selectInOp_TYPE) Init() {
	si.input.Init()
}

func (si *selectInOp_TYPE) Next(ctx context.Context) coldata.Batch {
	for {
		batch := si.input.Next(ctx)
		if batch.Length() == 0 {
			return batch
		}

		vec := batch.ColVec(si.colIdx)
		col := vec._TemplateType()[:coldata.BatchSize]
		var idx uint16
		n := batch.Length()
		nulls := vec.Nulls()
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			if si.negate {
				for _, i := range sel {
					si.isNotIn_TYPE(col, sel, nulls, i, &idx)
				}
			} else {
				for _, i := range sel {
					si.isIn_TYPE(col, sel, nulls, i, &idx)
				}
			}
		} else {
			batch.SetSelection(true)
			sel := batch.Selection()
			col = col[:n]
			if si.negate {
				for i := range col {
					si.isNotIn_TYPE(col, sel, nulls, uint16(i), &idx)
				}
			} else {
				for i := range col {
					si.isIn_TYPE(col, sel, nulls, uint16(i), &idx)
				}
			}
		}

		if idx > 0 {
			batch.SetLength(idx)
			return batch
		}
	}
}

// {{end}}
