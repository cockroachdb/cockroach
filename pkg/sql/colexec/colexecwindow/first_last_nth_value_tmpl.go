// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// {{/*
//go:build execgen_template
// +build execgen_template

//
// This file is the execgen template for first_value.eg.go, last_value.eg.go,
// and nth_value.eg.go. It's formatted in a special way, so it's both valid Go
// and a valid text/template input. This permits editing this file with editor
// support.
//
// */}}

package colexecwindow

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// {{/*

// Declarations to make the template compile properly.

// _CANONICAL_TYPE_FAMILY is the template variable.
const _CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _TYPE_WIDTH is the template variable.
const _TYPE_WIDTH = 0

// */}}

// New_UPPERCASE_NAMEOperator creates a new Operator that computes window
// function _OP_NAME. outputColIdx specifies in which coldata.Vec the operator
// should put its output (if there is no such column, a new column is appended).
func New_UPPERCASE_NAMEOperator(
	args *WindowArgs,
	frame *execinfrapb.WindowerSpec_Frame,
	ordering *execinfrapb.Ordering,
	argIdxs []int,
) (colexecop.Operator, error) {
	framer := newWindowFramer(args.EvalCtx, frame, ordering, args.InputTypes, args.PeersColIdx)
	colsToStore := framer.getColsToStore([]int{argIdxs[0]})

	// Allow the direct-access buffer 10% of the available memory. The rest will
	// be given to the bufferedWindowOp queue. While it is somewhat more important
	// for the direct-access buffer tuples to be kept in-memory, it only has to
	// store a single column. TODO(drewk): play around with benchmarks to find a
	// good empirically-supported fraction to use.
	bufferMemLimit := int64(float64(args.MemoryLimit) * 0.10)
	mainMemLimit := args.MemoryLimit - bufferMemLimit
	buffer := colexecutils.NewSpillingBuffer(
		args.BufferAllocator, bufferMemLimit, args.QueueCfg,
		args.FdSemaphore, args.InputTypes, args.DiskAcc, colsToStore...)
	base := _OP_NAMEBase{
		partitionSeekerBase: partitionSeekerBase{
			buffer:          buffer,
			partitionColIdx: args.PartitionColIdx,
		},
		framer:       framer,
		outputColIdx: args.OutputColIdx,
		bufferArgIdx: 0, // The arg column is the first column in the buffer.
	}
	argType := args.InputTypes[argIdxs[0]]
	switch typeconv.TypeFamilyToCanonicalTypeFamily(argType.Family()) {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch argType.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			windower := &_OP_NAME_TYPEWindow{_OP_NAMEBase: base}
			// {{if .IsNthValue}}
			windower.nColIdx = argIdxs[1]
			// {{end}}
			return newBufferedWindowOperator(args, windower, argType, mainMemLimit), nil
			// {{end}}
		}
		// {{end}}
	}
	return nil, errors.Errorf("unsupported _OP_NAME window operator type %s", argType.Name())
}

type _OP_NAMEBase struct {
	partitionSeekerBase
	colexecop.CloserHelper
	framer windowFramer

	outputColIdx int
	bufferArgIdx int
}

// {{range .}}
// {{range .WidthOverloads}}

type _OP_NAME_TYPEWindow struct {
	_OP_NAMEBase
	// {{if .IsNthValue}}
	nColIdx int
	// {{end}}
}

var _ bufferedWindower = &_OP_NAME_TYPEWindow{}

// processBatch implements the bufferedWindower interface.
func (w *_OP_NAME_TYPEWindow) processBatch(batch coldata.Batch, startIdx, endIdx int) {
	if startIdx >= endIdx {
		// No processing needs to be done for this portion of the current partition.
		return
	}
	outputVec := batch.ColVec(w.outputColIdx)
	outputCol := outputVec.TemplateType()
	outputNulls := outputVec.Nulls()
	// {{if .Sliceable}}
	_, _ = outputCol.Get(startIdx), outputCol.Get(endIdx-1)
	// {{end}}

	// {{if .IsNthValue}}
	nVec := batch.ColVec(w.nColIdx)
	nCol := nVec.Int64()
	nNulls := nVec.Nulls()
	_, _ = nCol[startIdx], nCol[endIdx-1]
	// {{end}}

	for i := startIdx; i < endIdx; i++ {
		w.framer.next(w.Ctx)
		// {{if .IsFirstValue}}
		requestedIdx := w.framer.frameFirstIdx()
		// {{else if .IsLastValue}}
		requestedIdx := w.framer.frameLastIdx()
		// {{else}}
		if nNulls.MaybeHasNulls() && nNulls.NullAt(i) {
			// TODO(drewk): this could be pulled out of the loop, but for now keep the
			// templating simple.
			outputNulls.SetNull(i)
			continue
		}
		// gcassert:bce
		nVal := int(nCol[i])
		if nVal <= 0 {
			colexecerror.ExpectedError(builtins.ErrInvalidArgumentForNthValue)
		}
		requestedIdx := w.framer.frameNthIdx(nVal)
		// {{end}}
		if requestedIdx == -1 {
			// The requested row does not exist.
			outputNulls.SetNull(i)
			continue
		}

		vec, idx, _ := w.buffer.GetVecWithTuple(w.Ctx, w.bufferArgIdx, requestedIdx)
		if vec.Nulls().MaybeHasNulls() && vec.Nulls().NullAt(idx) {
			outputNulls.SetNull(i)
			continue
		}
		col := vec.TemplateType()
		val := col.Get(idx)
		// {{if .Sliceable}}
		//gcassert:bce
		// {{end}}
		outputCol.Set(i, val)
	}
}

// {{end}}
// {{end}}

// transitionToProcessing implements the bufferedWindower interface.
func (b *_OP_NAMEBase) transitionToProcessing() {
	b.framer.startPartition(b.Ctx, b.partitionSize, b.buffer)
}

// startNewPartition implements the bufferedWindower interface.
func (b *_OP_NAMEBase) startNewPartition() {
	b.partitionSize = 0
	b.buffer.Reset(b.Ctx)
}

// Init implements the bufferedWindower interface.
func (b *_OP_NAMEBase) Init(ctx context.Context) {
	if !b.InitHelper.Init(ctx) {
		return
	}
}

// Close implements the bufferedWindower interface.
func (b *_OP_NAMEBase) Close(ctx context.Context) {
	if !b.CloserHelper.Close() {
		return
	}
	b.buffer.Close(ctx)
}
