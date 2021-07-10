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
// +build execgen_template
//
// This file is the execgen template for lag.eg.go and lead.eg.go. It's
// formatted in a special way, so it's both valid Go and a valid text/template
// input. This permits editing this file with editor support.
//
// */}}

package colexecwindow

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
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
	unlimitedAllocator *colmem.Allocator,
	bufferAllocator *colmem.Allocator,
	memoryLimit int64,
	diskQueueCfg colcontainer.DiskQueueCfg,
	fdSemaphore semaphore.Semaphore,
	diskAcc *mon.BoundAccount,
	input colexecop.Operator,
	inputTypes []*types.T,
	outputColIdx int,
	partitionColIdx int,
	argIdx int,
	offsetIdx int,
	defaultIdx int,
) (colexecop.Operator, error) {
	// Allow the direct-access buffer 10% of the available memory. The rest will
	// be given to the bufferedWindowOp queue. While it is somewhat more important
	// for the direct-access buffer tuples to be kept in-memory, it only has to
	// store a single column. TODO(drewk): play around with benchmarks to find a
	// good empirically-supported fraction to use.
	bufferMemLimit := int64(float64(memoryLimit) * 0.10)
	buffer := colexecutils.NewSpillingBuffer(
		bufferAllocator, bufferMemLimit, diskQueueCfg, fdSemaphore, inputTypes, diskAcc, argIdx)
	base := _OP_NAMEBase{
		buffer:          buffer,
		outputColIdx:    outputColIdx,
		partitionColIdx: partitionColIdx,
		argIdx:          argIdx,
		offsetIdx:       offsetIdx,
		defaultIdx:      defaultIdx,
	}
	argType := inputTypes[argIdx]
	switch typeconv.TypeFamilyToCanonicalTypeFamily(argType.Family()) {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch argType.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			return newBufferedWindowOperator(
				&_OP_NAME_TYPEWindow{_OP_NAMEBase: base}, unlimitedAllocator, memoryLimit-bufferMemLimit,
				diskQueueCfg, fdSemaphore, diskAcc, input, inputTypes, argType, outputColIdx,
			), nil
			// {{end}}
		}
		// {{end}}
	}
	return nil, errors.Errorf("unsupported _OP_NAME window operator type %s", argType.Name())
}

// _OP_NAMEBase extracts common fields and methods of the _OP_NAME windower
// variations.
type _OP_NAMEBase struct {
	colexecop.InitHelper
	colexecop.CloserHelper
	_OP_NAMEComputeFields

	buffer *colexecutils.SpillingBuffer

	outputColIdx    int
	partitionColIdx int
	argIdx          int
	offsetIdx       int
	defaultIdx      int
}

// _OP_NAMEComputeFields extracts the fields that are used to calculate _OP_NAME
// output values.
type _OP_NAMEComputeFields struct {
	partitionSize int
	idx           int
}

// {{range .}}
// {{range .WidthOverloads}}

type _OP_NAME_TYPEWindow struct {
	_OP_NAMEBase
}

var _ bufferedWindower = &_OP_NAME_TYPEWindow{}

func (w *_OP_NAME_TYPEWindow) seekNextPartition(
	batch coldata.Batch, startIdx int, isPartitionStart bool,
) (nextPartitionIdx int) {
	n := batch.Length()
	if w.partitionColIdx == -1 {
		// There is only one partition, so it includes the entirety of this batch.
		w.partitionSize += n
		nextPartitionIdx = n
	} else {
		i := startIdx
		partitionCol := batch.ColVec(w.partitionColIdx).Bool()
		_ = partitionCol[n-1]
		_ = partitionCol[i]
		// Find the location of the start of the next partition (and the end of the
		// current one).
		for ; i < n; i++ {
			//gcassert:bce
			if partitionCol[i] {
				// Don't break for the start of the current partition.
				if !isPartitionStart || i != startIdx {
					break
				}
			}
		}
		w.partitionSize += i - startIdx
		nextPartitionIdx = i
	}

	// Add all tuples from the argument column that fall within the current
	// partition to the buffer so that they can be accessed later.
	if startIdx < nextPartitionIdx {
		w.buffer.AppendTuples(w.Ctx, batch, startIdx, nextPartitionIdx)
	}
	return nextPartitionIdx
}

func (w *_OP_NAME_TYPEWindow) processBatch(batch coldata.Batch, startIdx, endIdx int) {
	if startIdx >= endIdx {
		// No processing needs to be done for this portion of the current partition.
		return
	}
	leadLagVec := batch.ColVec(w.outputColIdx)
	leadLagCol := leadLagVec.TemplateType()
	leadLagNulls := leadLagVec.Nulls()
	// {{if .Sliceable}}
	_ = leadLagCol.Get(startIdx)
	_ = leadLagCol.Get(endIdx - 1)
	// {{end}}

	offsetVec := batch.ColVec(w.offsetIdx)
	offsetCol := offsetVec.Int64()
	offsetNulls := offsetVec.Nulls()
	_ = offsetCol[startIdx]
	_ = offsetCol[endIdx-1]

	defaultVec := batch.ColVec(w.defaultIdx)
	defaultCol := defaultVec.TemplateType()
	defaultNulls := defaultVec.Nulls()
	// {{if .Sliceable}}
	_ = defaultCol.Get(startIdx)
	_ = defaultCol.Get(endIdx - 1)
	// {{end}}

	if offsetNulls.MaybeHasNulls() {
		if defaultNulls.MaybeHasNulls() {
			_PROCESS_BATCH(true, true)
			return
		}
		_PROCESS_BATCH(true, false)
		return
	}
	if defaultNulls.MaybeHasNulls() {
		_PROCESS_BATCH(false, true)
		return
	}
	_PROCESS_BATCH(false, false)
}

// {{end}}
// {{end}}

func (b *_OP_NAMEBase) transitionToProcessing() {

}

func (b *_OP_NAMEBase) startNewPartition() {
	b.idx = 0
	b.partitionSize = 0
	b.buffer.Reset(b.Ctx)
}

func (b *_OP_NAMEBase) Init(ctx context.Context) {
	if !b.InitHelper.Init(ctx) {
		return
	}
}

func (b *_OP_NAMEBase) Close() {
	if !b.CloserHelper.Close() {
		return
	}
	b.buffer.Close(b.EnsureCtx())
}

// {{/*
// _PROCESS_BATCH is a code fragment that iterates over the given batch and
// sets the lag or lead output value.
func _PROCESS_BATCH(_OFFSET_HAS_NULLS bool, _DEFAULT_HAS_NULLS bool) { // */}}
	// {{define "processBatchTmpl" -}}
	for i := startIdx; i < endIdx; i++ {
		// {{if .OffsetHasNulls}}
		if offsetNulls.NullAt(i) {
			// When the offset is null, the output value is also null.
			leadLagNulls.SetNull(i)
			w.idx++
			continue
		}
		// {{end}}
		// {{if eq "_OP_NAME" "lag"}}
		requestedIdx := w.idx - int(offsetCol[i])
		// {{else}}
		requestedIdx := w.idx + int(offsetCol[i])
		// {{end}}
		w.idx++
		if requestedIdx < 0 || requestedIdx >= w.partitionSize {
			// The offset is out of range, so set the output value to the default.
			// {{if .DefaultHasNulls}}
			if defaultNulls.NullAt(i) {
				leadLagNulls.SetNull(i)
				continue
			}
			// {{end}}
			// {{if .IsBytesLike}}
			leadLagCol.CopySlice(defaultCol, i, i, i+1)
			// {{else}}
			val := defaultCol.Get(i)
			leadLagCol.Set(i, val)
			// {{end}}
			continue
		}
		vec, idx, _ := w.buffer.GetVecWithTuple(w.Ctx, 0 /* colIdx */, requestedIdx)
		if vec.Nulls().MaybeHasNulls() && vec.Nulls().NullAt(idx) {
			leadLagNulls.SetNull(i)
			continue
		}
		col := vec.TemplateType()
		// {{if .IsBytesLike}}
		// We have to use CopySlice here because the column already has a length of
		// n elements, and Set cannot set values before the last one.
		leadLagCol.CopySlice(col, i, idx, idx+1)
		// {{else}}
		val := col.Get(idx)
		// {{if .Sliceable}}
		//gcassert:bce
		// {{end}}
		leadLagCol.Set(i, val)
		// {{end}}
	}
	// {{end}}
	// {{/*
} // */}}
