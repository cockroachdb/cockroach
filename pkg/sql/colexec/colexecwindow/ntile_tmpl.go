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
// This file is the execgen template for ntile.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexecwindow

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// TODO(yuzefovich): add benchmarks.
// TODO(drewk): consider adding a version optimized for the case where no
//  ORDER BY is specified, since the bucket value can be calculated simply by
//  taking the row number modulo the number of buckets.

// NewNTileOperator creates a new Operator that computes window function NTILE.
// outputColIdx specifies in which coldata.Vec the operator should put its
// output (if there is no such column, a new column is appended).
func NewNTileOperator(args *WindowArgs, argIdx int) colexecop.Operator {
	base := nTileBase{
		outputColIdx:    args.OutputColIdx,
		partitionColIdx: args.PartitionColIdx,
		argIdx:          argIdx,
	}
	var windower bufferedWindower
	if args.PartitionColIdx == -1 {
		windower = &nTileNoPartition{base}
	} else {
		windower = &nTileWithPartition{base}
	}
	return newBufferedWindowOperator(args, windower, types.Int, args.MemoryLimit)
}

// nTileBase extracts common fields and methods of the two variations of ntile
// windowers.
type nTileBase struct {
	nTileComputeFields

	outputColIdx    int
	partitionColIdx int
	argIdx          int
}

// nTileComputeFields extracts the fields that are used to calculate ntile
// bucket values.
type nTileComputeFields struct {
	// boundary is the number of rows that should be in the current bucket.
	boundary int

	// currBucketCount is the row number of the current bucket.
	currBucketCount int

	// partitionSize tracks the number of tuples in the current partition.
	partitionSize int

	// numBuckets is the number of buckets across which to distribute the rows of
	// the current partition. It is not necessarily the same value for different
	// partitions.
	numBuckets int

	// hasArg indicates whether a non-null value has been found for numBuckets in
	// the current partition.
	hasArg bool

	// leadingNulls stores the number of leading rows in the current partition for
	// which the num_buckets argument value is null. It is found during the
	// seeking phase, and is used to set the corresponding output values to null
	// during the processing phase
	leadingNulls int

	// nTile is the bucket value to which the row currently being processed is
	// assigned. It is reset once processing of a new partition begins.
	nTile int64
}

// {{range .}}

type _NTILE_STRING struct {
	nTileBase
}

var _ bufferedWindower = &_NTILE_STRING{}

func (w *_NTILE_STRING) seekNextPartition(
	batch coldata.Batch, startIdx int, isPartitionStart bool,
) (nextPartitionIdx int) {
	n := batch.Length()
	nTileVec := batch.ColVec(w.outputColIdx)
	argVec := batch.ColVec(w.argIdx)
	argNulls := argVec.Nulls()
	argCol := argVec.Int64()
	// {{if .HasPartition}}
	partitionCol := batch.ColVec(w.partitionColIdx).Bool()
	_ = partitionCol[n-1]
	// {{end}}
	i := startIdx
	_ = argCol[n-1]
	if !w.hasArg {
		// Scan to the first non-null num_buckets value. If it is not found,
		// scan to the beginning of the next partition.
		_ = argCol[i]
		for ; i < n; i++ {
			// {{if .HasPartition}}
			//gcassert:bce
			if partitionCol[i] {
				// Don't break for the start of the current partition.
				if !isPartitionStart || i != startIdx {
					break
				}
			}
			// {{end}}
			if !argNulls.NullAt(i) {
				// We have found the first row in the current partition for which
				// the argument is non-null.
				//gcassert:bce
				w.numBuckets = int(argCol[i])
				w.hasArg = true
				break
			}
		}
		// For all leading rows in a partition whose argument is null, the ntile
		// bucket value is also null. Set these rows to null, then increment
		// processingIdx to indicate that bucket values are not to be calculated
		// for these rows.
		nTileVec.Nulls().SetNullRange(startIdx, i)
		w.leadingNulls += i - startIdx
	}
	// {{if .HasPartition}}
	// Pick up where the last loop left off to find the location of the start
	// of the next partition (and the end of the current one).
	if i < n {
		_ = partitionCol[i]
		for ; i < n; i++ {
			//gcassert:bce
			if partitionCol[i] {
				// Don't break for the start of the current partition.
				if !isPartitionStart || i != startIdx {
					break
				}
			}
		}
	}
	w.partitionSize += i - startIdx
	nextPartitionIdx = i
	// {{else}}
	// There is only one partition, so it includes the entirety of this batch.
	w.partitionSize += n
	nextPartitionIdx = n
	// {{end}}
	if w.hasArg && w.numBuckets <= 0 {
		colexecerror.ExpectedError(builtins.ErrInvalidArgumentForNtile)
	}
	return nextPartitionIdx
}

// {{end}}

func (b *nTileBase) processBatch(batch coldata.Batch, startIdx, endIdx int) {
	// The ntile output value is null for any leading rows for which the input is
	// null. We have kept track of the number of leading input nulls - use it to
	// set the leading output nulls (if any) for this batch.
	nTileVec := batch.ColVec(b.outputColIdx)
	nullsToSet := b.leadingNulls
	if nullsToSet > endIdx-startIdx {
		// This can happen when the leading nulls span more than one batch.
		nullsToSet = endIdx - startIdx
	}
	nTileVec.Nulls().SetNullRange(startIdx, startIdx+nullsToSet)
	startIdx += nullsToSet
	b.leadingNulls -= nullsToSet
	if startIdx >= endIdx {
		// No further processing needs to be done for this portion of the current
		// partition. This can happen when the num_buckets value for a partition was
		// null up to the end of a batch.
		return
	}
	if b.numBuckets <= 0 {
		colexecerror.InternalError(
			errors.AssertionFailedf("ntile windower calculating bucket value with invalid argument"))
	}
	// Now set the non-null ntile output values.
	remainder := b.partitionSize % b.numBuckets
	nTileCol := nTileVec.Int64()
	_ = nTileCol[startIdx]
	_ = nTileCol[endIdx-1]
	for i := startIdx; i < endIdx; i++ {
		b.currBucketCount++
		if b.boundary < b.currBucketCount {
			// Move to next ntile bucket.
			if remainder != 0 && int(b.nTile) == remainder {
				b.boundary--
				remainder = 0
			}
			b.nTile++
			b.currBucketCount = 1
		}
		//gcassert:bce
		nTileCol[i] = b.nTile
	}
}

func (b *nTileBase) transitionToProcessing() {
	if !b.hasArg {
		// Since a non-null value for the number of buckets was not found, the
		// output column values will just be set to null, so no additional
		// calculations are necessary (and in any case could lead to a division by
		// zero).
		return
	}
	if b.numBuckets <= 0 {
		colexecerror.InternalError(
			errors.AssertionFailedf("ntile windower calculating bucket value with invalid argument"))
	}
	b.nTile = 1
	b.currBucketCount = 0
	b.boundary = b.partitionSize / b.numBuckets
	if b.boundary <= 0 {
		b.boundary = 1
	} else {
		// If the total number is not divisible, add 1 row to leading buckets.
		if b.partitionSize%b.numBuckets != 0 {
			b.boundary++
		}
	}
}

func (b *nTileBase) startNewPartition() {
	b.partitionSize = 0
	b.leadingNulls = 0
	b.hasArg = false
}

func (b *nTileBase) Init(ctx context.Context) {}

func (b *nTileBase) Close(context.Context) {}
