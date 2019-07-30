// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stats

import (
	"container/heap"
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// SampledRow is a row that was sampled.
type SampledRow struct {
	Row  sqlbase.EncDatumRow
	Rank uint64
}

// SampleReservoir implements reservoir sampling using random sort. Each
// row is assigned a rank (which should be a uniformly generated random value),
// and rows with the smallest K ranks are retained.
//
// This is implemented as a max-heap of the smallest K ranks; each row can
// replace the row with the maximum rank. Note that heap operations only happen
// when we actually encounter a row that is among the top K so far; the
// probability of this is K/N if there were N rows so far; for large streams, we
// would have O(K log K) heap operations. The overall running time for a stream
// of size N is O(N + K log^2 K).
//
// The same structure can be used to combine sample sets (as long as the
// original ranks are preserved) for distributed reservoir sampling. The
// requirement is that the capacity of each distributed reservoir must have been
// at least as large as this reservoir.
type SampleReservoir struct {
	samples  []SampledRow
	colTypes []types.T
	da       sqlbase.DatumAlloc
	ra       sqlbase.EncDatumRowAlloc
	memAcc   *mon.BoundAccount
}

var _ heap.Interface = &SampleReservoir{}

// Init initializes a SampleReservoir.
func (sr *SampleReservoir) Init(numSamples int, colTypes []types.T, memAcc *mon.BoundAccount) {
	sr.samples = make([]SampledRow, 0, numSamples)
	sr.colTypes = colTypes
	sr.memAcc = memAcc
}

// Len is part of heap.Interface.
func (sr *SampleReservoir) Len() int {
	return len(sr.samples)
}

// Less is part of heap.Interface.
func (sr *SampleReservoir) Less(i, j int) bool {
	// We want a max heap, so higher ranks sort first.
	return sr.samples[i].Rank > sr.samples[j].Rank
}

// Swap is part of heap.Interface.
func (sr *SampleReservoir) Swap(i, j int) {
	sr.samples[i], sr.samples[j] = sr.samples[j], sr.samples[i]
}

// Push is part of heap.Interface, but we're not using it.
func (sr *SampleReservoir) Push(x interface{}) { panic("unimplemented") }

// Pop is part of heap.Interface, but we're not using it.
func (sr *SampleReservoir) Pop() interface{} { panic("unimplemented") }

// SampleRow looks at a row and either drops it or adds it to the reservoir.
func (sr *SampleReservoir) SampleRow(
	ctx context.Context, row sqlbase.EncDatumRow, rank uint64,
) error {
	if len(sr.samples) < cap(sr.samples) {
		// We haven't accumulated enough rows yet, just append.
		rowCopy := sr.ra.AllocRow(len(row))

		// Perform memory accounting for the allocated EncDatumRow. We will account
		// for the additional memory used after copying inside copyRow.
		if sr.memAcc != nil {
			if err := sr.memAcc.Grow(ctx, int64(len(rowCopy))*int64(rowCopy[0].Size())); err != nil {
				return err
			}
		}
		if err := sr.copyRow(ctx, rowCopy, row); err != nil {
			return err
		}
		sr.samples = append(sr.samples, SampledRow{Row: rowCopy, Rank: rank})
		if len(sr.samples) == cap(sr.samples) {
			// We just reached the limit; initialize the heap.
			heap.Init(sr)
		}
		return nil
	}
	// Replace the max rank if ours is smaller.
	if len(sr.samples) > 0 && rank < sr.samples[0].Rank {
		if err := sr.copyRow(ctx, sr.samples[0].Row, row); err != nil {
			return err
		}
		sr.samples[0].Rank = rank
		heap.Fix(sr, 0)
	}
	return nil
}

// Get returns the sampled rows.
func (sr *SampleReservoir) Get() []SampledRow {
	return sr.samples
}

func (sr *SampleReservoir) copyRow(ctx context.Context, dst, src sqlbase.EncDatumRow) error {
	for i := range src {
		// Copy only the decoded datum to ensure that we remove any reference to
		// the encoded bytes. The encoded bytes would have been scanned in a batch
		// of ~10000 rows, so we must delete the reference to allow the garbage
		// collector to release the memory from the batch.
		if err := src[i].EnsureDecoded(&sr.colTypes[i], &sr.da); err != nil {
			return err
		}
		beforeSize := dst[i].Size()
		dst[i] = sqlbase.DatumToEncDatum(&sr.colTypes[i], src[i].Datum)
		// Perform memory accounting.
		if afterSize := dst[i].Size(); sr.memAcc != nil && afterSize > beforeSize {
			if err := sr.memAcc.Grow(ctx, int64(afterSize-beforeSize)); err != nil {
				return err
			}
		}

	}
	return nil
}
