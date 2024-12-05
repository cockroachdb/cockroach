// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"container/heap"
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/memsize"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// SampledRow is a row that was sampled.
type SampledRow struct {
	Row  rowenc.EncDatumRow
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
	colTypes []*types.T
	ra       rowenc.EncDatumRowAlloc
	memAcc   *mon.BoundAccount

	// minNumSamples is the minimum capcity (K) needed for sampling to be
	// meaningful. If the reservoir capacity would fall below this, SampleRow will
	// err instead of decreasing it further.
	minNumSamples int

	// sampleCols contains the ordinals of columns that should be sampled from
	// each row. Note that the sampled rows still contain all columns, but
	// any columns not part of this set are given a null value.
	sampleCols intsets.Fast

	// scratch is used as the staging area for the new row to be copied into the
	// reservoir, before we know whether the memory reservation for it will be
	// approved.
	scratch rowenc.EncDatumRow
}

var _ heap.Interface = &SampleReservoir{}

// Init initializes a SampleReservoir.
func (sr *SampleReservoir) Init(
	numSamples, minNumSamples int,
	colTypes []*types.T,
	memAcc *mon.BoundAccount,
	sampleCols intsets.Fast,
) {
	if minNumSamples < 1 || minNumSamples > numSamples {
		minNumSamples = numSamples
	}
	sr.samples = make([]SampledRow, 0, numSamples)
	sr.minNumSamples = minNumSamples
	sr.colTypes = colTypes
	sr.memAcc = memAcc
	sr.sampleCols = sampleCols
	sr.scratch = make(rowenc.EncDatumRow, len(colTypes))
}

// Disable releases the memory of this SampleReservoir and sets its capacity
// to zero.
func (sr *SampleReservoir) Disable() {
	sr.samples = nil
}

// Len is part of heap.Interface.
func (sr *SampleReservoir) Len() int {
	return len(sr.samples)
}

// Cap returns K, the maximum number of samples the reservoir can hold.
func (sr *SampleReservoir) Cap() int {
	return cap(sr.samples)
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

// Pop is part of heap.Interface.
func (sr *SampleReservoir) Pop() interface{} {
	n := len(sr.samples)
	samp := sr.samples[n-1]
	sr.samples[n-1] = SampledRow{} // Avoid leaking the popped sample.
	sr.samples = sr.samples[:n-1]
	return samp
}

// MaybeResize safely shrinks the capacity of the reservoir (K) without
// introducing bias if the requested capacity is less than the current
// capacity, and returns whether the capacity changed. (Note that the capacity
// can only decrease without introducing bias. Increasing capacity would cause
// later rows to be over-represented relative to earlier rows.)
func (sr *SampleReservoir) MaybeResize(ctx context.Context, k int) bool {
	if k >= cap(sr.samples) {
		return false
	}
	// Make sure we have initialized the heap before popping.
	heap.Init(sr)
	for len(sr.samples) > k {
		samp := heap.Pop(sr).(SampledRow)
		if sr.memAcc != nil {
			sr.memAcc.Shrink(ctx, int64(samp.Row.Size()))
		}
	}
	// Copy to a new array to allow garbage collection.
	samples := make([]SampledRow, len(sr.samples), k)
	copy(samples, sr.samples)
	sr.samples = samples
	return true
}

// retryMaybeResize tries to execute a memory-allocating operation, shrinking
// the capacity of the reservoir (K) as necessary until the operation succeeds
// or the capacity reaches minNumSamples, at which point an error is returned.
func (sr *SampleReservoir) retryMaybeResize(ctx context.Context, op func() error) error {
	for {
		if err := op(); err == nil || !sqlerrors.IsOutOfMemoryError(err) ||
			len(sr.samples) == 0 || len(sr.samples)/2 < sr.minNumSamples {
			return err
		}
		// We've used too much memory. Remove half the samples and try again.
		sr.MaybeResize(ctx, len(sr.samples)/2)
	}
}

// SampleRow looks at a row and either drops it or adds it to the reservoir. The
// capacity of the reservoir (K) will shrink if it hits a memory limit. If
// capacity goes below minNumSamples, SampleRow will return an error. If
// SampleRow returns an error (any type of error), no additional calls to
// SampleRow should be made as the failed samples will have introduced bias.
func (sr *SampleReservoir) SampleRow(
	ctx context.Context, evalCtx *eval.Context, row rowenc.EncDatumRow, rank uint64,
) error {
	return sr.retryMaybeResize(ctx, func() error {
		if len(sr.samples) < cap(sr.samples) {
			// We haven't accumulated enough rows yet, just append.
			rowCopy := sr.ra.AllocRow(len(row))

			// Perform memory accounting for the allocated EncDatumRow. We will
			// account for the additional memory used after copying inside copyRow.
			if sr.memAcc != nil {
				if err := sr.memAcc.Grow(ctx, int64(rowCopy.Size())); err != nil {
					return err
				}
			}
			if err := sr.copyRow(ctx, evalCtx, rowCopy, row); err != nil {
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
			if err := sr.copyRow(ctx, evalCtx, sr.samples[0].Row, row); err != nil {
				return err
			}
			sr.samples[0].Rank = rank
			heap.Fix(sr, 0)
		}
		return nil
	})
}

// Get returns the sampled rows.
func (sr *SampleReservoir) Get() []SampledRow {
	return sr.samples
}

// GetNonNullDatums returns the non-null values of the specified column. The
// capacity of the reservoir (K) will shrink if we hit a memory limit while
// building this return slice. If the capacity goes below minNumSamples,
// GetNonNullDatums will return an error.
func (sr *SampleReservoir) GetNonNullDatums(
	ctx context.Context, memAcc *mon.BoundAccount, colIdx int,
) (values tree.Datums, err error) {
	err = sr.retryMaybeResize(ctx, func() error {
		// Account for the memory we'll use copying the samples into values.
		if memAcc != nil {
			if err := memAcc.Grow(ctx, memsize.DatumOverhead*int64(len(sr.samples))); err != nil {
				return err
			}
		}
		values = make(tree.Datums, 0, len(sr.samples))
		for _, sample := range sr.samples {
			d := sample.Row[colIdx].Datum
			if d == nil {
				values = nil
				return errors.AssertionFailedf("value in column %d not decoded", colIdx)
			}
			if d != tree.DNull {
				values = append(values, d)
			}
		}
		return nil
	})
	return
}

func (sr *SampleReservoir) copyRow(
	ctx context.Context, evalCtx *eval.Context, dst, src rowenc.EncDatumRow,
) error {
	// First, we calculate how much memory has already been accounted for the
	// "before" row (row that we're about to overwrite) as well as how much
	// space we need for the "after" row (row that we're about to keep).
	// Simultaneously, we're staging the "after" row into the scratch space.
	var beforeRowSize, afterRowSize int64
	for i := range src {
		if !sr.sampleCols.Contains(i) {
			sr.scratch[i].Datum = tree.DNull
			continue
		}
		// Copy only the decoded datum to ensure that we remove any reference to
		// the encoded bytes. The encoded bytes would have been scanned in a batch
		// of ~10000 rows, so we must delete the reference to allow the garbage
		// collector to release the memory from the batch.
		if err := src[i].EnsureDecoded(sr.colTypes[i], nil /* da */); err != nil {
			return err
		}
		beforeRowSize += int64(dst[i].Size())
		sr.scratch[i] = rowenc.DatumToEncDatum(sr.colTypes[i], src[i].Datum)
		afterSize := sr.scratch[i].Size()

		// If the datum is too large, truncate it.
		if afterSize > uintptr(maxBytesPerSample) {
			sr.scratch[i].Datum = truncateDatum(evalCtx, sr.scratch[i].Datum, maxBytesPerSample)
			afterSize = sr.scratch[i].Size()
		}
		afterRowSize += int64(afterSize)
	}
	// Now that we know the exact row sizes we're dealing with, we perform the
	// memory accounting.
	if sr.memAcc != nil {
		if err := sr.memAcc.Resize(ctx, beforeRowSize, afterRowSize); err != nil {
			return err
		}
	}
	// The memory reservation, if needed, was approved, so we're ok to keep the
	// row.
	copy(dst, sr.scratch)
	return nil
}

const maxBytesPerSample = 400

// truncateDatum truncates large datums to avoid using excessive memory or disk
// space. It performs a best-effort attempt to return a datum that is similar
// to d using at most maxBytes bytes.
//
// For example, if maxBytes=10, "Cockroach Labs" would be truncated to
// "Cockroach ".
func truncateDatum(evalCtx *eval.Context, d tree.Datum, maxBytes int) tree.Datum {
	switch t := d.(type) {
	case *tree.DBitArray:
		b := tree.DBitArray{BitArray: t.ToWidth(uint(maxBytes * 8))}
		return &b

	case *tree.DBytes:
		// Make a copy so the memory from the original byte string can be garbage
		// collected.
		b := make([]byte, maxBytes)
		copy(b, *t)
		return tree.NewDBytes(tree.DBytes(b))

	case *tree.DString:
		return tree.NewDString(truncateString(string(*t), maxBytes))

	case *tree.DCollatedString:
		contents := truncateString(t.Contents, maxBytes)

		// Note: this will end up being larger than maxBytes due to the key and
		// locale, so this is just a best-effort attempt to limit the size.
		res, err := tree.NewDCollatedString(contents, t.Locale, &evalCtx.CollationEnv)
		if err != nil {
			return d
		}
		return res

	case *tree.DOidWrapper:
		return &tree.DOidWrapper{
			Wrapped: truncateDatum(evalCtx, t.Wrapped, maxBytes),
			Oid:     t.Oid,
		}

	default:
		// It's not easy to truncate other types (e.g. Decimal).
		return d
	}
}

// truncateString truncates long strings to the longest valid substring that is
// less than maxBytes bytes. It is rune-aware so it does not cut unicode
// characters in half.
func truncateString(s string, maxBytes int) string {
	last := 0
	// For strings, range skips from rune to rune and i is the byte index of
	// the current rune.
	for i := range s {
		if i > maxBytes {
			break
		}
		last = i
	}

	// Copy the truncated string so that the memory from the longer string can
	// be garbage collected.
	b := make([]byte, last)
	copy(b, s)
	return string(b)
}
