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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
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
	colTypes []*types.T
	da       sqlbase.DatumAlloc
	ra       sqlbase.EncDatumRowAlloc
	memAcc   *mon.BoundAccount

	// sampleCols contains the ordinals of columns that should be sampled from
	// each row. Note that the sampled rows still contain all columns, but
	// any columns not part of this set are given a null value.
	sampleCols util.FastIntSet
}

var _ heap.Interface = &SampleReservoir{}

// Init initializes a SampleReservoir.
func (sr *SampleReservoir) Init(
	numSamples int, colTypes []*types.T, memAcc *mon.BoundAccount, sampleCols util.FastIntSet,
) {
	sr.samples = make([]SampledRow, 0, numSamples)
	sr.colTypes = colTypes
	sr.memAcc = memAcc
	sr.sampleCols = sampleCols
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
	ctx context.Context, evalCtx *tree.EvalContext, row sqlbase.EncDatumRow, rank uint64,
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
}

// Get returns the sampled rows.
func (sr *SampleReservoir) Get() []SampledRow {
	return sr.samples
}

func (sr *SampleReservoir) copyRow(
	ctx context.Context, evalCtx *tree.EvalContext, dst, src sqlbase.EncDatumRow,
) error {
	for i := range src {
		if !sr.sampleCols.Contains(i) {
			dst[i].Datum = tree.DNull
			continue
		}
		// Copy only the decoded datum to ensure that we remove any reference to
		// the encoded bytes. The encoded bytes would have been scanned in a batch
		// of ~10000 rows, so we must delete the reference to allow the garbage
		// collector to release the memory from the batch.
		if err := src[i].EnsureDecoded(sr.colTypes[i], &sr.da); err != nil {
			return err
		}
		beforeSize := dst[i].Size()
		dst[i] = sqlbase.DatumToEncDatum(sr.colTypes[i], src[i].Datum)
		afterSize := dst[i].Size()

		// If the datum is too large, truncate it (this also performs a copy).
		// Otherwise, just perform a copy.
		if afterSize > uintptr(maxBytesPerSample) {
			dst[i].Datum = truncateDatum(evalCtx, dst[i].Datum, maxBytesPerSample)
			afterSize = dst[i].Size()
		} else {
			if enc, ok := src[i].Encoding(); ok && enc != sqlbase.DatumEncoding_VALUE {
				// Only datums that were key-encoded might reference the kv batch.
				dst[i].Datum = deepCopyDatum(evalCtx, dst[i].Datum)
			}
		}

		// Perform memory accounting.
		if sr.memAcc != nil && afterSize > beforeSize {
			if err := sr.memAcc.Grow(ctx, int64(afterSize-beforeSize)); err != nil {
				return err
			}
		}
	}
	return nil
}

const maxBytesPerSample = 400

// truncateDatum truncates large datums to avoid using excessive memory or disk
// space. It performs a best-effort attempt to return a datum that is similar
// to d using at most maxBytes bytes.
//
// For example, if maxBytes=10, "Cockroach Labs" would be truncated to
// "Cockroach ".
func truncateDatum(evalCtx *tree.EvalContext, d tree.Datum, maxBytes int) tree.Datum {
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

// deepCopyDatum performs a deep copy for datums such as DString to remove any
// references to the kv batch and allow the batch to be garbage collected.
// Note: this function is currently only called for key-encoded datums. Update
// the calling function if there is a need to call this for value-encoded
// datums as well.
func deepCopyDatum(evalCtx *tree.EvalContext, d tree.Datum) tree.Datum {
	switch t := d.(type) {
	case *tree.DString:
		return tree.NewDString(deepCopyString(string(*t)))

	case *tree.DCollatedString:
		return &tree.DCollatedString{
			Contents: deepCopyString(t.Contents),
			Locale:   t.Locale,
			Key:      t.Key,
		}

	case *tree.DOidWrapper:
		return &tree.DOidWrapper{
			Wrapped: deepCopyDatum(evalCtx, t.Wrapped),
			Oid:     t.Oid,
		}

	default:
		// We do not collect stats on JSON, and other types do not require a deep
		// copy (or they are already copied during decoding).
		return d
	}
}

// deepCopyString performs a deep copy of a string.
func deepCopyString(s string) string {
	b := make([]byte, len(s))
	copy(b, s)
	return string(b)
}
