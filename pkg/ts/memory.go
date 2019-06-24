// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ts

import (
	"context"
	"fmt"
	"math"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// Compute the size of various structures to use when tracking memory usage.
var (
	sizeOfTimeSeriesData = int64(unsafe.Sizeof(roachpb.InternalTimeSeriesData{}))
	sizeOfSample         = int64(unsafe.Sizeof(roachpb.InternalTimeSeriesSample{}))
	sizeOfDataPoint      = int64(unsafe.Sizeof(tspb.TimeSeriesDatapoint{}))
	sizeOfInt32          = int64(unsafe.Sizeof(int32(0)))
	sizeOfUint32         = int64(unsafe.Sizeof(uint32(0)))
	sizeOfFloat64        = int64(unsafe.Sizeof(float64(0)))
	sizeOfTimestamp      = int64(unsafe.Sizeof(hlc.Timestamp{}))
)

// QueryMemoryOptions represents the adjustable options of a QueryMemoryContext.
type QueryMemoryOptions struct {
	// BudgetBytes is the maximum number of bytes that should be reserved by this
	// query at any one time.
	BudgetBytes int64
	// EstimatedSources is an estimate of the number of distinct sources that this
	// query will encounter on disk. This is needed to better estimate how much
	// memory a query will actually consume.
	EstimatedSources int64
	// InterpolationLimitNanos determines the maximum gap size for which missing
	// values will be interpolated. By making this limit explicit, we can put a
	// hard limit on the timespan that needs to be read from disk to satisfy
	// a query.
	InterpolationLimitNanos int64
	// If true, memory will be computed assuming the columnar layout.
	Columnar bool
}

// QueryMemoryContext encapsulates the memory-related parameters of a time
// series query. These same parameters are often repeated across numerous
// queries.
type QueryMemoryContext struct {
	workerMonitor *mon.BytesMonitor
	resultAccount *mon.BoundAccount
	QueryMemoryOptions
}

// MakeQueryMemoryContext constructs a new query memory context from the
// given parameters.
func MakeQueryMemoryContext(
	workerMonitor, resultMonitor *mon.BytesMonitor, opts QueryMemoryOptions,
) QueryMemoryContext {
	resultAccount := resultMonitor.MakeBoundAccount()
	return QueryMemoryContext{
		workerMonitor:      workerMonitor,
		resultAccount:      &resultAccount,
		QueryMemoryOptions: opts,
	}
}

// Close closes any resources held by the queryMemoryContext.
func (qmc QueryMemoryContext) Close(ctx context.Context) {
	if qmc.resultAccount != nil {
		qmc.resultAccount.Close(ctx)
	}
}

// overflowSafeMultiply64 is a check for signed integer multiplication taken
// from https://github.com/JohnCGriffin/overflow/blob/master/overflow_impl.go
func overflowSafeMultiply64(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	c := a * b
	if (c < 0) == ((a < 0) != (b < 0)) {
		if c/b == a {
			return c, true
		}
	}
	return c, false
}

// GetMaxTimespan computes the longest timespan that can be safely queried while
// remaining within the given memory budget. Inputs are the resolution of data
// being queried, the budget, the estimated number of sources, and the
// interpolation limit being used for the query.
func (qmc QueryMemoryContext) GetMaxTimespan(r Resolution) (int64, error) {
	slabDuration := r.SlabDuration()

	// Compute the size of a slab.
	sizeOfSlab := qmc.computeSizeOfSlab(r)

	// InterpolationBuffer is the number of slabs outside of the query range
	// needed to satisfy the interpolation limit. Extra slabs may be queried
	// on both sides of the target range.
	interpolationBufferOneSide :=
		int64(math.Ceil(float64(qmc.InterpolationLimitNanos) / float64(slabDuration)))

	interpolationBuffer := interpolationBufferOneSide * 2

	// If the (interpolation buffer timespan - interpolation limit) is less than
	// half of a slab, then it is possible for one additional slab to be queried
	// that would not have otherwise been queried. This can occur when the queried
	// timespan does not start on an even slab boundary.
	if (interpolationBufferOneSide*slabDuration)-qmc.InterpolationLimitNanos < slabDuration/2 {
		interpolationBuffer++
	}

	// The number of slabs that can be queried safely is perSeriesMem/sizeOfSlab,
	// less the interpolation buffer.
	perSourceMem := qmc.BudgetBytes / qmc.EstimatedSources
	numSlabs := perSourceMem/sizeOfSlab - interpolationBuffer
	if numSlabs <= 0 {
		return 0, fmt.Errorf("insufficient memory budget to attempt query")
	}

	maxDuration, valid := overflowSafeMultiply64(numSlabs, slabDuration)
	if valid {
		return maxDuration, nil
	}
	return math.MaxInt64, nil
}

// GetMaxRollupSlabs returns the maximum number of rows that should be processed
// at one time when rolling up the given resolution.
func (qmc QueryMemoryContext) GetMaxRollupSlabs(r Resolution) int64 {
	// Rollup computations only occur when columnar is true.
	return qmc.BudgetBytes / qmc.computeSizeOfSlab(r)
}

// computeSizeOfSlab returns the size of a completely full data slab for the supplied
// data resolution.
func (qmc QueryMemoryContext) computeSizeOfSlab(r Resolution) int64 {
	slabDuration := r.SlabDuration()

	var sizeOfSlab int64
	if qmc.Columnar {
		// Contains an Offset (int32) and Last (float64) for each sample.
		sizeOfColumns := (sizeOfInt32 + sizeOfFloat64)
		if r.IsRollup() {
			// Five additional float64 (First, Min, Max, Sum, Variance) and one uint32
			// (count) per sample
			sizeOfColumns += 5*sizeOfFloat64 + sizeOfUint32
		}
		sizeOfSlab = sizeOfTimeSeriesData + (slabDuration/r.SampleDuration())*sizeOfColumns
	} else {
		// Contains a sample structure for each sample.
		sizeOfSlab = sizeOfTimeSeriesData + (slabDuration/r.SampleDuration())*sizeOfSample
	}
	return sizeOfSlab
}
