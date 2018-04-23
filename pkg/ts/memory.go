// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ts

import (
	"context"
	"fmt"
	"math"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// Compute the size of various structures to use when tracking memory usage.
var (
	sizeOfDataSpan       = int64(unsafe.Sizeof(dataSpan{}))
	sizeOfCalibratedData = int64(unsafe.Sizeof(calibratedData{}))
	sizeOfTimeSeriesData = int64(unsafe.Sizeof(roachpb.InternalTimeSeriesData{}))
	sizeOfSample         = int64(unsafe.Sizeof(roachpb.InternalTimeSeriesSample{}))
	sizeOfDataPoint      = int64(unsafe.Sizeof(tspb.TimeSeriesDatapoint{}))
)

// QueryMemoryOptions represents the adjustable options of a QueryMemoryContext.
type QueryMemoryOptions struct {
	BudgetBytes             int64
	EstimatedSources        int64
	InterpolationLimitNanos int64
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

// GetMaxTimespan computes the longest timespan that can be safely queried while
// remaining within the given memory budget. Inputs are the resolution of data
// being queried, the budget, the estimated number of sources, and the
// interpolation limit being used for the query.
func (qmc QueryMemoryContext) GetMaxTimespan(r Resolution) (int64, error) {
	slabDuration := r.SlabDuration()

	// Size of slab is the size of a completely full data slab for the supplied
	// data resolution.
	sizeOfSlab := sizeOfTimeSeriesData + (slabDuration/r.SampleDuration())*sizeOfSample

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

	return numSlabs * slabDuration, nil
}
