// Copyright 2017 The Cockroach Authors.
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

package stats

import (
	"container/heap"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	samples []SampledRow
}

var _ heap.Interface = &SampleReservoir{}

// Init initializes a SampleReservoir.
func (sr *SampleReservoir) Init(numSamples int) {
	sr.samples = make([]SampledRow, 0, numSamples)
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
func (sr *SampleReservoir) SampleRow(row sqlbase.EncDatumRow, rank uint64) {
	if len(sr.samples) < cap(sr.samples) {
		// We haven't accumulated enough rows yet, just append.
		sr.samples = append(sr.samples, SampledRow{Row: row, Rank: rank})
		if len(sr.samples) == cap(sr.samples) {
			// We just reached the limit; initialize the heap.
			heap.Init(sr)
		}
		return
	}
	// Replace the max rank if ours is smaller.
	if rank < sr.samples[0].Rank {
		sr.samples[0] = SampledRow{Row: row, Rank: rank}
		heap.Fix(sr, 0)
	}
}

// Get returns the sampled rows.
func (sr *SampleReservoir) Get() []SampledRow {
	return sr.samples
}
