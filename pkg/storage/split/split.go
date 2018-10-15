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

package split

import (
	"bytes"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// Load-based splitting.
//
// - Engage split for ranges:
//  - With size exceeding min-range-bytes
//  - with reqs/s rate over a configurable threshold
// - Disengage when a range no longer meets the criteria
// - During split:
//  - Record start time
//  - Keep a sample of 10 keys
//   - Each sample contains three counters: left, right and contained.
//   - On each span, increment the left and/or right counters, depending
//     on whether the span falls entirely to the left, to the right.
//     If exactly on the key, increment neither.
//   - If the span overlaps with the key, increment the contained counter.
//   - When a sample is replaced, discard its counters.
//  - If a range is on for more than a threshold interval:
//   - Examine sample for the smallest diff between left and right counters,
//     excluding any whose counters are not sufficiently advanced;
//     If not less than some constant threshold, skip split.
//   - Use the contained counters to give lower priority to potential split
//     points that have more requests that span over it.
//   - If a desired point is reached, add range to split queue with the chosen
//     key as split key, and provide hint to scatter the replicas.

const (
	durationThreshold  = 10 * time.Second // 10s
	splitKeySampleSize = 20               // size of split key sample
	splitKeyMinCounter = 200              // min aggregate counters before consideration
	splitKeyThreshold  = 0.25             // 25% difference between left/right counters
)

type sample struct {
	key                    roachpb.Key
	left, right, contained int
}

// Finder is a structure that is used to determine the split point
// using the Reservoir Sampling method.
type Finder struct {
	startNanos int64
	samples    [splitKeySampleSize]sample
	count      int
}

// New initiates a Finder with the given time.
func New(startNanos int64) *Finder {
	return &Finder{
		startNanos: startNanos,
	}
}

// Ready checks if the Finder has been initialized with a sufficient
// sample duration.
func (f *Finder) Ready(nowNanos int64) bool {
	return time.Duration(nowNanos-f.startNanos) > durationThreshold
}

// Record informs the Finder about where the span lies with
// regard to the keys in the samples.
func (f *Finder) Record(span roachpb.Span, intNFn func(int) int) {
	if f == nil {
		return
	}

	var idx int
	count := f.count
	f.count++
	if count < splitKeySampleSize {
		idx = count
	} else if idx = intNFn(count); idx >= splitKeySampleSize {
		// Increment all existing keys' counters.
		for i, sa := range f.samples {
			if span.ContainsKey(sa.key) {
				sa.contained++
			} else {
				if comp := bytes.Compare(sa.key, span.Key); comp < 0 {
					sa.right++
				} else if comp > 0 {
					sa.left++
				}
			}
			f.samples[i] = sa
		}
		return
	}

	// Note we always use the start key of the span. We could
	// take the average of the byte slices, but that seems
	// unnecessarily complex for practical usage.
	f.samples[idx] = sample{key: span.Key}
}

// Key finds an appropriate split point based on the Reservoir
// sampling method.
func (f *Finder) Key() (bool, roachpb.Key) {
	if f == nil {
		return false, nil
	}

	var bestIdx = -1
	var bestScore float64 = 2
	var mostContainedKeyCount = 1
	for _, s := range f.samples {
		if s.contained >= mostContainedKeyCount {
			mostContainedKeyCount = s.contained
		}
	}

	for i, s := range f.samples {
		if s.left+s.right < splitKeyMinCounter {
			continue
		}
		balanceScore := math.Abs(float64(s.left-s.right)) / float64(s.left+s.right)
		finalScore := balanceScore + (float64(s.contained) / float64(mostContainedKeyCount))
		if balanceScore >= splitKeyThreshold {
			continue
		}
		if finalScore < bestScore {
			bestIdx = i
			bestScore = finalScore
		}
	}

	if bestIdx == -1 {
		return false, nil
	}
	return true, f.samples[bestIdx].key
}
