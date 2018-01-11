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
// Until merging is implemented, this mechanism is active only when the
// whole system contains 1,000 or fewer ranges, to prevent runaway range
// growth.
//
// - Engage split for ranges:
//  - With size exceeding min-range-bytes
//  - with reqs/s rate over a threshold
// - Disengage when a range no longer meets the criteria
// - During split:
//  - Record start time
//  - Keep a sample of 10 keys
//   - Each sample contains two counters: left and right.
//   - On each span, increment the left and/or right counters, depending
//     on whether the span falls entirely to the left, to the right, or
//     overlaps the key. If exactly on the key, increment neither.
//   - When a sample is replaced, discard its counters.
//  - If a range is on for more than a threshold interval:
//   - Examine sample for the smallest diff between left and right counters,
//     excluding any whose counters are not sufficiently advanced;
//     If not less than some constant threshold, skip split.
//   - If this point is reached, add range to split queue with the chosen
//     key as split key, and provide hint to scatter the replicas.

const (
	durationThreshold  = 10 * time.Second // 10s
	splitKeySampleSize = 20               // size of split key sample
	splitKeyMinCounter = 200              // min aggregate counters before consideration
	splitKeyThreshold  = 0.25             // 25% difference between left/right counters
)

type sample struct {
	key         roachpb.Key
	left, right int
}

type Split struct {
	startNanos int64
	samples    [splitKeySampleSize]sample
	count      int
}

func New(startNanos int64) *Split {
	return &Split{
		startNanos: startNanos,
	}
}

func (s *Split) Ready(nowNanos int64) bool {
	return time.Duration(nowNanos-s.startNanos) > durationThreshold
}

func (s *Split) Record(span roachpb.Span, intNFn func(int) int) {
	if s == nil {
		return
	}

	var idx int
	count := s.count
	s.count++
	if count < splitKeySampleSize {
		idx = count
	} else if idx = intNFn(count); idx >= splitKeySampleSize {
		// Increment all existing keys' counters.
		for i, sa := range s.samples {
			if span.ContainsKey(sa.key) {
				sa.left++
				sa.right++
			} else {
				if comp := bytes.Compare(sa.key, span.Key); comp < 0 {
					sa.right++
				} else if comp > 0 {
					sa.left++
				}
			}
			s.samples[i] = sa
		}
		return
	}

	// Note we always use the start key of the span. We could
	// take the average of the byte slices, but that seems
	// unnecessarily complex for practical usage.
	s.samples[idx] = sample{key: span.Key}
}

func (s *Split) Key() (bool, roachpb.Key) {
	if s == nil {
		return false, nil
	}

	var bestIdx int = -1
	var bestScore float64 = 1
	for i, s := range s.samples {
		if s.left+s.right < splitKeyMinCounter {
			continue
		}
		score := math.Abs(float64(s.left-s.right)) / float64(s.left+s.right)
		if score >= splitKeyThreshold {
			continue
		}
		if score < bestScore {
			bestIdx = i
			bestScore = score
		}
	}

	if bestIdx == -1 {
		return false, nil
	}
	return true, s.samples[bestIdx].key
}
