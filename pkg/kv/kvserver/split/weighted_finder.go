// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package split

import (
	"bytes"
	"math"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/redact"
)

// Load-based splitting.
//
// - Engage split for ranges:
//  - With size exceeding min-range-bytes
//  - with reqs/s rate over a configurable threshold
// - Disengage when a range no longer meets the criteria
// - During split:
//  - Record start time
//  - Keep a sample of 20 keys using weighted reservoir sampling (a simplified
//    version of A-Chao algorithm). For more information on A-Chao, see
//    https://en.wikipedia.org/wiki/Reservoir_sampling#Algorithm_A-Chao or
//    https://arxiv.org/pdf/1012.0256.pdf.
//   - Each sample contains two counters: left and right.
//   - For a weighted point span (key, weight), record that as is; for a
//     weighted ranged span ([key, endKey), weight), record that as two
//     weighted point spans: (key, weight/2) and (endKey, weight/2).
//   - On each weighted point span, increment the left and/or right counters by
//     the weight, depending on whether the key falls to the left or to the
//     right.
//   - When a sample is replaced, discard its counters.
//  - If a range is on for more than a threshold interval:
//   - Examine sample for the smallest diff between left and right counters,
//     excluding any whose counters are not sufficiently advanced;
//     If not less than some constant threshold, skip split.
//   - If a desired point is reached, add range to split queue with the chosen
//     key as split key, and provide hint to scatter the replicas.

type weightedSample struct {
	key         roachpb.Key
	weight      float64
	left, right float64
	count       int
}

// SafeFormat implements the redact.SafeFormatter interface.
func (ws weightedSample) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("%s(l=%.1f r=%.1f c=%d w=%.1f)",
		ws.key, ws.left, ws.right, ws.count, ws.weight)
}

func (ws weightedSample) String() string {
	return redact.StringWithoutMarkers(ws)
}

// WeightedFinder is a structure that is used to determine the split point
// using the Weighted Reservoir Sampling method (a simplified version of A-Chao
// algorithm).
type WeightedFinder struct {
	samples     [splitKeySampleSize]weightedSample
	count       int
	totalWeight float64
	startTime   time.Time
	randSource  RandSource
}

// NewWeightedFinder initiates a WeightedFinder with the given time.
func NewWeightedFinder(startTime time.Time, randSource RandSource) *WeightedFinder {
	return &WeightedFinder{
		startTime:  startTime,
		randSource: randSource,
	}
}

// Ready implements the LoadBasedSplitter interface.
func (f *WeightedFinder) Ready(nowTime time.Time) bool {
	return nowTime.Sub(f.startTime) > RecordDurationThreshold
}

// record informs the WeightedFinder about where the incoming point span (i.e.
// key) lies with regard to the candidate split keys. We use weighted reservoir
// sampling (a simplified version of A-Chao algorithm) to get the candidate
// split keys.
func (f *WeightedFinder) record(key roachpb.Key, weight float64) {
	if f == nil {
		return
	}

	var idx int
	count := f.count
	f.count++
	f.totalWeight += weight
	if count < splitKeySampleSize {
		idx = count
	} else if f.randSource.Float64() > splitKeySampleSize*weight/f.totalWeight {
		for i := range f.samples {
			// Example: Suppose we have candidate split key = "k" (i.e.
			// f.samples[i].Key).
			//
			// Suppose we record the following weighted keys:
			// record("i", 3)    x                       Increment left counter by 3
			// record("k", 5)              x             Increment right counter by 5
			// record("l", 1)                   x        Increment right counter by 1
			//                   |----|----|----|----|
			//                  "i"  "j"  "k"  "l"  "m"
			//                             ^
			//                    Candidate split key
			// Left range split  [         )
			// Right range split           [         )
			if comp := key.Compare(f.samples[i].key); comp < 0 {
				// Case key < f.samples[i].Key i.e. key is to the left of the candidate
				// split key (left is exclusive to split key).
				f.samples[i].left += weight
			} else {
				// Case key >= f.samples[i].Key i.e. key is to the right of or on the
				// candidate split key (right is inclusive to split key).
				f.samples[i].right += weight
			}
			f.samples[i].count++
		}
		return
	} else {
		idx = f.randSource.Intn(splitKeySampleSize)
	}

	// Note we always use the start key of the span. We could
	// take the average of the byte slices, but that seems
	// unnecessarily complex for practical usage.
	f.samples[idx] = weightedSample{key: key, weight: weight}
}

// Record implements the LoadBasedSplitter interface.
//
// Note that we treat a weighted range request ([start, end), w) as two
// weighted point requests (start, w/2) and (end, w/2). The motivation for this
// is that within the range [start, end), we do not know anything about the
// underlying data distribution without complex methods to retrieve such
// information. Moreover, we do not even know what keys are within this range
// since all keys are byte arrays, and consequently we have no concept of
// “distance” or “midpoint” between a pair of keys. The most basic approach is
// to be agnostic to the underlying data distribution and relative distance
// between keys, and simply assume that a weighted range request that contains
// a candidate split key will contribute half of its weight to the left counter
// and half of its weight to the right counter of that candidate split key.
func (f *WeightedFinder) Record(span roachpb.Span, weight float64) {
	if span.EndKey == nil {
		f.record(span.Key, weight)
	} else {
		f.record(span.Key, weight/2)
		f.record(span.EndKey, weight/2)
	}
}

// Key implements the LoadBasedSplitter interface. Key returns the candidate
// split key that minimizes the balance score (percentage difference between
// the left and right counters), provided the balance score is < 0.25.
func (f *WeightedFinder) Key() roachpb.Key {
	if f == nil {
		return nil
	}

	var bestIdx = -1
	var bestScore float64 = 1
	// For simplicity, we suppose splitKeyMinCounter = 5.
	//
	// Example 1 (numbers refer to weights of requests):
	// 1    |
	//    2 |
	//      | 1
	//      |    1
	//      |
	//  split key
	// s.left = 3
	// s.right = 2
	// s.count = 4
	// Invalid split key because insufficient counters
	// (s.count < splitKeyMinCounter).
	//
	// Example 2 (numbers refer to weights of requests):
	// 1    |
	//    2 |
	//      | 1
	//      |    1
	//      |      3
	//  split key
	// s.left = 3
	// s.right = 5
	// balance score = |3 - 5| / (3 + 5) = 0.25
	// Invalid split key because imbalance in left and right counters i.e.
	// balance score >= splitKeyThreshold.
	//
	// Example 3:
	// 1    |  |
	//  2   |  |
	//    2 |  |
	//      | 1|
	//      |  | 1
	//      |  |   6
	//     sk1
	//        sk2
	// balance score of sk1 = 0.23
	// balance score of sk2 = 0.08
	// We choose split key sk2 because it has the lowest balance score.
	for i, s := range f.samples {
		if s.count < splitKeyMinCounter {
			continue
		}
		balanceScore := math.Abs(s.left-s.right) / (s.left + s.right)
		if balanceScore >= splitKeyThreshold {
			continue
		}
		if balanceScore < bestScore {
			bestIdx = i
			bestScore = balanceScore
		}
	}

	if bestIdx == -1 {
		return nil
	}
	return f.samples[bestIdx].key
}

// noSplitKeyCause iterates over all sampled candidate split keys and
// determines the number of samples that don't pass each split key requirement
// (e.g. insufficient counters, imbalance in left and right counters).
func (f *WeightedFinder) noSplitKeyCause() (insufficientCounters, imbalance int) {
	for _, s := range f.samples {
		if s.count < splitKeyMinCounter {
			insufficientCounters++
		} else if balanceScore := math.Abs(s.left-s.right) / (s.left + s.right); balanceScore >= splitKeyThreshold {
			imbalance++
		}
	}
	return
}

// NoSplitKeyCauseLogMsg implements the LoadBasedSplitter interface.
func (f *WeightedFinder) NoSplitKeyCauseLogMsg() redact.RedactableString {
	insufficientCounters, imbalance := f.noSplitKeyCause()
	if insufficientCounters == splitKeySampleSize {
		return ""
	}
	return redact.Sprintf(
		"no split key found: insufficient counters = %d, imbalance = %d",
		insufficientCounters, imbalance)
}

// PopularKeyFrequency implements the LoadBasedSplitter interface.
func (f *WeightedFinder) PopularKeyFrequency() float64 {
	// Sort the sample slice to determine the frequency that a popular key
	// appears. We could copy the slice, however it would require an allocation.
	// The probability a sample is replaced doesn't change as it is independent
	// of position.
	slices.SortFunc(f.samples[:], func(a, b weightedSample) int {
		return bytes.Compare(a.key, b.key)
	})

	weight := f.samples[0].weight
	currentKeyWeight := weight
	popularKeyWeight := weight
	totalWeight := weight
	for i := 1; i < len(f.samples); i++ {
		weight := f.samples[i].weight
		if f.samples[i].key.Equal(f.samples[i-1].key) {
			currentKeyWeight += weight
		} else {
			currentKeyWeight = weight
		}
		if popularKeyWeight < currentKeyWeight {
			popularKeyWeight = currentKeyWeight
		}
		totalWeight += weight
	}

	return popularKeyWeight / totalWeight
}

// SafeFormat implements the redact.SafeFormatter interface.
func (f *WeightedFinder) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("key=%v start=%v count=%d total=%.2f samples=%v",
		f.Key(), f.startTime, f.count, f.totalWeight, f.samples)
}

func (f *WeightedFinder) String() string {
	return redact.StringWithoutMarkers(f)
}
