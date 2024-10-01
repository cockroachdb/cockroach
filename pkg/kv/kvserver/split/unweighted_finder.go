// Copyright 2018 The Cockroach Authors.
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
//  - Keep a sample of 20 keys
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
	// RecordDurationThreshold is the minimum duration of time the split finder
	// will record a range for, before being ready for a split.
	RecordDurationThreshold    = 10 * time.Second // 10s
	splitKeySampleSize         = 20               // size of split key sample
	splitKeyMinCounter         = 100              // min aggregate counters before consideration
	splitKeyThreshold          = 0.25             // 25% difference between left/right counters
	splitKeyContainedThreshold = 0.50             // too many spanning queries over split point
)

type sample struct {
	key                    roachpb.Key
	left, right, contained int
}

// SafeFormat implements the redact.SafeFormatter interface.
func (s sample) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("%s(l=%d r=%d c=%d)",
		s.key, s.left, s.right, s.contained)
}

func (s sample) String() string {
	return redact.StringWithoutMarkers(s)
}

// UnweightedFinder is a structure that is used to determine the split point
// using the Reservoir Sampling method.
type UnweightedFinder struct {
	startTime  time.Time
	randSource RandSource
	samples    [splitKeySampleSize]sample
	count      int
}

// NewUnweightedFinder initiates a UnweightedFinder with the given time.
func NewUnweightedFinder(startTime time.Time, randSource RandSource) *UnweightedFinder {
	return &UnweightedFinder{
		startTime:  startTime,
		randSource: randSource,
	}
}

// Ready implements the LoadBasedSplitter interface.
func (f *UnweightedFinder) Ready(nowTime time.Time) bool {
	return nowTime.Sub(f.startTime) > RecordDurationThreshold
}

// Record implements the LoadBasedSplitter interface. Record uses reservoir
// sampling to get the candidate split keys.
func (f *UnweightedFinder) Record(span roachpb.Span, weight float64) {
	if f == nil {
		return
	}

	var idx int
	count := f.count
	f.count++
	if count < splitKeySampleSize {
		idx = count
	} else if idx = f.randSource.Intn(count); idx >= splitKeySampleSize {
		// Increment all existing keys' counters.
		for i := range f.samples {
			if span.ProperlyContainsKey(f.samples[i].key) {
				f.samples[i].contained++
			} else {
				// If the split is chosen to be here and the key is on or to the left
				// of the start key of the span, we know that the request the span represents
				// - would be isolated to the right of the split point.
				// Similarly, if the split key is greater than the start key of the span
				// (and given that it is not properly contained by the span) it must mean
				// that the request the span represents would be on the left.
				if comp := bytes.Compare(f.samples[i].key, span.Key); comp <= 0 {
					f.samples[i].right++
				} else if comp > 0 {
					f.samples[i].left++
				}
			}
		}
		return
	}

	// Note we always use the start key of the span. We could
	// take the average of the byte slices, but that seems
	// unnecessarily complex for practical usage.
	f.samples[idx] = sample{key: span.Key}
}

// Key implements the LoadBasedSplitter interface. Key returns the candidate
// split key that minimizes the sum of the balance score (percentage difference
// between the left and right counters) and the contained score (percentage of
// counters are contained), provided the balance score is < 0.25 and the
// contained score is < 0.5.
func (f *UnweightedFinder) Key() roachpb.Key {
	if f == nil {
		return nil
	}

	var bestIdx = -1
	var bestScore float64 = 2
	for i, s := range f.samples {
		if s.left+s.right+s.contained < splitKeyMinCounter {
			continue
		}
		balanceScore := math.Abs(float64(s.left-s.right)) / float64(s.left+s.right)
		containedScore := (float64(s.contained) / float64(s.left+s.right+s.contained))
		finalScore := balanceScore + containedScore
		if balanceScore >= splitKeyThreshold ||
			containedScore >= splitKeyContainedThreshold {
			continue
		}
		if finalScore < bestScore {
			bestIdx = i
			bestScore = finalScore
		}
	}

	if bestIdx == -1 {
		return nil
	}
	return f.samples[bestIdx].key
}

// NoSplitKeyCause iterates over all sampled candidate split keys and
// determines the number of samples that don't pass each split key requirement
// (e.g. insufficient counters, imbalance in left and right counters, too many
// contained counters, or a combination of the last two).
func (f *UnweightedFinder) noSplitKeyCause() (
	insufficientCounters, imbalance, tooManyContained, imbalanceAndTooManyContained int,
) {
	for _, s := range f.samples {
		if s.left+s.right+s.contained < splitKeyMinCounter {
			insufficientCounters++
		} else {
			balanceScore := math.Abs(float64(s.left-s.right)) / float64(s.left+s.right)
			imbalanceBool := balanceScore >= splitKeyThreshold
			containedScore := float64(s.contained) / float64(s.left+s.right+s.contained)
			tooManyContainedBool := containedScore >= splitKeyContainedThreshold
			if imbalanceBool && !tooManyContainedBool {
				imbalance++
			} else if !imbalanceBool && tooManyContainedBool {
				tooManyContained++
			} else if imbalanceBool && tooManyContainedBool {
				imbalanceAndTooManyContained++
			}
		}
	}
	return
}

// NoSplitKeyCauseLogMsg implements the LoadBasedSplitter interface.
func (f *UnweightedFinder) NoSplitKeyCauseLogMsg() redact.RedactableString {
	insufficientCounters, imbalance, tooManyContained, imbalanceAndTooManyContained := f.noSplitKeyCause()
	if insufficientCounters == splitKeySampleSize {
		return ""
	}
	return redact.Sprintf(
		"no split key found: insufficient counters = %d, imbalance = %d, "+
			"too many contained = %d, imbalance and too many contained = %d",
		insufficientCounters, imbalance, tooManyContained,
		imbalanceAndTooManyContained)
}

// PopularKeyFrequency implements the LoadBasedSplitter interface.
func (f *UnweightedFinder) PopularKeyFrequency() float64 {
	slices.SortFunc(f.samples[:], func(a, b sample) int {
		return bytes.Compare(a.key, b.key)
	})

	currentKeyCount := 1
	popularKeyCount := 1
	for i := 1; i < len(f.samples); i++ {
		if bytes.Equal(f.samples[i].key, f.samples[i-1].key) {
			currentKeyCount++
		} else {
			currentKeyCount = 1
		}
		if popularKeyCount < currentKeyCount {
			popularKeyCount = currentKeyCount
		}
	}

	return float64(popularKeyCount) / float64(splitKeySampleSize)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (f *UnweightedFinder) SafeFormat(w redact.SafePrinter, r rune) {
	w.Printf("key=%v start=%v count=%d samples=%v",
		f.Key(), f.startTime, f.count, f.samples)
}

func (f *UnweightedFinder) String() string {
	return redact.StringWithoutMarkers(f)
}
