// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package split

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type weightedSample struct {
	key         roachpb.Key
	weight      float64
	left, right float64
	count       int
}

type RandSource interface {
	Float64() float64
	Intn(n int) int
}

type WeightedFinder struct {
	samples     [splitKeySampleSize]weightedSample
	count       int
	totalWeight float64
	startTime   time.Time
	randSource  RandSource
}

func NewWeightedFinder(startTime time.Time, randSource RandSource) *WeightedFinder {
	return &WeightedFinder{
		startTime:  startTime,
		randSource: randSource,
	}
}

// Ready checks if the WeightedFinder has been initialized with a sufficient
// sample duration.
func (f *WeightedFinder) Ready(nowTime time.Time) bool {
	return nowTime.Sub(f.startTime) > RecordDurationThreshold
}

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
			if comp := key.Compare(f.samples[i].key); comp < 0 {
				// Case key < f.samples[i].Key (left is exclusive to split key).
				f.samples[i].left += weight
			} else {
				// Case key >= f.samples[i].Key (right is inclusive to split key).
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

func (f *WeightedFinder) Record(span roachpb.Span, weight float64) {
	if span.EndKey == nil {
		f.record(span.Key, weight)
	} else {
		f.record(span.Key, weight/2)
		f.record(span.EndKey, weight/2)
	}
}

// Key finds an appropriate split point based on the Reservoir sampling method.
// Returns a nil key if no appropriate key was found.
func (f *WeightedFinder) Key() roachpb.Key {
	if f == nil {
		return nil
	}

	var bestIdx = -1
	var bestScore float64 = 1
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

// NoSplitKeyCauseLogMsg returns a log message containing all of this
// information if not all samples are invalid due to insufficient counters,
// otherwise returns an empty string.
func (f *WeightedFinder) NoSplitKeyCauseLogMsg() string {
	insufficientCounters, imbalance := f.noSplitKeyCause()
	if insufficientCounters == splitKeySampleSize {
		return ""
	}
	noSplitKeyCauseLogMsg := fmt.Sprintf("No split key found: insufficient counters = %d, imbalance = %d", insufficientCounters, imbalance)
	return noSplitKeyCauseLogMsg
}

// PopularKeyFrequency returns the percentage of the total weight that the most
// popular key (as measured by weight) appears in f.samples.
func (f *WeightedFinder) PopularKeyFrequency() float64 {
	sort.Slice(f.samples[:], func(i, j int) bool {
		return f.samples[i].key.Compare(f.samples[j].key) < 0
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
