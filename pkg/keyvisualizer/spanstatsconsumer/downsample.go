package spanstatsconsumer

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanstats/spanstatspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"math"
	"sort"
)

const NotImportant = -1
const MediumImportance = 0
const Important = 1


// bucketImportance is the heuristic to decide to merge buckets.
// buckets with values less than the mean and the median and deemed
// NotImportant, and thus are merge candidates.
func bucketImportance(value, mean, median float64) int {
	if value <= mean && value <= median {
		return NotImportant
	}

	if value >= mean && value >= median {
		return Important
	}

	return MediumImportance
}


// mergeBuckets averages 2 or more buckets into one.
// The resulting span start key is from the first bucket,
// and the resulting span end key is from the last bucket.
func mergeBuckets(buckets []*spanstatspb.SpanStats) *spanstatspb.SpanStats {

	first := buckets[0]
	last := buckets[len(buckets)-1]

	requests := 0.0
	for _, bucket := range buckets {
		requests += float64(bucket.Requests)
	}
	requests = requests / float64(len(buckets))

	span := roachpb.Span{
		Key:    first.Span.Key.Clone(),
		EndKey: last.Span.EndKey.Clone(),
	}

	return &spanstatspb.SpanStats{
		Span:     &span,
		Requests: uint64(math.Ceil(requests)),
	}
}

// aggressiveAggregate does not consider bucket importance
// and will recursively merge adjacent buckets until the total number of buckets
// is lte maxBuckets.
func aggressiveAggregate(
	spanStats []*spanstatspb.SpanStats,
	maxBuckets int,
) []*spanstatspb.SpanStats {
	if len(spanStats) <= maxBuckets {
		return spanStats
	}

	ret := make([]*spanstatspb.SpanStats, 0)

	// merge adjacent buckets regardless of importance
	for i := 0; i < len(spanStats); i += 2 {
		if len(spanStats)-1 == i {
			ret = append(ret, spanStats[i])
		} else {
			merged := mergeBuckets(spanStats[i : i+2])
			ret = append(ret, merged)
		}
	}

	return aggressiveAggregate(ret, maxBuckets)
}


// aggregate considers the importance of each bucket and merges unimportant
// buckets together.
func aggregate(
	spanStats []*spanstatspb.SpanStats,
	mean float64,
	median float64) []*spanstatspb.SpanStats {

	ret := make([]*spanstatspb.SpanStats, 0)

	i := 0
	for {

		if i >= len(spanStats) {
			break
		}

		bucket := spanStats[i]

		// at the end, no possibility of an adjacent bucket
		if i == len(spanStats)-1 {
			ret = append(ret, bucket)
			break
		}

		importanceScore := bucketImportance(float64(bucket.Requests), mean, median)

		// keep important buckets
		if importanceScore == Important || importanceScore == MediumImportance {
			ret = append(ret, bucket)
			i++
			continue
		}

		// merge adjacent buckets that are not important.
		groupSize := 1
		start := i
		for {

			if i >= len(spanStats)-1 {
				break
			}

			nextBucket := spanStats[i+1]
			nextBucketScore := bucketImportance(
				float64(nextBucket.Requests),
				mean,
				median)

			if nextBucketScore == NotImportant {
				i++
				groupSize++
			} else {
				break
			}
		}

		if groupSize == 1 {
			// the next adjacent bucket was important
			ret = append(ret, bucket)
			i++
		} else {
			toMerge := spanStats[start : start+groupSize]
			ret = append(ret, mergeBuckets(toMerge))
		}
	}

	return ret
}

func mean(s []float64) float64 {
	if len(s) == 0 {
		return 0.0
	}

	sum := 0.0

	for _, x := range s {
		sum += x
	}

	return sum / float64(len(s))
}

func median(s []float64) float64 {
	sort.Float64s(s)
	n := len(s)
	if n%2 == 0 {
		return (s[n/2] + s[(n/2)-1]) / 2.0
	} else {
		return s[(n-1)/2]
	}
}


// downsample performs a 2-step downsampling scheme.
// First, adjacent unimportant buckets are merged together
// Optionally, second,
// buckets are merged with their neighbor recursively until maxBuckets is
// reached.
func downsample(
	spanStats []*spanstatspb.SpanStats,
	maxBuckets int,
) []*spanstatspb.SpanStats {

	if len(spanStats) <= maxBuckets {
		return spanStats
	}

	reqs := make([]float64, 0)
	for _, bucket := range spanStats {
		reqs = append(reqs, float64(bucket.Requests))
	}

	// aggregation requires that spans are contiguous,
	// so they must be lexicographically sorted.
	sort.Slice(spanStats, func(a, b int) bool {
		return spanStats[a].Span.Key.Compare(
			spanStats[b].Span.Key) == -1
	})

	smaller := aggregate(spanStats, mean(reqs), median(reqs))
	smallest := aggressiveAggregate(smaller, maxBuckets)
	return smallest
}
