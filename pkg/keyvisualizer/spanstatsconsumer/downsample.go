// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanstatsconsumer

import (
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

const notImportant = -1
const mediumImportance = 0
const important = 1

// bucketImportance is the heuristic to decide to merge buckets.
// buckets with values less than the mean and the median and deemed
// NotImportant, and thus are merge candidates.
func bucketImportance(value, mean, median float64) int {
	if value <= mean && value <= median {
		return notImportant
	}

	if value >= mean && value >= median {
		return important
	}

	return mediumImportance
}

// mergeBuckets averages 2 or more buckets into one.
// The resulting span start key is from the first bucket,
// and the resulting span end key is from the last bucket.
func mergeBuckets(buckets []keyvispb.SpanStats) keyvispb.SpanStats {

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

	return keyvispb.SpanStats{
		Span:     span,
		Requests: uint64(math.Ceil(requests)),
	}
}

// aggressiveAggregate does not consider bucket importance
// and will recursively merge adjacent buckets until the total number of buckets
// is <= maxBuckets.
func aggressiveAggregate(spanStats []keyvispb.SpanStats, maxBuckets int) []keyvispb.SpanStats {
	if len(spanStats) <= maxBuckets {
		return spanStats
	}

	var ret []keyvispb.SpanStats

	// Merge adjacent buckets regardless of importance.
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

// aggregate considers the importance of each bucket and merges statistically
// unimportant buckets together. Its purpose is to preserve resolution in the
// parts of the keyspace that are interesting, while removing resolution from
// the parts of the keyspace that are not interesting. The return value's length
// will be >= 1 and <= len(spanStats).
func aggregate(spanStats []keyvispb.SpanStats, mean float64, median float64) []keyvispb.SpanStats {

	var ret []keyvispb.SpanStats

	i := 0
	for {

		if i >= len(spanStats) {
			break
		}

		bucket := spanStats[i]

		// There's no possibility of an adjacent bucket at the end
		// of the slice. break.
		if i == len(spanStats)-1 {
			ret = append(ret, bucket)
			break
		}

		// Calculate this bucket's importance.
		importanceScore := bucketImportance(float64(bucket.Requests), mean, median)

		// Only notImportant buckets should be aggregated, so if this bucket is
		// important or mediumImportance, leave it alone.
		if importanceScore == important || importanceScore == mediumImportance {
			ret = append(ret, bucket)
			i++
			continue
		}

		// Starting from this index, find the subsequent adjacent buckets that
		// are not important. This collection of adjacent not-important buckets
		// will comprise a group that will be merged into one via mergeBuckets.
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

			if nextBucketScore == notImportant {
				i++
				groupSize++
			} else {
				break
			}
		}

		if groupSize == 1 {
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
	}
	return s[(n-1)/2]
}

// downsample performs a 2-step downsampling scheme. First, adjacent unimportant
// buckets are merged together. Optionally, second, buckets are merged with
// their neighbor recursively until maxBuckets is reached.
func downsample(spanStats []keyvispb.SpanStats, maxBuckets int) []keyvispb.SpanStats {

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
