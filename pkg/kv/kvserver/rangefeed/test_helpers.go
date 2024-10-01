// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// GenerateRandomizedTs generates a timestamp between 1 and ns nanoseconds.
func GenerateRandomizedTs(rand *rand.Rand, maxTime int64) hlc.Timestamp {
	// Avoid generating zero timestamp which will equal to an empty event.
	return hlc.Timestamp{WallTime: rand.Int63n(maxTime) + 1}
}

func generateStartAndEndKey(rand *rand.Rand) (roachpb.Key, roachpb.Key) {
	startKey, endKey, _ := generateStartAndEndKeyFromK(rand, 0)
	return startKey, endKey
}

// generateStartAndEndKey generates a start key at or above k, an end key at or
// above the start key, and returns the keys and the value of the end key.
func generateStartAndEndKeyFromK(rand *rand.Rand, k int) (roachpb.Key, roachpb.Key, int) {
	start := k + rand.Intn(2<<20)
	end := start + rand.Intn(2<<20)
	startDatum := tree.NewDInt(tree.DInt(start))
	endDatum := tree.NewDInt(tree.DInt(end))
	const tableID = 42

	startKey, err := keyside.Encode(
		keys.SystemSQLCodec.TablePrefix(tableID),
		startDatum,
		encoding.Ascending,
	)
	if err != nil {
		panic(err)
	}

	endKey, err := keyside.Encode(
		keys.SystemSQLCodec.TablePrefix(tableID),
		endDatum,
		encoding.Ascending,
	)
	if err != nil {
		panic(err)
	}
	return startKey, endKey, end
}

// GenerateRandomizedSpans generates n non-overlapping spans.
func GenerateRandomizedSpans(rand *rand.Rand, n int) []roachpb.RSpan {
	spans := make([]roachpb.RSpan, 0, n)
	var startKey, endKey roachpb.Key
	k := 0
	for range n {
		startKey, endKey, k = generateStartAndEndKeyFromK(rand, k)
		spans = append(spans, roachpb.RSpan{
			Key:    roachpb.RKey(startKey),
			EndKey: roachpb.RKey(endKey),
		})
	}
	return spans
}
