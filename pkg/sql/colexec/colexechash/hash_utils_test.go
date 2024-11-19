// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexechash

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestHashFunctionFamily verifies the assumption that our vectorized hashing
// function (the combination of initHash, rehash, and finalizeHash) actually
// defines a function family and that changing the initial hash value is
// sufficient to get a "different" hash function.
func TestHashFunctionFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	bucketsA, bucketsB := make([]uint32, coldata.BatchSize()), make([]uint32, coldata.BatchSize())
	nKeys := coldata.BatchSize()
	keyTypes := []*types.T{types.Int}
	keys := []*coldata.Vec{testAllocator.NewVec(keyTypes[0], coldata.BatchSize())}
	for i := int64(0); i < int64(coldata.BatchSize()); i++ {
		keys[0].Int64()[i] = i
	}
	numBuckets := uint32(16)
	var (
		cancelChecker colexecutils.CancelChecker
		datumAlloc    tree.DatumAlloc
	)
	cancelChecker.Init(context.Background())

	for initHashValue, buckets := range [][]uint32{bucketsA, bucketsB} {
		// We need +1 here because 0 is not a valid initial hash value.
		initHash(buckets, nKeys, uint32(initHashValue+1))
		for _, keysCol := range keys {
			rehash(buckets, keysCol, nKeys, nil /* sel */, cancelChecker, &datumAlloc)
		}
		finalizeHash(buckets, nKeys, numBuckets)
	}

	numKeysInSameBucket := 0
	for key := range bucketsA {
		if bucketsA[key] == bucketsB[key] {
			numKeysInSameBucket++
		}
	}
	// We expect that about 1/numBuckets keys remained in the same bucket, so if
	// the actual number deviates by more than a factor of 3, we fail the test.
	if nKeys*3/int(numBuckets) < numKeysInSameBucket {
		t.Fatalf("too many keys remained in the same bucket: expected about %d, actual %d",
			nKeys/int(numBuckets), numKeysInSameBucket)
	}
}
