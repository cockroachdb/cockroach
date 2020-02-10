// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestHashFunctionFamily verifies the assumption that our vectorized hashing
// function (the combination of initHash, rehash, and finalizeHash) actually
// defines a function family and that changing the initial hash value is
// sufficient to get a "different" hash function.
func TestHashFunctionFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	bucketsA, bucketsB := make([]uint64, coldata.BatchSize()), make([]uint64, coldata.BatchSize())
	nKeys := uint64(coldata.BatchSize())
	keyTypes := []coltypes.T{coltypes.Int64}
	keys := []coldata.Vec{testAllocator.NewMemColumn(keyTypes[0], int(coldata.BatchSize()))}
	for i := int64(0); i < int64(coldata.BatchSize()); i++ {
		keys[0].Int64()[i] = i
	}
	numBuckets := uint64(16)
	var (
		cancelChecker  CancelChecker
		decimalScratch decimalOverloadScratch
	)

	for initHashValue, buckets := range [][]uint64{bucketsA, bucketsB} {
		// We need +1 here because 0 is not a valid initial hash value.
		initHash(buckets, nKeys, uint64(initHashValue+1))
		for i, typ := range keyTypes {
			rehash(ctx, buckets, typ, keys[i], nKeys, nil /* sel */, cancelChecker, decimalScratch)
		}
		finalizeHash(buckets, nKeys, numBuckets)
	}

	numKeysInSameBucket := uint64(0)
	for key := range bucketsA {
		if bucketsA[key] == bucketsB[key] {
			numKeysInSameBucket++
		}
	}
	// We expect that about 1/numBuckets keys remained in the same bucket, so if
	// the actual number deviates by more than a factor of 3, we fail the test.
	if nKeys*3/numBuckets < numKeysInSameBucket {
		t.Fatal(fmt.Sprintf("too many keys remained in the same bucket: expected about %d, actual %d",
			nKeys/numBuckets, numKeysInSameBucket))
	}
}
