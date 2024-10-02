// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mutations

import (
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
)

const productionMaxBatchSize = 10000

var maxBatchSize = defaultMaxBatchSize

var defaultMaxBatchSize = int64(metamorphic.ConstantWithTestRange(
	"max-batch-size",
	productionMaxBatchSize, /* defaultValue */
	1,                      /* min */
	productionMaxBatchSize, /* max */
))

var testingMaxBatchByteSize = metamorphic.ConstantWithTestRange(
	"max-batch-byte-size",
	0,      // we'll use the cluster setting instead if we see zero.
	1,      /* min */
	32<<20, /* max */
)

// MaxBatchSize returns the max number of entries in the KV batch for a
// mutation operation (delete, insert, update, upsert) - including secondary
// index updates, FK cascading updates, etc - before the current KV batch is
// executed and a new batch is started.
//
// If forceProductionMaxBatchSize is true, then the "production" value will be
// returned regardless of whether the build is metamorphic or not. This should
// only be used by tests the output of which differs if maxBatchSize is
// randomized.
func MaxBatchSize(forceProductionMaxBatchSize bool) int {
	if forceProductionMaxBatchSize {
		return productionMaxBatchSize
	}
	return int(atomic.LoadInt64(&maxBatchSize))
}

// SetMaxBatchSizeForTests modifies maxBatchSize variable. It
// should only be used in tests.
func SetMaxBatchSizeForTests(newMaxBatchSize int) {
	atomic.SwapInt64(&maxBatchSize, int64(newMaxBatchSize))
}

// ResetMaxBatchSizeForTests resets the maxBatchSize variable to
// the default mutation batch size. It should only be used in tests.
func ResetMaxBatchSizeForTests() {
	atomic.SwapInt64(&maxBatchSize, defaultMaxBatchSize)
}

// MaxBatchByteSize takes the passed value read from the cluster setting and
// returns it unless the testing metamorphic value overrides it.
func MaxBatchByteSize(clusterSetting int, forceProductionBatchSizes bool) int {
	if forceProductionBatchSizes || testingMaxBatchByteSize == 0 {
		return clusterSetting
	}
	return testingMaxBatchByteSize
}
