// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mutations

import (
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

var maxBatchSize int64 = defaultMaxBatchSize

const defaultMaxBatchSize = 10000

// MaxBatchSize returns the max number of entries in the KV batch for a
// mutation operation (delete, insert, update, upsert) - including secondary
// index updates, FK cascading updates, etc - before the current KV batch is
// executed and a new batch is started.
func MaxBatchSize() int {
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

// MutationsTestingMaxBatchSize is a testing cluster setting that sets the
// default max mutation batch size. A low max batch size is useful to test
// batching logic of the mutations.
var MutationsTestingMaxBatchSize = settings.RegisterNonNegativeIntSetting(
	"sql.testing.mutations.max_batch_size",
	"the max number of rows that are processed by a single KV batch when performing a mutation operation (0=default)",
	0,
)
