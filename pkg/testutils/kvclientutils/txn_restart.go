// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvclientutils

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

var randRetryRngSource, _ = randutil.NewLockedPseudoRand()

func RandomTransactionRetryFilter() func(*kv.Txn) bool {
	return func(*kv.Txn) bool {
		return randRetryRngSource.Float64() < kv.RandomTxnRetryProbability
	}
}

func PrefixTransactionRetryFilter(
	t testutils.TB, prefix string, maxCount int,
) (func(*kv.Txn) bool, func()) {
	var count int
	var mu syncutil.Mutex
	verifyFunc := func() {
		mu.Lock()
		defer mu.Unlock()
		if count == 0 {
			t.Errorf("expected at least 1 transaction to match prefix %q", prefix)
		}
	}
	filterFunc := func(txn *kv.Txn) bool {
		// Use DebugNameLocked because txn is locked by the caller.
		if !strings.HasPrefix(txn.DebugNameLocked(), prefix) {
			return false
		}

		mu.Lock()
		defer mu.Unlock()
		if count < maxCount {
			count++
			return true
		}
		return false
	}

	return filterFunc, verifyFunc
}
