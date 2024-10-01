// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvclientutils

import (
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

func RandomTransactionRetryFilter() func(roachpb.Transaction) bool {
	return func(roachpb.Transaction) bool {
		return rand.Float64() < kv.RandomTxnRetryProbability
	}
}

func PrefixTransactionRetryFilter(
	t testutils.TestErrorer, prefix string, maxCount int,
) (func(roachpb.Transaction) bool, func()) {
	var count int
	var mu syncutil.Mutex
	verifyFunc := func() {
		mu.Lock()
		defer mu.Unlock()
		if count == 0 {
			t.Errorf("expected at least 1 transaction to match prefix %q", prefix)
		}
	}
	filterFunc := func(txn roachpb.Transaction) bool {
		if !strings.HasPrefix(txn.Name, prefix) {
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
