// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// TestingRequestFilterRetryTxnWithPrefix returns a TestingRequestFilter that
// forces maxCount retry errors for each transaction whose debug name
// begins with the given prefix.
//
// Example:
//
//	base.TestingKnobs{
//		Store: &kvserver.StoreTestingKnobs{
//		 	TestingRequestFilter: testutils.TestingRequestFilterRetryTxnWithPrefix("ttljob-", 1),
//		},
//	}
func TestingRequestFilterRetryTxnWithPrefix(
	prefix string, maxCount int,
) func(ctx context.Context, r *kvpb.BatchRequest) *kvpb.Error {
	txnTracker := struct {
		syncutil.Mutex
		retryCounts map[string]int
	}{
		retryCounts: make(map[string]int),
	}
	return func(ctx context.Context, r *kvpb.BatchRequest) *kvpb.Error {
		if r.Txn == nil || !strings.HasPrefix(r.Txn.Name, prefix) {
			return nil
		}

		if _, ok := r.GetArg(kvpb.EndTxn); ok {
			txnTracker.Lock()
			defer txnTracker.Unlock()
			if txnTracker.retryCounts[r.Txn.Name] < maxCount {
				txnTracker.retryCounts[r.Txn.Name]++
				return kvpb.NewError(kvpb.NewTransactionRetryError(kvpb.RETRY_REASON_UNKNOWN, "injected error"))
			}
		}
		return nil
	}
}
