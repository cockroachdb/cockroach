// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package result

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// FromAcquiredLocks creates a Result communicating that the locks were
// acquired or re-acquired by the given transaction and should be handled.
func FromAcquiredLocks(txn *roachpb.Transaction, keys ...roachpb.Key) Result {
	var pd Result
	if txn == nil {
		return pd
	}
	pd.Local.AcquiredLocks = make([]roachpb.LockAcquisition, len(keys))
	for i := range pd.Local.AcquiredLocks {
		pd.Local.AcquiredLocks[i] = roachpb.MakeLockAcquisition(txn, keys[i], lock.Replicated)
	}
	return pd
}

// EndTxnIntents contains a finished transaction and a bool (Always),
// which indicates whether the intents should be resolved whether or
// not the command succeeds through Raft.
type EndTxnIntents struct {
	Txn    *roachpb.Transaction
	Always bool
	Poison bool
}

// FromEndTxn creates a Result communicating that a transaction was
// completed and its locks should be resolved.
func FromEndTxn(txn *roachpb.Transaction, alwaysReturn, poison bool) Result {
	var pd Result
	if len(txn.LockSpans) == 0 {
		return pd
	}
	pd.Local.EndTxns = []EndTxnIntents{{Txn: txn, Always: alwaysReturn, Poison: poison}}
	return pd
}
