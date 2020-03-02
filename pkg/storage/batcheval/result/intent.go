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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/concurrency/lock"
)

// FromEncounteredIntents creates a Result communicating that the intents were encountered
// by the given request and should be handled.
func FromEncounteredIntents(intents []roachpb.Intent) Result {
	var pd Result
	if len(intents) == 0 {
		return pd
	}
	pd.Local.EncounteredIntents = intents
	return pd
}

// FromWrittenIntents creates a Result communicating that the intents were
// written or re-written by the given request and should be handled.
func FromWrittenIntents(txn *roachpb.Transaction, keys ...roachpb.Key) Result {
	var pd Result
	if txn == nil {
		return pd
	}
	pd.Local.WrittenIntents = make([]roachpb.LockUpdate, len(keys))
	for i := range pd.Local.WrittenIntents {
		pd.Local.WrittenIntents[i] = roachpb.LockUpdate{
			Span:       roachpb.Span{Key: keys[i]},
			Txn:        txn.TxnMeta,
			Status:     roachpb.PENDING,
			Durability: lock.Replicated,
		}
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
// completed and its intents should be resolved.
func FromEndTxn(txn *roachpb.Transaction, alwaysReturn, poison bool) Result {
	var pd Result
	if len(txn.IntentSpans) == 0 {
		return pd
	}
	pd.Local.EndTxns = []EndTxnIntents{{Txn: txn, Always: alwaysReturn, Poison: poison}}
	return pd
}
