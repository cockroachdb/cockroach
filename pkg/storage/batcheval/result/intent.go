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

import "github.com/cockroachdb/cockroach/pkg/roachpb"

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
