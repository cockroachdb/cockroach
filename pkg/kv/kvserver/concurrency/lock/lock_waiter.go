// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package lock provides type definitions for locking-related concepts used by
// concurrency control in the key-value layer.
package lock

import "github.com/cockroachdb/redact"

// SafeFormat implements redact.SafeFormatter.
func (lw Waiter) SafeFormat(w redact.SafePrinter, _ rune) {
	expand := w.Flag('+')

	txnIDRedactableString := redact.Sprint(nil)
	if lw.WaitingTxn != nil {
		if expand {
			txnIDRedactableString = redact.Sprint(lw.WaitingTxn.ID)
		} else {
			txnIDRedactableString = redact.Sprint(lw.WaitingTxn.Short())
		}
	}
	w.Printf("waiting_txn:%s active_waiter:%t strength:%s wait_duration:%s", txnIDRedactableString, lw.ActiveWaiter, lw.Strength, lw.WaitDuration)
}
