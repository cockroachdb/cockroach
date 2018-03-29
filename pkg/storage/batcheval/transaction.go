// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package batcheval

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/pkg/errors"
)

// ErrTransactionUnsupported is returned when a non-transactional command is
// evaluated in the context of a transaction.
var ErrTransactionUnsupported = errors.New("not supported within a transaction")

// VerifyTransaction runs sanity checks verifying that the transaction in the
// header and the request are compatible.
func VerifyTransaction(h roachpb.Header, args roachpb.Request) error {
	if h.Txn == nil {
		return errors.Errorf("no transaction specified to %s", args.Method())
	}
	if !bytes.Equal(args.Header().Key, h.Txn.Key) {
		return errors.Errorf("request key %s should match txn key %s", args.Header().Key, h.Txn.Key)
	}
	return nil
}

// WriteAbortSpanOnResolve returns true if the abort span must be written when
// the transaction with the given status is resolved.
func WriteAbortSpanOnResolve(status roachpb.TransactionStatus) bool {
	return status == roachpb.ABORTED
}

// SetAbortSpan clears any AbortSpan entry if poison is false.
// Otherwise, if poison is true, creates an entry for this transaction
// in the AbortSpan to prevent future reads or writes from
// spuriously succeeding on this range.
func SetAbortSpan(
	ctx context.Context,
	rec EvalContext,
	batch engine.ReadWriter,
	ms *enginepb.MVCCStats,
	txn enginepb.TxnMeta,
	poison bool,
) error {
	// Read the current state of the AbortSpan so we can detect when
	// no changes are needed. This can help us avoid unnecessary Raft
	// proposals.
	var curEntry roachpb.AbortSpanEntry
	exists, err := rec.AbortSpan().Get(ctx, batch, txn.ID, &curEntry)
	if err != nil {
		return err
	}

	if !poison {
		if !exists {
			return nil
		}
		return rec.AbortSpan().Del(ctx, batch, ms, txn.ID)
	}

	entry := roachpb.AbortSpanEntry{
		Key:       txn.Key,
		Timestamp: txn.Timestamp,
		Priority:  txn.Priority,
	}
	if exists && curEntry.Equal(entry) {
		return nil
	}
	return rec.AbortSpan().Put(ctx, batch, ms, txn.ID, &entry)
}

// CanPushWithPriority returns true if the given pusher can push the pushee
// based on its priority.
func CanPushWithPriority(pusher, pushee *roachpb.Transaction) bool {
	return (pusher.Priority > roachpb.MinTxnPriority && pushee.Priority == roachpb.MinTxnPriority) ||
		(pusher.Priority == roachpb.MaxTxnPriority && pushee.Priority < pusher.Priority)
}
