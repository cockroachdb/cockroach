// Copyright 2018 The Cockroach Authors.
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

package kv

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// txnSeqNumAllocator is a txnInterceptor in charge of allocating sequence
// numbers to all the individual requests in batches.
//
// The sequence number is used for replay and reordering protection. At the
// Store, a sequence number less than or equal to the last observed one (on a
// given key) incurs a transaction restart (if the request is transactional).
// This semantic could be adjusted in the future to provide idempotency for
// replays and re-issues. However, a side effect of providing this property is
// that reorder protection would no longer be provided by the counter, so
// ordering guarantees between requests within the same transaction would need
// to be strengthened elsewhere (e.g. by the transport layer).
type txnSeqNumAllocator struct {
	wrapped lockedSender

	seqNumCounter int32

	// commandCount indicates how many requests have been sent through
	// this transaction. Reset on retryable txn errors.
	// TODO(andrei): let's get rid of this. It should be maintained
	// in the SQL level.
	commandCount int32
}

// SendLocked is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) SendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	for _, ru := range ba.Requests {
		s.seqNumCounter++

		oldHeader := ru.GetInner().Header()
		oldHeader.Sequence = s.seqNumCounter
		ru.GetInner().SetHeader(oldHeader)
	}
	// For 2.0 compatibility.
	ba.Txn.Sequence = s.seqNumCounter

	s.commandCount += int32(len(ba.Requests))

	return s.wrapped.SendLocked(ctx, ba)
}

// setWrapped is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) setWrapped(wrapped lockedSender) { s.wrapped = wrapped }

// populateMetaLocked is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) populateMetaLocked(meta *roachpb.TxnCoordMeta) {
	meta.CommandCount = s.commandCount
}

// augmentMetaLocked is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) augmentMetaLocked(meta roachpb.TxnCoordMeta) {
	s.commandCount += meta.CommandCount
}

// epochBumpedLocked is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) epochBumpedLocked() {
	s.seqNumCounter = 0
	s.commandCount = 0
}

// closeLocked is part of the txnInterceptor interface.
func (*txnSeqNumAllocator) closeLocked() {}
