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

package rangefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/pkg/errors"
)

// A runnable can be run as an async task.
type runnable interface {
	// Run executes the runnable. Cannot be called multiple times.
	Run(context.Context)
	// Must be called if runnable is not Run.
	Cancel()
}

// initResolvedTSScan scans over all keys using the provided iterator and
// informs the rangefeed Processor of any intents. This allows the Processor to
// backfill its unresolvedIntentQueue with any intents that were written before
// the Processor was started and hooked up to a stream of logical operations.
// The Processor can initialize its resolvedTimestamp once the scan completes
// because it knows it is now tracking all intents in its key range.
//
// Iterator Contract:
//   The provided Iterator must observe all intents in the Processor's keyspan.
//   An important implication of this is that if the iterator is a
//   TimeBoundIterator, its MinTimestamp cannot be above the keyspan's largest
//   known resolved timestamp, if one has ever been recorded. If one has never
//   been recorded, the TimeBoundIterator cannot have any lower bound.
//
type initResolvedTSScan struct {
	p  *Processor
	it engine.SimpleIterator
}

func newInitResolvedTSScan(p *Processor, it engine.SimpleIterator) runnable {
	return &initResolvedTSScan{p: p, it: it}
}

func (s *initResolvedTSScan) Run(ctx context.Context) {
	defer s.Cancel()
	if err := s.iterateAndConsume(ctx); err != nil {
		err = errors.Wrap(err, "initial resolved timestamp scan failed")
		log.Error(ctx, err)
		s.p.StopWithErr(roachpb.NewError(err))
	} else {
		// Inform the processor that its resolved timestamp can be initialized.
		s.p.setResolvedTSInitialized()
	}
}

func (s *initResolvedTSScan) iterateAndConsume(ctx context.Context) error {
	startKey := engine.MakeMVCCMetadataKey(s.p.Span.Key.AsRawKey())
	endKey := engine.MakeMVCCMetadataKey(s.p.Span.EndKey.AsRawKey())

	// Iterate through all keys using NextKey. This will look at the first MVCC
	// version for each key. We're only looking for MVCCMetadata versions, which
	// will always be the first version of a key if it exists, so its fine that
	// we skip over all other versions of keys.
	var meta enginepb.MVCCMetadata
	for s.it.Seek(startKey); ; s.it.NextKey() {
		if ok, err := s.it.Valid(); err != nil {
			return err
		} else if !ok || !s.it.UnsafeKey().Less(endKey) {
			break
		}

		// If the key is not a metadata key, ignore it.
		unsafeKey := s.it.UnsafeKey()
		if unsafeKey.IsValue() {
			continue
		}

		// Found a metadata key. Unmarshal.
		if err := protoutil.Unmarshal(s.it.UnsafeValue(), &meta); err != nil {
			return errors.Wrapf(err, "unmarshaling mvcc meta: %v", unsafeKey)
		}

		// If this is an intent, inform the Processor.
		if meta.Txn != nil {
			var ops [1]enginepb.MVCCLogicalOp
			ops[0].SetValue(&enginepb.MVCCWriteIntentOp{
				TxnID:     meta.Txn.ID,
				TxnKey:    meta.Txn.Key,
				Timestamp: meta.Txn.Timestamp,
			})
			s.p.sendEvent(event{ops: ops[:]}, 0 /* timeout */)
		}
	}
	return nil
}

func (s *initResolvedTSScan) Cancel() {
	s.it.Close()
}

// catchUpScan scans over the provided iterator and publishes committed values
// to the registration's stream. This backfill allows a registration to request
// a starting timestamp in the past and observe events for writes that have
// already happened.
//
// Iterator Contract:
//   Committed values beneath the registration's starting timestamp will be
//   ignored, but all values above the registration's starting timestamp must be
//   present. An important implication of this is that if the iterator is a
//   TimeBoundIterator, its MinTimestamp cannot be above the registration's
//   starting timestamp.
//
type catchUpScan struct {
	p  *Processor
	r  *registration
	it engine.SimpleIterator
	a  bufalloc.ByteAllocator
}

func newCatchUpScan(p *Processor, r *registration) runnable {
	s := catchUpScan{p: p, r: r, it: r.catchUpIter}
	r.catchUpIter = nil // detach
	return &s
}

func (s *catchUpScan) Run(ctx context.Context) {
	defer s.Cancel()
	if err := s.iterateAndSend(ctx); err != nil {
		err = errors.Wrap(err, "catch-up scan failed")
		log.Error(ctx, err)
		s.p.deliverCatchUpScanRes(s.r, roachpb.NewError(err))
	} else {
		s.p.deliverCatchUpScanRes(s.r, nil)
	}
}

func (s *catchUpScan) iterateAndSend(ctx context.Context) error {
	startKey := engine.MakeMVCCMetadataKey(s.r.span.Key)
	endKey := engine.MakeMVCCMetadataKey(s.r.span.EndKey)

	// Iterate though all keys using Next. We want to publish all committed
	// versions of each key that are after the registration's startTS, so we
	// can't use NextKey.
	var meta enginepb.MVCCMetadata
	for s.it.Seek(startKey); ; s.it.Next() {
		if ok, err := s.it.Valid(); err != nil {
			return err
		} else if !ok || !s.it.UnsafeKey().Less(endKey) {
			break
		}

		unsafeKey := s.it.UnsafeKey()
		unsafeVal := s.it.UnsafeValue()
		if !unsafeKey.IsValue() {
			// Found a metadata key.
			if err := protoutil.Unmarshal(unsafeVal, &meta); err != nil {
				return errors.Wrapf(err, "unmarshaling mvcc meta: %v", unsafeKey)
			}
			if !meta.IsInline() {
				// Not an inline value. Ignore.
				continue
			}

			// If write is inline, it doesn't have a timestamp so we don't
			// filter on the registration's starting timestamp. Instead, we
			// return all inline writes.
			unsafeVal = meta.RawBytes
		} else if !s.r.startTS.Less(unsafeKey.Timestamp) {
			// At or before the registration's exclusive starting timestamp.
			// Ignore.
			continue
		}

		var key, val []byte
		s.a, key = s.a.Copy(unsafeKey.Key, 0)
		s.a, val = s.a.Copy(unsafeVal, 0)
		ts := unsafeKey.Timestamp

		var event roachpb.RangeFeedEvent
		event.MustSetValue(&roachpb.RangeFeedValue{
			Key: key,
			Value: roachpb.Value{
				RawBytes:  val,
				Timestamp: ts,
			},
		})
		if err := s.r.stream.Send(&event); err != nil {
			return err
		}
	}
	return nil
}

func (s *catchUpScan) Cancel() {
	s.it.Close()
	s.a = nil
}

// TxnPusher is capable of pushing transactions to a new timestamp and
// cleaning up the intents of transactions that are found to be committed.
type TxnPusher interface {
	// PushTxns attempts to push the specified transactions to a new
	// timestamp. It returns the resulting transaction protos.
	PushTxns(context.Context, []enginepb.TxnMeta, hlc.Timestamp) ([]roachpb.Transaction, error)
	// CleanupTxnIntentsAsync asynchronously cleans up intents owned
	// by the specified transactions.
	CleanupTxnIntentsAsync(context.Context, []roachpb.Transaction) error
}

// txnPushAttempt pushes all old transactions that have unresolved intents on
// the range which are blocking the resolved timestamp from moving forward. It
// does so in two steps.
// 1. it pushes all old transactions to the current timestamp and gathers
//    up the transactions' authoritative transaction records.
// 2. for each transaction that is pushed, it checks the transaction's current
//    status and reacts accordingly:
//    - PENDING:   inform the Processor that the transaction's timestamp has
//                 increased so that the transaction's intents no longer need
//                 to block the resolved timestamp. Even though the intents
//                 may still be at an older timestamp, we know that they can't
//                 commit at that timestamp.
//    - COMMITTED: launch async processes to resolve the transaction's intents
//                 so they will be resolved sometime soon and unblock the
//                 resolved timestamp.
//    - ABORTED:   inform the Processor to stop caring about the transaction.
//                 It will never commit and its intents can be safely ignored.
type txnPushAttempt struct {
	p     *Processor
	txns  []enginepb.TxnMeta
	ts    hlc.Timestamp
	doneC chan struct{}
}

func newTxnPushAttempt(
	p *Processor, txns []enginepb.TxnMeta, ts hlc.Timestamp, doneC chan struct{},
) runnable {
	return &txnPushAttempt{
		p:     p,
		txns:  txns,
		ts:    ts,
		doneC: doneC,
	}
}

func (a *txnPushAttempt) Run(ctx context.Context) {
	defer a.Cancel()
	if err := a.pushOldTxns(ctx); err != nil {
		log.Error(ctx, errors.Wrap(err, "pushing old intents failed"))
	}
}

func (a *txnPushAttempt) pushOldTxns(ctx context.Context) error {
	// Push all transactions using the TxnPusher to the current time.
	// This may cause transaction restarts, but span refreshing should
	// prevent a restart for any transaction that has not been written
	// over at a larger timestamp.
	pushedTxns, err := a.p.TxnPusher.PushTxns(ctx, a.txns, a.ts)
	if err != nil {
		return err
	}

	// Inform the Processor of the results of the push for each transaction.
	ops := make([]enginepb.MVCCLogicalOp, len(pushedTxns))
	var toCleanup []roachpb.Transaction
	for i, txn := range pushedTxns {
		switch txn.Status {
		case roachpb.PENDING:
			// The intent is still pending but its timestamp was moved forward to
			// the current time. Inform the Processor that it can forward the txn's
			// timestamp in its unresolvedIntentQueue.
			ops[i].SetValue(&enginepb.MVCCUpdateIntentOp{
				TxnID:     txn.ID,
				Timestamp: txn.Timestamp,
			})
		case roachpb.COMMITTED:
			// The intent is committed and its timestamp may have moved forward
			// since we last saw an intent. Inform the Processor immediately in case
			// this is the transaction that is holding back the resolved timestamp.
			// However, we still need to wait for the transaction's intents to
			// actually be resolved.
			ops[i].SetValue(&enginepb.MVCCUpdateIntentOp{
				TxnID:     txn.ID,
				Timestamp: txn.Timestamp,
			})

			// Asynchronously clean up the transaction's intents, which should
			// eventually cause all unresolved intents for this transaction on the
			// rangefeed's range to be resolved. We'll have to wait until the
			// intents are resolved before the resolved timestamp can advance past
			// the transaction's commit timestamp, so the best we can do is help
			// speed up the resolution.
			toCleanup = append(toCleanup, txn)
		case roachpb.ABORTED:
			// The intent is aborted, so it doesn't need to be tracked anymore, nor
			// does it need to prevent the resolved timestamp from advancing. Inform
			// the Processor that it can remove the txn from its
			// unresolvedIntentQueue.
			//
			// NOTE: this only interacts correctly with the unresolvedIntentQueue if
			// it has been initialized. If not, it will decrement the txn reference
			// count without necessarily removing it from the queue, which is not
			// what we want. This complication is avoided because we never launch
			// txnPushAttempt tasks before the unresolvedIntentQueue is initialized.
			ops[i].SetValue(&enginepb.MVCCAbortIntentOp{
				TxnID: txn.ID,
			})

			// While we're here, we might as well also clean up the transaction's
			// intents so that no-one else needs to deal with them.
			toCleanup = append(toCleanup, txn)
		}
	}

	// Inform the processor of all logical ops.
	a.p.sendEvent(event{ops: ops}, 0 /* timeout */)

	// Clean up txns, if necessary,
	return a.p.TxnPusher.CleanupTxnIntentsAsync(ctx, toCleanup)
}

func (a *txnPushAttempt) Cancel() {
	close(a.doneC)
}
