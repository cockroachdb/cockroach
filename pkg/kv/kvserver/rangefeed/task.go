// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
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
	it storage.SimpleIterator
}

func newInitResolvedTSScan(p *Processor, it storage.SimpleIterator) runnable {
	return &initResolvedTSScan{p: p, it: it}
}

func (s *initResolvedTSScan) Run(ctx context.Context) {
	defer s.Cancel()
	if err := s.iterateAndConsume(ctx); err != nil {
		err = errors.Wrap(err, "initial resolved timestamp scan failed")
		log.Errorf(ctx, "%v", err)
		s.p.StopWithErr(roachpb.NewError(err))
	} else {
		// Inform the processor that its resolved timestamp can be initialized.
		s.p.setResolvedTSInitialized()
	}
}

func (s *initResolvedTSScan) iterateAndConsume(ctx context.Context) error {
	startKey := storage.MakeMVCCMetadataKey(s.p.Span.Key.AsRawKey())
	endKey := storage.MakeMVCCMetadataKey(s.p.Span.EndKey.AsRawKey())

	// Iterate through all keys using NextKey. This will look at the first MVCC
	// version for each key. We're only looking for MVCCMetadata versions, which
	// will always be the first version of a key if it exists, so its fine that
	// we skip over all other versions of keys.
	var meta enginepb.MVCCMetadata
	for s.it.SeekGE(startKey); ; s.it.NextKey() {
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
				TxnID:           meta.Txn.ID,
				TxnKey:          meta.Txn.Key,
				TxnMinTimestamp: meta.Txn.MinTimestamp,
				Timestamp:       meta.Txn.WriteTimestamp,
			})
			s.p.sendEvent(event{ops: ops[:]}, 0 /* timeout */)
		}
	}
	return nil
}

func (s *initResolvedTSScan) Cancel() {
	s.it.Close()
}

// TxnPusher is capable of pushing transactions to a new timestamp and
// cleaning up the intents of transactions that are found to be committed.
type TxnPusher interface {
	// PushTxns attempts to push the specified transactions to a new
	// timestamp. It returns the resulting transaction protos.
	PushTxns(context.Context, []enginepb.TxnMeta, hlc.Timestamp) ([]*roachpb.Transaction, error)
	// CleanupTxnIntentsAsync asynchronously cleans up intents owned
	// by the specified transactions.
	CleanupTxnIntentsAsync(context.Context, []*roachpb.Transaction) error
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
		log.Errorf(ctx, "pushing old intents failed: %v", err)
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
	var toCleanup []*roachpb.Transaction
	for i, txn := range pushedTxns {
		switch txn.Status {
		case roachpb.PENDING, roachpb.STAGING:
			// The transaction is still in progress but its timestamp was moved
			// forward to the current time. Inform the Processor that it can
			// forward the txn's timestamp in its unresolvedIntentQueue.
			ops[i].SetValue(&enginepb.MVCCUpdateIntentOp{
				TxnID:     txn.ID,
				Timestamp: txn.WriteTimestamp,
			})
		case roachpb.COMMITTED:
			// The transaction is committed and its timestamp may have moved
			// forward since we last saw an intent. Inform the Processor
			// immediately in case this is the transaction that is holding back
			// the resolved timestamp. However, we still need to wait for the
			// transaction's intents to actually be resolved.
			ops[i].SetValue(&enginepb.MVCCUpdateIntentOp{
				TxnID:     txn.ID,
				Timestamp: txn.WriteTimestamp,
			})

			// Asynchronously clean up the transaction's intents, which should
			// eventually cause all unresolved intents for this transaction on the
			// rangefeed's range to be resolved. We'll have to wait until the
			// intents are resolved before the resolved timestamp can advance past
			// the transaction's commit timestamp, so the best we can do is help
			// speed up the resolution.
			toCleanup = append(toCleanup, txn)
		case roachpb.ABORTED:
			// The transaction is aborted, so it doesn't need to be tracked
			// anymore nor does it need to prevent the resolved timestamp from
			// advancing. Inform the Processor that it can remove the txn from
			// its unresolvedIntentQueue.
			//
			// NOTE: the unresolvedIntentQueue will ignore MVCCAbortTxn operations
			// before it has been initialized. This is not a concern here though
			// because we never launch txnPushAttempt tasks before the queue has
			// been initialized.
			ops[i].SetValue(&enginepb.MVCCAbortTxnOp{
				TxnID: txn.ID,
			})

			// While we're here, we might as well also clean up the transaction's
			// intents so that no-one else needs to deal with them. However, it's
			// likely that if our push caused the abort then the transaction's
			// intents will be unknown and we won't be doing much good here.
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
