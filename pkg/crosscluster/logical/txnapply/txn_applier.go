// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnapply

import (
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnwriter"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/ring"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type transactionState struct {
	ScheduledTransaction
	applied bool
}

type appliedTransaction struct {
	ldrdecoder.Transaction
	applyResult txnwriter.ApplyResult
}

type ScheduledTransaction struct {
	ldrdecoder.Transaction
	Dependencies []hlc.Timestamp
	EventHorizon hlc.Timestamp
}

// Applier coordinates applying transactions in parallel while ensuring that a
// given txn's dependencies have applied first and that the replicated time has
// advanced past the txn's EventHorizon before application. For example: if t5
// depends on t2 and t4 with a horizon of t1, then t5 will not be applied until
// both t2 and t4 have been applied and after the replicated time advances past
// t1. Since t5 doesn't depend on t3, t5 can be applied before t3 completes.
//
// Also note that the applier assumes it is sent transactions in increasing
// timestamp order.
type Applier struct {
	mu struct {
		syncutil.Mutex
		replicatedTime hlc.Timestamp

		// transactions maps a txn's origin timestamp to its state.
		transactions map[hlc.Timestamp]transactionState

		// waiting maps a pending transaction to its dependents. I.e. if t5 depends
		// on t2, then waiting[t2] will contain t5.
		waiting map[hlc.Timestamp][]hlc.Timestamp

		// timestamps buffers timestamps of transactions in the order they were
		// received, to help with replicatedTime tracking.
		timestamps ring.Buffer[hlc.Timestamp]

		// horizonWaiting tracks transactions that have no remaining
		// dependencies but cannot be applied yet because the applier's
		// replicatedTime has not advanced past their EventHorizon.
		horizonWaiting []hlc.Timestamp
	}
	txnWriters []txnwriter.TransactionWriter

	frontier Latest[hlc.Timestamp]
}

func NewApplier(writers []txnwriter.TransactionWriter) *Applier {
	a := &Applier{
		txnWriters: writers,
		frontier:   MakeLatest[hlc.Timestamp](),
	}
	a.mu.transactions = make(map[hlc.Timestamp]transactionState)
	a.mu.waiting = make(map[hlc.Timestamp][]hlc.Timestamp)
	return a
}

func (a *Applier) Close(ctx context.Context) {
	a.frontier.Close()
	for _, writer := range a.txnWriters {
		writer.Close(ctx)
	}
}

func (a *Applier) Frontier() chan hlc.Timestamp {
	return a.frontier.Chan
}

func (a *Applier) Run(ctx context.Context, input chan ScheduledTransaction) error {
	ready := make(chan ldrdecoder.Transaction)
	applied := make(chan appliedTransaction)

	group := ctxgroup.WithContext(ctx)
	group.GoCtx(func(ctx context.Context) error {
		return a.coordinator(ctx, input, ready)
	})

	// TODO make the number of writers a configuration option
	for _, writer := range a.txnWriters {
		group.GoCtx(func(ctx context.Context) error {
			return a.writer(ctx, writer, ready, applied)
		})
	}

	group.GoCtx(func(ctx context.Context) error {
		return a.aggregator(ctx, applied, ready)
	})

	return group.Wait()
}

func (a *Applier) coordinator(
	ctx context.Context, input chan ScheduledTransaction, ready chan ldrdecoder.Transaction,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case transaction := <-input:
			appliable, err := a.recordTransaction(transaction)
			if err != nil {
				return err
			}
			if appliable {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case ready <- transaction.Transaction:
					// done
				}
			}
		}
	}
}

func (a *Applier) recordTransaction(transaction ScheduledTransaction) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var err error
	// Clone the slice to avoid mutating the caller's slice.
	transaction.Dependencies = slices.Clone(transaction.Dependencies)
	transaction.Dependencies = slices.DeleteFunc(transaction.Dependencies, func(txn hlc.Timestamp) bool {
		if txn.LessEq(a.mu.replicatedTime) {
			return true
		}
		dependency, ok := a.mu.transactions[txn]
		if !ok {
			err = errors.AssertionFailedf("missing dependency %+v", txn)
		}
		return dependency.applied
	})
	if err != nil {
		return false, err
	}

	a.mu.transactions[transaction.Timestamp] = transactionState{
		ScheduledTransaction: transaction,
		applied:              false,
	}
	if a.mu.timestamps.Len() != 0 && transaction.Timestamp.LessEq(a.mu.timestamps.GetLast()) {
		return false, errors.AssertionFailedf("transactions must be sent in increasing timestamp order: got %s, last was %s", transaction.Timestamp, a.mu.timestamps.GetLast())
	}
	a.mu.timestamps.AddLast(transaction.Timestamp)

	for _, dependency := range transaction.Dependencies {
		a.mu.waiting[dependency] = append(a.mu.waiting[dependency], transaction.Transaction.Timestamp)
	}

	if len(transaction.Dependencies) == 0 {
		if transaction.EventHorizon.LessEq(a.mu.replicatedTime) {
			return true, nil
		}
		a.mu.horizonWaiting = append(a.mu.horizonWaiting, transaction.Timestamp)
	}
	return false, nil
}

func (a *Applier) writer(
	ctx context.Context,
	txnWriter txnwriter.TransactionWriter,
	ready chan ldrdecoder.Transaction,
	applied chan appliedTransaction,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case transaction := <-ready:
			// TODO(jeffswenson): build up a batch to apply by pulling from the ready
			// channel.
			results, err := txnWriter.ApplyBatch(ctx, []ldrdecoder.Transaction{transaction})
			if err != nil {
				return err
			}
			txn := appliedTransaction{
				Transaction: transaction,
				applyResult: results[0],
			}
			// TODO(jeffswenson): write to the DLQ here if necessary.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case applied <- txn:
			}
		}
	}
}

func (a *Applier) aggregator(
	ctx context.Context, applied chan appliedTransaction, ready chan ldrdecoder.Transaction,
) error {
	// WARNING: there is a deadlock risk in aggregator because we are creating a
	// loop between the channels. We avoid this deadlock by buffering newly ready
	// transactions.
	readyBuffer := ring.MakeBuffer[ldrdecoder.Transaction](nil)
	for {
		if readyBuffer.Len() == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case transaction := <-applied:
				err := a.recordCompletion(ctx, transaction, &readyBuffer)
				if err != nil {
					return err
				}
			}
		} else {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ready <- readyBuffer.GetFirst():
				readyBuffer.RemoveFirst()
			case transaction := <-applied:
				err := a.recordCompletion(ctx, transaction, &readyBuffer)
				if err != nil {
					return err
				}
			}
		}
	}
}

func (a *Applier) recordCompletion(
	ctx context.Context,
	completedTxn appliedTransaction,
	readyBuffer *ring.Buffer[ldrdecoder.Transaction],
) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, waitingTs := range a.mu.waiting[completedTxn.Timestamp] {
		waitingTxn, ok := a.mu.transactions[waitingTs]
		if !ok {
			return errors.AssertionFailedf("missing transaction %+v", waitingTs)
		}

		waitingTxn.Dependencies = slices.DeleteFunc(waitingTxn.Dependencies, func(ts hlc.Timestamp) bool {
			return ts == completedTxn.Timestamp
		})
		a.mu.transactions[waitingTs] = waitingTxn

		if len(waitingTxn.Dependencies) == 0 {
			if waitingTxn.EventHorizon.LessEq(a.mu.replicatedTime) {
				readyBuffer.AddLast(waitingTxn.Transaction)
			} else {
				a.mu.horizonWaiting = append(a.mu.horizonWaiting, waitingTs)
			}
		}
	}

	delete(a.mu.waiting, completedTxn.Timestamp)
	txnState := a.mu.transactions[completedTxn.Timestamp]
	txnState.applied = true
	a.mu.transactions[completedTxn.Timestamp] = txnState

	var newReplicatedTime hlc.Timestamp
	for a.mu.timestamps.Len() != 0 {
		txn := a.mu.timestamps.GetFirst()
		if !a.mu.transactions[txn].applied {
			break
		}
		newReplicatedTime = txn
		delete(a.mu.transactions, txn)
		a.mu.timestamps.RemoveFirst()
	}
	if newReplicatedTime.IsSet() {
		log.Dev.Infof(ctx, "advancing frontier: %+v", newReplicatedTime)
		a.mu.replicatedTime = newReplicatedTime
		a.frontier.Set(newReplicatedTime)

		// Drain transactions whose EventHorizon is now satisfied.
		a.mu.horizonWaiting = slices.DeleteFunc(a.mu.horizonWaiting, func(ts hlc.Timestamp) bool {
			txn := a.mu.transactions[ts]
			if txn.EventHorizon.LessEq(newReplicatedTime) {
				readyBuffer.AddLast(txn.Transaction)
				return true
			}
			return false
		})
	}
	return nil
}
