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
	Dependencies []ldrdecoder.TxnID
	EventHorizon hlc.Timestamp
}

// ApplierEvent is an event that can be sent to the Applier's input channel.
type ApplierEvent interface {
	applierEvent()
}

func (ScheduledTransaction) applierEvent() {}
func (Checkpoint) applierEvent()           {}

// Checkpoint indicates that all transactions with timestamp ≤ Timestamp have
// been sent. This allows idle appliers to advance their frontier.
type Checkpoint struct {
	Timestamp hlc.Timestamp
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
	id          ldrdecoder.ApplierID
	depResolver DependencyResolver

	mu struct {
		syncutil.Mutex

		// transactions maps a txn's TxnID to its state.
		transactions map[ldrdecoder.TxnID]transactionState

		// localWaiting maps a pending transaction to its dependents. I.e. if t5
		// depends on t2, then localWaiting[t2] will contain t5.
		localWaiting map[ldrdecoder.TxnID][]ldrdecoder.TxnID

		// remoteWaiting maps a remote dependency TxnID to the local txns
		// waiting on it.
		remoteWaiting map[ldrdecoder.TxnID][]ldrdecoder.TxnID

		// applierFrontiers tracks the latest known frontier per remote
		// applier, updated via DependencyResolver notifications.
		applierFrontiers map[ldrdecoder.ApplierID]hlc.Timestamp

		// txnIDs buffers TxnIDs of transactions in the order they were
		// received, to help with replicatedTime tracking.
		txnIDs ring.Buffer[ldrdecoder.TxnID]

		// horizonWaiting tracks transactions that have no remaining
		// dependencies but cannot be applied yet because the applier's
		// replicatedTime has not advanced past their EventHorizon.
		//
		// TODO(msbutler): consider making this a heap.
		horizonWaiting []ldrdecoder.TxnID

		// checkpoint is the latest checkpoint timestamp received. Used by
		// drainSatisfiedHorizonWaitersLocked for the relaxed horizon check.
		checkpoint hlc.Timestamp
	}
	txnWriters []txnwriter.TransactionWriter

	frontier Latest[hlc.Timestamp]
}

// NewApplier creates a new Applier with the given ID and writers. allApplierIDs
// must include all applier IDs in the system (including this applier's own ID)
// so that the applier can initialize the frontier map used to track when all
// appliers have advanced past an EventHorizon.
func NewApplier(
	id ldrdecoder.ApplierID,
	writers []txnwriter.TransactionWriter,
	depResolver DependencyResolver,
	allApplierIDs []ldrdecoder.ApplierID,
) (*Applier, error) {
	if id == 0 {
		return nil, errors.New("applier ID must be nonzero")
	}
	if depResolver == nil {
		return nil, errors.New("dependency resolver must not be nil")
	}
	a := &Applier{
		id:          id,
		depResolver: depResolver,
		txnWriters:  writers,
		frontier:    MakeLatest[hlc.Timestamp](),
	}
	a.mu.transactions = make(map[ldrdecoder.TxnID]transactionState)
	a.mu.localWaiting = make(map[ldrdecoder.TxnID][]ldrdecoder.TxnID)
	a.mu.remoteWaiting = make(map[ldrdecoder.TxnID][]ldrdecoder.TxnID)
	a.mu.applierFrontiers = make(map[ldrdecoder.ApplierID]hlc.Timestamp, len(allApplierIDs))
	for _, applierID := range allApplierIDs {
		a.mu.applierFrontiers[applierID] = hlc.Timestamp{}
	}
	return a, nil
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

func (a *Applier) Run(ctx context.Context, input chan ApplierEvent) error {
	ready := make(chan ldrdecoder.Transaction)
	applied := make(chan appliedTransaction)

	group := ctxgroup.WithContext(ctx)
	group.GoCtx(func(ctx context.Context) error {
		return a.coordinator(ctx, input, ready, applied)
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
	ctx context.Context,
	input chan ApplierEvent,
	ready chan ldrdecoder.Transaction,
	applied chan appliedTransaction,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-input:
			switch e := event.(type) {
			case ScheduledTransaction:
				appliable, err := a.recordTransaction(e)
				if err != nil {
					return err
				}
				if appliable {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case ready <- e.Transaction:
						// done
					}
				}
			case Checkpoint:
				synthTxn, ok := a.processCheckpoint(e.Timestamp)
				if ok {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case applied <- synthTxn:
					}
				}
			}
		}
	}
}

func (a *Applier) recordTransaction(transaction ScheduledTransaction) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if transaction.TxnID.ApplierID != a.id {
		return false, errors.AssertionFailedf(
			"transaction %+v has ApplierID %d, expected %d",
			transaction.TxnID, transaction.TxnID.ApplierID, a.id)
	}

	var err error
	// Clone the slice to avoid mutating the caller's slice.
	transaction.Dependencies = slices.Clone(transaction.Dependencies)
	transaction.Dependencies = slices.DeleteFunc(
		transaction.Dependencies,
		func(txnID ldrdecoder.TxnID) bool {
			if txnID.ApplierID == a.id {
				// Local dep: prune if already applied.
				if txnID.Timestamp.LessEq(a.mu.applierFrontiers[a.id]) {
					return true
				}
				dependency, ok := a.mu.transactions[txnID]
				if !ok {
					err = errors.AssertionFailedf("missing dependency %+v", txnID)
				}
				return dependency.applied
			}
			// Remote dep: prune if the applier's frontier has advanced
			// past its timestamp.
			frontier := a.mu.applierFrontiers[txnID.ApplierID]
			return txnID.Timestamp.LessEq(frontier)
		})
	if err != nil {
		return false, err
	}

	a.mu.transactions[transaction.TxnID] = transactionState{
		ScheduledTransaction: transaction,
		applied:              false,
	}
	if a.mu.txnIDs.Len() != 0 && transaction.TxnID.LessEq(a.mu.txnIDs.GetLast()) {
		return false, errors.AssertionFailedf(
			"transactions must be sent in increasing timestamp order: got %s, last was %s",
			transaction.TxnID.Timestamp, a.mu.txnIDs.GetLast().Timestamp)
	}
	a.mu.txnIDs.AddLast(transaction.TxnID)

	var newRemoteDeps []ldrdecoder.TxnID
	for _, dep := range transaction.Dependencies {
		if dep.ApplierID == a.id {
			a.mu.localWaiting[dep] = append(a.mu.localWaiting[dep], transaction.TxnID)
		} else {
			a.mu.remoteWaiting[dep] = append(a.mu.remoteWaiting[dep], transaction.TxnID)
			if len(a.mu.remoteWaiting[dep]) == 1 {
				newRemoteDeps = append(newRemoteDeps, dep)
			}
		}
	}

	// Register remote deps with the dependency resolver.
	if len(newRemoteDeps) > 0 {
		a.depResolver.Wait(a.id, newRemoteDeps)
	}

	if len(transaction.Dependencies) == 0 {
		if transaction.EventHorizon.LessEq(a.getGlobalFrontierLocked()) {
			return true, nil
		}
		a.mu.horizonWaiting = append(a.mu.horizonWaiting, transaction.TxnID)
		a.registerHorizonWaitLocked(transaction.EventHorizon)
	}
	return false, nil
}

// processCheckpoint handles a checkpoint event. It stores the checkpoint
// timestamp for the relaxed horizon check and, if the checkpoint is beyond the
// last recorded txn, synthesizes an already-applied transaction to advance the
// frontier.
func (a *Applier) processCheckpoint(checkpoint hlc.Timestamp) (appliedTransaction, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Always store the checkpoint for the relaxed horizon check.
	if a.mu.checkpoint.Less(checkpoint) {
		a.mu.checkpoint = checkpoint
	}

	// Drop if checkpoint ts ≤ last txnID ts (no synthetic txn needed;
	// the real txn's completion will advance the frontier).
	if a.mu.txnIDs.Len() != 0 &&
		!a.mu.txnIDs.GetLast().Timestamp.Less(checkpoint) {
		return appliedTransaction{}, false
	}

	// Synthesize an already-applied txn at the checkpoint timestamp.
	synthID := ldrdecoder.TxnID{ApplierID: a.id, Timestamp: checkpoint}
	a.mu.transactions[synthID] = transactionState{
		ScheduledTransaction: ScheduledTransaction{
			Transaction: ldrdecoder.Transaction{TxnID: synthID},
		},
		applied: true,
	}
	a.mu.txnIDs.AddLast(synthID)

	return appliedTransaction{
		Transaction: ldrdecoder.Transaction{TxnID: synthID},
	}, true
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
			if txn.applyResult.DlqReason != nil {
				// TODO(msbutler): actually write to the DLQ.
				log.Dev.Errorf(ctx, "transaction %s should be sent to DLQ with reason: %v", transaction.TxnID.Timestamp, txn.applyResult.DlqReason)
			}
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
	remoteUpdates := a.depResolver.Receive(a.id)
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
			case update := <-remoteUpdates:
				if err := a.processRemoteUpdate(update, &readyBuffer); err != nil {
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
			case update := <-remoteUpdates:
				if err := a.processRemoteUpdate(update, &readyBuffer); err != nil {
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

	completedID := completedTxn.TxnID
	if err := a.resolveDependencyLocked(
		completedID, a.mu.localWaiting[completedID], readyBuffer,
	); err != nil {
		return err
	}
	delete(a.mu.localWaiting, completedID)
	txnState, ok := a.mu.transactions[completedID]
	if !ok {
		// This txn was already drained by a previous recordCompletion's
		// drain loop. This happens with synthetic checkpoint txns:
		// processCheckpoint marks them applied=true at creation, so a
		// later drain loop can remove them before their own completion
		// message is processed by the aggregator.
		return nil
	}
	txnState.applied = true
	a.mu.transactions[completedID] = txnState

	// After resolving dependncies, advance the local frontier if possible.
	var newLocalReplicatedTime hlc.Timestamp
	for a.mu.txnIDs.Len() != 0 {
		id := a.mu.txnIDs.GetFirst()
		if !a.mu.transactions[id].applied {
			break
		}
		newLocalReplicatedTime = id.Timestamp
		delete(a.mu.transactions, id)
		a.mu.txnIDs.RemoveFirst()
	}
	if newLocalReplicatedTime.IsSet() {
		a.mu.applierFrontiers[a.id] = newLocalReplicatedTime
		a.frontier.Set(newLocalReplicatedTime)
	}

	a.drainSatisfiedHorizonWaitersLocked(readyBuffer)
	a.depResolver.Ready(completedTxn.TxnID, a.mu.applierFrontiers[a.id])
	return nil
}

// resolveDependencyLocked processes the completion of completedID by removing
// it from the dependency lists of all waitingIDs. Transactions that become
// dependency-free are either added to readyBuffer (if their EventHorizon is
// satisfied) or moved to horizonWaiting.
//
// REQUIRES: a.mu is held.
func (a *Applier) resolveDependencyLocked(
	completedID ldrdecoder.TxnID,
	waitingIDs []ldrdecoder.TxnID,
	readyBuffer *ring.Buffer[ldrdecoder.Transaction],
) error {
	for _, waitingID := range waitingIDs {
		waitingTxn, ok := a.mu.transactions[waitingID]
		if !ok {
			return errors.AssertionFailedf("missing transaction %+v", waitingID)
		}

		waitingTxn.Dependencies = slices.DeleteFunc(
			waitingTxn.Dependencies,
			func(id ldrdecoder.TxnID) bool {
				return id == completedID
			})
		a.mu.transactions[waitingID] = waitingTxn

		if len(waitingTxn.Dependencies) == 0 {
			if waitingTxn.EventHorizon.LessEq(a.getGlobalFrontierLocked()) {
				readyBuffer.AddLast(waitingTxn.Transaction)
			} else {
				a.mu.horizonWaiting = append(
					a.mu.horizonWaiting, waitingID)
				a.registerHorizonWaitLocked(waitingTxn.EventHorizon)
			}
		}
	}
	return nil
}

// getGlobalFrontierLocked returns the minimum frontier across all applier
// frontiers.
//
// REQUIRES: a.mu is held.
func (a *Applier) getGlobalFrontierLocked() hlc.Timestamp {
	var minFrontier hlc.Timestamp
	first := true
	for _, frontier := range a.mu.applierFrontiers {
		if first || frontier.Less(minFrontier) {
			minFrontier = frontier
			first = false
		}
	}
	return minFrontier
}

// registerHorizonWaitLocked identifies which remote appliers' frontiers are
// blocking the given EventHorizon and registers horizon waits with the
// dependency resolver.
//
// REQUIRES: a.mu is held.
func (a *Applier) registerHorizonWaitLocked(horizon hlc.Timestamp) {
	for applierID, remoteFrontier := range a.mu.applierFrontiers {
		if applierID == a.id {
			continue
		}
		if horizon.After(remoteFrontier) {
			a.depResolver.WaitHorizon(a.id, applierID, horizon)
		}
	}
}

// drainSatisfiedHorizonWaitersLocked checks all horizonWaiting txns and moves
// any whose EventHorizon is now satisfied into readyBuffer.
//
// Two checks are performed:
//  1. Standard: release any waiter whose EventHorizon ≤ globalFrontier.
//  2. Relaxed: if nothing was released above, release the oldest waiter if its
//     EventHorizon ≤ remoteFrontier AND EventHorizon ≤ checkpoint. This handles
//     the case where an idle applier's local frontier hasn't advanced yet but
//     the checkpoint guarantees no more txns will arrive below that timestamp.
//
// REQUIRES: a.mu is held.
func (a *Applier) drainSatisfiedHorizonWaitersLocked(
	readyBuffer *ring.Buffer[ldrdecoder.Transaction],
) {
	globalFrontier := a.getGlobalFrontierLocked()

	// First pass: standard check against global frontier.
	startLen := readyBuffer.Len()
	a.mu.horizonWaiting = slices.DeleteFunc(a.mu.horizonWaiting,
		func(id ldrdecoder.TxnID) bool {
			txn := a.mu.transactions[id]
			if txn.EventHorizon.LessEq(globalFrontier) {
				readyBuffer.AddLast(txn.Transaction)
				return true
			}
			return false
		})
	if readyBuffer.Len() != startLen {
		return
	}

	// Second pass: relaxed check for the smallest-timestamp horizon waiter.
	// Use remote frontier (min of all OTHER appliers) + checkpoint as a
	// proxy for the local applier's guaranteed progress. We must pick the
	// smallest-timestamp entry for correctness: a txn with eventHorizon=H
	// requires all txns at ts ≤ H to be applied, and other horizonWaiting
	// entries are unapplied. Picking the smallest timestamp guarantees its
	// eventHorizon falls below all other horizonWaiting txns' timestamps
	// (since eventHorizon < timestamp for any txn).
	if !a.mu.checkpoint.IsSet() || len(a.mu.horizonWaiting) == 0 {
		return
	}

	oldestIdx := -1
	for i, id := range a.mu.horizonWaiting {
		if oldestIdx == -1 ||
			id.Timestamp.Less(a.mu.horizonWaiting[oldestIdx].Timestamp) {
			oldestIdx = i
		}
	}
	// Additionally verify all local txns at ts ≤ horizon are applied. The
	// drain loop has already run, so txnIDs.GetFirst() is the earliest
	// unapplied local txn. If its timestamp ≤ horizon, there are in-flight
	// local txns that must complete before the horizon is truly satisfied.
	oldest := a.mu.transactions[a.mu.horizonWaiting[oldestIdx]]
	if a.mu.txnIDs.Len() > 0 &&
		a.mu.txnIDs.GetFirst().Timestamp.LessEq(oldest.EventHorizon) {
		return
	}

	var remoteFrontier hlc.Timestamp
	first := true
	for id, frontier := range a.mu.applierFrontiers {
		if id == a.id {
			continue
		}
		if first || frontier.Less(remoteFrontier) {
			remoteFrontier = frontier
			first = false
		}
	}

	if oldest.EventHorizon.LessEq(remoteFrontier) &&
		oldest.EventHorizon.LessEq(a.mu.checkpoint) {
		readyBuffer.AddLast(oldest.Transaction)
		a.mu.horizonWaiting = slices.Delete(
			a.mu.horizonWaiting, oldestIdx, oldestIdx+1)
	}
}

func (a *Applier) processRemoteUpdate(
	update DependencyUpdate, readyBuffer *ring.Buffer[ldrdecoder.Transaction],
) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.mu.applierFrontiers[update.TxnID.ApplierID] = update.Frontier

	if err := a.resolveDependencyLocked(
		update.TxnID, a.mu.remoteWaiting[update.TxnID], readyBuffer,
	); err != nil {
		return err
	}
	delete(a.mu.remoteWaiting, update.TxnID)
	a.drainSatisfiedHorizonWaitersLocked(readyBuffer)
	return nil
}
