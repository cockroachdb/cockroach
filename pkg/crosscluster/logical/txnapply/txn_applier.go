// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnapply

import (
	"context"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnwriter"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/container/heap"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/ring"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/gogo/protobuf/proto"
)

func init() {
	pKey := errors.GetTypeKey((*ReplicationError)(nil))
	errors.RegisterWrapperEncoder(pKey, encodeReplicationError)
	errors.RegisterWrapperDecoder(pKey, decodeReplicationError)
}

var leaseReleaseInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.lease_release_interval",
	"how often applier goroutines release descriptor leases held by tombstone updaters",
	5*time.Second,
	settings.PositiveDuration,
)

// ReplicationError is returned by a writer goroutine when a source transaction
// cannot be applied to the destination (e.g. a constraint violation).
type ReplicationError struct {
	// The underlying error that prevented the transaction from being applied.
	Err error
	// The MVCC timestamp of the source transaction that could not be applied.
	Timestamp hlc.Timestamp
}

// Error implements the error interface.
func (e *ReplicationError) Error() string {
	return redact.StringWithoutMarkers(e)
}

// SafeFormat implements redact.SafeFormatter.
func (e *ReplicationError) SafeFormat(p redact.SafePrinter, _ rune) {
	p.Printf("replication error at %s: %v", e.Timestamp, e.Err)
}

// Unwrap implements the errors.Wrapper interface.
func (e *ReplicationError) Unwrap() error { return e.Err }

func encodeReplicationError(
	_ context.Context, err error,
) (msgPrefix string, safe []string, details proto.Message) {
	var e *ReplicationError
	errors.As(err, &e)
	return "", nil, &e.Timestamp
}

func decodeReplicationError(
	_ context.Context, cause error, _ string, _ []string, payload proto.Message,
) error {
	ts, ok := payload.(*hlc.Timestamp)
	if !ok {
		// Payload type changed unexpectedly; fall back to opaque decode.
		return nil
	}
	return &ReplicationError{Timestamp: *ts, Err: cause}
}

type appliedTransaction struct {
	ldrdecoder.Transaction
	applyResult txnwriter.ApplyResult
}

type ScheduledTransaction struct {
	ldrdecoder.Transaction
	Dependencies []ldrdecoder.TxnID
	EventHorizon hlc.Timestamp

	// remainingDeps tracks how many dependencies are still pending before this
	// txn can be applied. This count is hydrated and updated within the applier,
	// while the Dependencies slice remains static.
	remainingDeps int
}

// ApplierEvent is an event that can be sent to the Applier's input channel.
type ApplierEvent interface {
	applierEvent()
}

func (ScheduledTransaction) applierEvent() {}
func (Checkpoint) applierEvent()           {}

// Checkpoint indicates that all transactions with timestamp ≤ Timestamp have
// been sent (but not applied). This allows idle appliers to advance their
// frontier.
//
// The coordinator sends a Checkpoint to every applier in two cases:
//  1. Periodically, as an intermediate heartbeat so that idle appliers (those
//     with no pending transactions) can advance their frontier.
//  2. Deterministically, when an incoming transaction's EventHorizon is greater
//     than the previous checkpoint. This ensures that every applier has seen
//     the checkpoint before any applier attempts to apply the transaction that
//     depends on it.
//
// The Timestamp is set to the incoming replication event's (upstream checkpoint
// or txn) timestamp minus one, so that the checkpoint strictly precedes the
// event it was derived from.
type Checkpoint struct{ Timestamp hlc.Timestamp }

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
	settings    *cluster.Settings
	depResolver DependencyResolverClient

	mu struct {
		syncutil.Mutex

		// committed tracks which local transactions have been applied.
		committed committedSet

		// transactions maps unapplied txn TxnIDs to their state.
		transactions map[ldrdecoder.TxnID]ScheduledTransaction

		// localWaiting maps a pending transaction to its dependents. I.e. if t5
		// depends on t2, then localWaiting[t2] will contain t5.
		localWaiting map[ldrdecoder.TxnID][]ldrdecoder.TxnID

		// remoteWaiting maps a remote TxnID to the local txns
		// waiting on it.
		remoteWaiting map[ldrdecoder.TxnID][]ldrdecoder.TxnID

		// remoteApplierResolvedTimes tracks the latest known resolved time per
		// remote applier, updated via DependencyResolver notifications.
		remoteApplierResolvedTimes map[ldrdecoder.ApplierID]hlc.Timestamp

		// txnIDs buffers TxnIDs of transactions in the order they were
		// received, to help with replicatedTime tracking.
		txnIDs ring.Buffer[ldrdecoder.TxnID]

		// horizonWaiting is a min-heap of transactions that have no
		// remaining dependencies but cannot be applied yet because the
		// global resolved time has not advanced past their EventHorizon.
		horizonWaiting horizonHeap
	}
	txnWriters []txnwriter.TransactionWriter

	// newCPUHandle creates a per-goroutine SQLCPUHandle for CPU accounting.
	// Each writer goroutine calls this to get its own handle, avoiding
	// cross-writer mutex contention on a shared handle.
	newCPUHandle func() *admission.SQLCPUHandle

	// TODO(msbutler): consider removing and simply call commited.ResolvedTime().
	localResolvedTime Latest[hlc.Timestamp]
}

// NewApplier creates a new Applier with the given ID and writers. NewApplier
// takes ownership of the writers: on success they are closed by Applier.Close;
// on error they are closed before returning. allApplierIDs must include all
// applier IDs in the system (including this applier's own ID) so that the
// applier can initialize the frontier map used to track when all appliers have
// advanced past an EventHorizon.
func NewApplier(
	ctx context.Context,
	id ldrdecoder.ApplierID,
	settings *cluster.Settings,
	writers []txnwriter.TransactionWriter,
	depResolver DependencyResolverClient,
	allApplierIDs []ldrdecoder.ApplierID,
	newCPUHandle func() *admission.SQLCPUHandle,
) (_ *Applier, retErr error) {
	defer func() {
		if retErr != nil {
			for _, w := range writers {
				w.Close(ctx)
			}
		}
	}()
	if id == 0 {
		return nil, errors.AssertionFailedf("applier ID must be nonzero")
	}
	if depResolver == nil {
		return nil, errors.AssertionFailedf("dependency resolver must not be nil")
	}
	if newCPUHandle == nil {
		return nil, errors.AssertionFailedf("newCPUHandle must not be nil")
	}
	a := &Applier{
		id:                id,
		settings:          settings,
		depResolver:       depResolver,
		txnWriters:        writers,
		newCPUHandle:      newCPUHandle,
		localResolvedTime: MakeLatest[hlc.Timestamp](),
	}
	a.mu.committed = makeCommittedSet()
	a.mu.transactions = make(map[ldrdecoder.TxnID]ScheduledTransaction)
	a.mu.localWaiting = make(map[ldrdecoder.TxnID][]ldrdecoder.TxnID)
	a.mu.remoteWaiting = make(map[ldrdecoder.TxnID][]ldrdecoder.TxnID)
	a.mu.remoteApplierResolvedTimes = make(map[ldrdecoder.ApplierID]hlc.Timestamp, len(allApplierIDs))
	for _, applierID := range allApplierIDs {
		if applierID == id {
			continue
		}
		a.mu.remoteApplierResolvedTimes[applierID] = hlc.Timestamp{}
	}
	return a, nil
}

func (a *Applier) Close(ctx context.Context) {
	a.localResolvedTime.Close()
	for _, writer := range a.txnWriters {
		writer.Close(ctx)
	}
}

func (a *Applier) Frontier() chan hlc.Timestamp {
	return a.localResolvedTime.Chan
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
	//
	// TODO(msbutler): once thses are sent over RPCs from the job coordinator, I
	// don't think we need to clone here.
	transaction.Dependencies = slices.Clone(transaction.Dependencies)
	transaction.Dependencies = slices.DeleteFunc(
		transaction.Dependencies,
		func(txnID ldrdecoder.TxnID) bool {
			if txnID.ApplierID == a.id {
				// Local dep: prune if already committed.
				if a.mu.committed.IsResolved(txnID) {
					return true
				}
				if _, ok := a.mu.transactions[txnID]; !ok {
					err = errors.AssertionFailedf("missing dependency %+v", txnID)
				}
				return false
			}
			// Remote dep: prune if remote applier's resolvedTime has advanced
			// past its timestamp.
			resolvedTime := a.mu.remoteApplierResolvedTimes[txnID.ApplierID]
			return txnID.Timestamp.LessEq(resolvedTime)
		})
	if err != nil {
		return false, err
	}

	transaction.remainingDeps = len(transaction.Dependencies)
	a.mu.transactions[transaction.TxnID] = transaction
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

	if transaction.remainingDeps == 0 {
		if transaction.EventHorizon.LessEq(a.getGlobalFrontierLocked()) {
			return true, nil
		}
		heap.Push(&a.mu.horizonWaiting, horizonWaiter{
			txnID:   transaction.TxnID,
			horizon: transaction.EventHorizon,
		})
		a.registerHorizonWaitLocked(transaction.EventHorizon)
	}
	return false, nil
}

// processCheckpoint handles a checkpoint event. If the checkpoint is beyond the
// last recorded txn, it synthesizes an already-applied transaction to advance
// the frontier.
func (a *Applier) processCheckpoint(checkpoint hlc.Timestamp) (appliedTransaction, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Drop if checkpoint ts ≤ last txnID ts (no synthetic txn needed;
	// the real txn's completion will advance the frontier).
	if a.mu.txnIDs.Len() != 0 &&
		checkpoint.LessEq(a.mu.txnIDs.GetLast().Timestamp) {
		return appliedTransaction{}, false
	}

	// Also drop if checkpoint ts ≤ current resolved time (the aggregator
	// may have already drained txnIDs past this point).
	if checkpoint.LessEq(a.mu.committed.ResolvedTime()) {
		return appliedTransaction{}, false
	}

	// Synthesize an already-applied txn at the checkpoint timestamp.
	// Only added to txnIDs (not transactions) since it is already resolved.
	synthID := ldrdecoder.TxnID{ApplierID: a.id, Timestamp: checkpoint}
	a.mu.committed.Resolve(synthID)
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
	handle := a.newCPUHandle()
	defer handle.Close()
	ctx = admission.ContextWithSQLCPUHandle(ctx, handle)
	defer handle.RegisterGoroutine().Close(ctx)
	ticker := time.NewTicker(leaseReleaseInterval.Get(&a.settings.SV))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			ticker.Reset(leaseReleaseInterval.Get(&a.settings.SV))
			txnWriter.ReleaseLeases(ctx)
		case transaction := <-ready:
			// TODO(jeffswenson): build up a batch to apply by pulling from the
			// ready channel.
			results, err := txnWriter.ApplyBatch(
				ctx, []ldrdecoder.Transaction{transaction})
			if err != nil {
				return err
			}
			txn := appliedTransaction{
				Transaction: transaction,
				applyResult: results[0],
			}
			if txn.applyResult.DlqReason != nil {
				return &ReplicationError{
					Err:       txn.applyResult.DlqReason,
					Timestamp: transaction.TxnID.Timestamp,
				}
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
	receiveBuffer := a.depResolver.Receive(a.id)
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
			case <-receiveBuffer.Ch():
				update, ok := receiveBuffer.Pop()
				if !ok {
					// NB: !ok implies the buffer is empty. Further, the receivBuffer.Ch
					// is never closed, so there's no reason tear down the flow based this
					// channel closing.
					break
				}
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
			case <-receiveBuffer.Ch():
				update, ok := receiveBuffer.Pop()
				if !ok {
					break
				}
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

	a.mu.committed.Resolve(completedID)
	delete(a.mu.transactions, completedID)

	// Advance the resolved time by draining applied txns from the front
	// of the ordered txnIDs buffer.
	prevResolvedTime := a.mu.committed.ResolvedTime()
	for a.mu.txnIDs.Len() != 0 {
		id := a.mu.txnIDs.GetFirst()
		if !a.mu.committed.IsResolved(id) {
			// Advance the resolved time through the gap before this
			// unapplied txn. Since the coordinator records txns in
			// timestamp order, if txnIDs contains a txn at timestamp T,
			// there are no txns for this applier in the interval
			// (resolvedTime, T). The resolved time can safely advance
			// to T.Prev().
			a.mu.committed.UpdateResolvedTime(id.Timestamp.Prev())
			break
		}
		a.mu.committed.UpdateResolvedTime(id.Timestamp)
		a.mu.txnIDs.RemoveFirst()
	}

	resolvedTime := a.mu.committed.ResolvedTime()
	if prevResolvedTime.Less(resolvedTime) {
		a.localResolvedTime.Set(resolvedTime)
	}

	a.drainSatisfiedHorizonWaitersLocked(readyBuffer)
	a.depResolver.Ready(completedTxn.TxnID, resolvedTime)
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

		waitingTxn.remainingDeps--
		a.mu.transactions[waitingID] = waitingTxn

		if waitingTxn.remainingDeps == 0 {
			if waitingTxn.EventHorizon.LessEq(a.getGlobalFrontierLocked()) {
				readyBuffer.AddLast(waitingTxn.Transaction)
			} else {
				heap.Push(&a.mu.horizonWaiting, horizonWaiter{
					txnID:   waitingID,
					horizon: waitingTxn.EventHorizon,
				})
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
	minFrontier := a.mu.committed.ResolvedTime()
	for _, frontier := range a.mu.remoteApplierResolvedTimes {
		if frontier.Less(minFrontier) {
			minFrontier = frontier
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
	for applierID, remoteFrontier := range a.mu.remoteApplierResolvedTimes {
		if horizon.After(remoteFrontier) {
			a.depResolver.WaitHorizon(a.id, applierID, horizon)
		}
	}
}

// drainSatisfiedHorizonWaitersLocked pops transactions from the
// horizonWaiting min-heap whose EventHorizon ≤ globalFrontier and adds
// them to readyBuffer.
//
// REQUIRES: a.mu is held.
func (a *Applier) drainSatisfiedHorizonWaitersLocked(
	readyBuffer *ring.Buffer[ldrdecoder.Transaction],
) {
	globalFrontier := a.getGlobalFrontierLocked()
	for a.mu.horizonWaiting.Len() > 0 {
		top := a.mu.horizonWaiting.peek()
		if !top.horizon.LessEq(globalFrontier) {
			break
		}
		heap.Pop(&a.mu.horizonWaiting)
		txn := a.mu.transactions[top.txnID]
		readyBuffer.AddLast(txn.Transaction)
	}
}

func (a *Applier) processRemoteUpdate(
	update DependencyUpdate, readyBuffer *ring.Buffer[ldrdecoder.Transaction],
) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Only advance the frontier, never regress it. Out-of-order updates can
	// arrive when a WaitHorizon eager response (with an older resolvedTime)
	// races with a Ready notification (with a newer resolvedTime) from the
	// same remote applier.
	if a.mu.remoteApplierResolvedTimes[update.TxnID.ApplierID].Less(update.ResolvedTime) {
		a.mu.remoteApplierResolvedTimes[update.TxnID.ApplierID] = update.ResolvedTime
	}

	if err := a.resolveDependencyLocked(
		update.TxnID, a.mu.remoteWaiting[update.TxnID], readyBuffer,
	); err != nil {
		return err
	}
	delete(a.mu.remoteWaiting, update.TxnID)
	a.drainSatisfiedHorizonWaitersLocked(readyBuffer)
	return nil
}
