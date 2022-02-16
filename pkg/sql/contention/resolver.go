// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package contention

import (
	"context"
	"math"
	"sort"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// resolverQueue's main responsibility is to map the transaction IDs
// present in the contention events into transaction fingerprint IDs.
// When a transaction waits because of contention, we will not yet know the
// blocking transaction's fingerprint ID. The mapping from the txnID of the
// blocking transaction to its transaction fingerprint ID is likely stored in
// txnID cache of a different node. So in order to translate blocking
// transaction's txnID into transaction fingerprint ID, RPC calls need to be
// issued to other nodes to query their transaction ID caches.
// These operations can be very expensive. To amortize the network cost, the
// resolver batches up the contention events in the queue with same coordinator
// ID, and send only one RPC request per remote node when the resolution is
// explicitly requested.
type resolverQueue interface {
	// enqueue queues a block of unresolved contention events into resolverQueue
	// to be resolved later.
	enqueue([]contentionpb.ExtendedContentionEvent)

	// dequeue attempts to resolveLocked the pending unresolved contention events
	// and returns all the resolved contention events. The contention events
	// that cannot be resolved will either be re-queued or dropped (only after
	// retry has been attempted).
	dequeue(context.Context) ([]contentionpb.ExtendedContentionEvent, error)
}

const (
	// retryBudgetForMissingResult is set to a relatively low value. This is
	// because when a txnID entry is missing on remote node, there are two
	// scenarios:
	// 1. the txnID is in the writer buffer of a remote node, and it's not yet
	//    inserted into the txnID cache.
	//    The RPC handler for TxnIDResolution forces the txnID cache to drain its
	//    write buffer if the handler encounters txnIDs it doesn't know about.
	//    This means next retry should succeed or always fails.
	//
	//    More detail note: In CRDB, when a transaction starts executing, it
	//    immediately writes a provisional entry into txnID cache before even
	//    executing its first statement. (This means, before the transaction even
	//    has a chance of causing contention, it has made itself aware to other
	//    node that it exists). Due to the asynchronous nature of txnID cache,
	//    there's always non-zero probability that it takes a while before the
	//    transaction's write operation to complete. (E.g. this can happen when
	//    the cluster QPS is very low and it didn't generate enough transaction to
	//    fill the write buffer of the txnID cache). Since when the
	//    TxnIDResolution handler encounters a missing txnID entry, it will force
	//    a txnID cache flush. This means that next time when the resolver attempt
	//    to issue RPC request, the txnID entry should be present in the txnID
	//    cache in the remote node. If the txnID cache entry is still missing upon
	//    subsequent RPC call, then this leads us to the next scenario.
	// 2. the txnID entry is permanently lost. This can happen due to TxnID cache
	//    eviction (very not ideal situation, indicating that the cluster is
	//    very overloaded or a transaction has been running way too long) or
	//    in-memory data corruption (shouldn't happen in normal circumstances,
	//    since access to txnID cache is all synchronized). In this case, no
	//    amount of retries will be able to resolveLocked the txnID.
	retryBudgetForMissingResult = uint32(1)

	// retryBudgetForRPCFailure is the number of times the resolverQueue will
	// retry resolving until giving up. This needs to be a finite number to handle
	// the case where the node is permanently removed from the cluster.
	retryBudgetForRPCFailure = uint32(3)

	// retryBudgetForTxnInProgress is a special value indicating that the resolver should
	// indefinitely retry the resolution. This is because the retry is due to the
	// transaction is still in progress.
	retryBudgetForTxnInProgress = uint32(math.MaxUint32)
)

// ResolverEndpoint is an alias for the TxnIDResolution RPC endpoint in the
// status server.
type ResolverEndpoint func(context.Context, *serverpb.TxnIDResolutionRequest) (*serverpb.TxnIDResolutionResponse, error)

type resolverQueueImpl struct {
	mu struct {
		syncutil.RWMutex

		unresolvedEvents []contentionpb.ExtendedContentionEvent
		resolvedEvents   []contentionpb.ExtendedContentionEvent

		// remainingRetries stores a mapping of each contention event to its
		// remaining number of retries attempts. The key in the map is the hash of
		// the contention event.
		remainingRetries map[uint64]uint32
	}

	resolverEndpoint ResolverEndpoint
}

var _ resolverQueue = &resolverQueueImpl{}

func newResolver(endpoint ResolverEndpoint, sizeHint int) *resolverQueueImpl {
	s := &resolverQueueImpl{
		resolverEndpoint: endpoint,
	}

	s.mu.unresolvedEvents = make([]contentionpb.ExtendedContentionEvent, 0, sizeHint)
	s.mu.resolvedEvents = make([]contentionpb.ExtendedContentionEvent, 0, sizeHint)
	s.mu.remainingRetries = make(map[uint64]uint32, sizeHint)

	return s
}

// enqueue implements the resolverQueue interface.
func (q *resolverQueueImpl) enqueue(block []contentionpb.ExtendedContentionEvent) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.mu.unresolvedEvents = append(q.mu.unresolvedEvents, block...)
}

// dequeue implements the resolverQueue interface.
func (q *resolverQueueImpl) dequeue(
	ctx context.Context,
) ([]contentionpb.ExtendedContentionEvent, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	err := q.resolveLocked(ctx)
	result := q.mu.resolvedEvents
	q.mu.resolvedEvents = q.mu.resolvedEvents[:0]

	return result, err
}

func (q *resolverQueueImpl) resolveLocked(ctx context.Context) error {
	queueCpy := make([]contentionpb.ExtendedContentionEvent, len(q.mu.unresolvedEvents))
	copy(queueCpy, q.mu.unresolvedEvents)

	// Clear the queue.
	q.mu.unresolvedEvents = q.mu.unresolvedEvents[:0]

	// We sort the queue by the CoordinatorNodeID.
	sort.Slice(queueCpy, func(i, j int) bool {
		return queueCpy[i].BlockingEvent.TxnMeta.CoordinatorNodeID <
			queueCpy[j].BlockingEvent.TxnMeta.CoordinatorNodeID
	})

	currentBatch, remaining := readUntilNextCoordinatorID(queueCpy)
	var allErrors error
	for len(currentBatch) > 0 {
		// TODO(azhng): we can inject random sleep/wait here to slow down our
		//  outbound RPC request rate. This can be down proactively by self-throttle
		//  the outbound RPC rate at a given cluster setting, or reactively
		//  by observing some node'q load metrics (e.g. QPS value) and start
		//  self-throttling once that QPS value exceed certain value.

		blockingTxnIDsReq, waitingTxnIDsReq := makeRPCRequestsFromBatch(currentBatch)

		blockingTxnIDsResp, err := q.resolverEndpoint(ctx, blockingTxnIDsReq)
		if err != nil {
			allErrors = errors.CombineErrors(allErrors, err)
		}

		waitingTxnIDsResp, err := q.resolverEndpoint(ctx, waitingTxnIDsReq)
		if err != nil {
			allErrors = errors.CombineErrors(allErrors, err)
		}

		resolvedBlockingTxnIDs, inProgressBlockingTxnIDs := extractResolvedAndInProgressTxnIDs(blockingTxnIDsResp)
		resolvedWaitingTxnIDs, inProgressWaitingTxnIDs := extractResolvedAndInProgressTxnIDs(waitingTxnIDsResp)

		for _, event := range currentBatch {
			needToRetryDueToBlockingTxnID, initialRetryBudgetDueToBlockingTxnID :=
				maybeUpdateTxnFingerprintID(
					event.BlockingEvent.TxnMeta.ID,
					&event.BlockingTxnFingerprintID,
					resolvedBlockingTxnIDs,
					inProgressBlockingTxnIDs,
				)

			needToRetryDueToWaitingTxnID, initialRetryBudgetDueToWaitingTxnID :=
				maybeUpdateTxnFingerprintID(
					event.WaitingTxnID,
					&event.WaitingTxnFingerprintID,
					resolvedWaitingTxnIDs,
					inProgressWaitingTxnIDs,
				)

			// The initial retry budget is
			// max(
			//  initialRetryBudgetDueToBlockingTxnID,
			//  initialRetryBudgetDueToWaitingTxnID,
			// ).
			initialRetryBudget := initialRetryBudgetDueToBlockingTxnID
			if initialRetryBudget < initialRetryBudgetDueToWaitingTxnID {
				initialRetryBudget = initialRetryBudgetDueToWaitingTxnID
			}

			if needToRetryDueToBlockingTxnID || needToRetryDueToWaitingTxnID {
				q.maybeRequeueEventForRetryLocked(event, initialRetryBudget)
			} else {
				q.mu.resolvedEvents = append(q.mu.resolvedEvents, event)
				delete(q.mu.remainingRetries, event.Hash())
			}
		}

		currentBatch, remaining = readUntilNextCoordinatorID(remaining)
	}

	return allErrors
}

func maybeUpdateTxnFingerprintID(
	txnID uuid.UUID,
	existingTxnFingerprintID *roachpb.TransactionFingerprintID,
	resolvedTxnIDs, inProgressTxnIDs map[uuid.UUID]roachpb.TransactionFingerprintID,
) (needToRetry bool, initialRetryBudget uint32) {
	// This means the txnID has already been resolved into transaction fingerprint
	// ID.
	if *existingTxnFingerprintID != roachpb.InvalidTransactionFingerprintID {
		return false /* needToRetry */, 0 /* initialRetryBudget */
	}

	// Sometimes DistSQL engine is used in weird ways. It is possible for a
	// DistSQL flow to exist without being associated with any transactions and
	// can still experience contentions. When that happens, we don't attempt to
	// resolve it.
	if uuid.Nil.Equal(txnID) {
		return false /* needToRetry */, 0 /* initialRetryBudget */
	}

	if resolvedTxnIDs == nil {
		return true /* needToRetry */, retryBudgetForRPCFailure
	}

	if _, ok := inProgressTxnIDs[txnID]; ok {
		return true /* needToRetry */, retryBudgetForTxnInProgress
	}

	if inProgressTxnIDs == nil {
		return true /* needToRetry */, retryBudgetForRPCFailure
	}

	if txnFingerprintID, ok := resolvedTxnIDs[txnID]; ok {
		*existingTxnFingerprintID = txnFingerprintID
		return false /* needToRetry */, 0 /* initialRetryBudget */
	}

	return true /* needToRetry */, retryBudgetForMissingResult
}

func (q *resolverQueueImpl) maybeRequeueEventForRetryLocked(
	event contentionpb.ExtendedContentionEvent, initialBudget uint32,
) (requeued bool) {
	var remainingRetryBudget uint32
	var ok bool

	if initialBudget == retryBudgetForTxnInProgress {
		delete(q.mu.remainingRetries, event.Hash())
	} else {
		// If we fail to resolve the result, we look up this event's remaining retry
		// count. If its retry budget is exhausted, we discard it. Else, we
		// re-queue the event for retry and decrement its retry budget for the
		// event.
		remainingRetryBudget, ok = q.mu.remainingRetries[event.Hash()]
		if !ok {
			remainingRetryBudget = initialBudget
		} else {
			remainingRetryBudget--
		}

		q.mu.remainingRetries[event.Hash()] = remainingRetryBudget

		if remainingRetryBudget == 0 {
			delete(q.mu.remainingRetries, event.Hash())
			return false /* requeued */
		}
	}

	q.mu.unresolvedEvents = append(q.mu.unresolvedEvents, event)
	return true /* requeued */
}

func readUntilNextCoordinatorID(
	sortedEvents []contentionpb.ExtendedContentionEvent,
) (eventsForFirstCoordinator, remaining []contentionpb.ExtendedContentionEvent) {
	if len(sortedEvents) == 0 {
		return nil /* eventsForFirstCoordinator */, nil /* remaining */
	}

	currentCoordinatorID := sortedEvents[0].BlockingEvent.TxnMeta.CoordinatorNodeID
	for idx, event := range sortedEvents {
		if event.BlockingEvent.TxnMeta.CoordinatorNodeID != currentCoordinatorID {
			return sortedEvents[:idx], sortedEvents[idx:]
		}
	}

	return sortedEvents, nil /* remaining */
}

func extractResolvedAndInProgressTxnIDs(
	resp *serverpb.TxnIDResolutionResponse,
) (resolvedTxnIDs, inProgressTxnIDs map[uuid.UUID]roachpb.TransactionFingerprintID) {
	if resp == nil {
		return nil /* resolvedTxnID */, nil /* inProgressTxnIDs */
	}

	resolvedTxnIDs = make(map[uuid.UUID]roachpb.TransactionFingerprintID, len(resp.ResolvedTxnIDs))
	inProgressTxnIDs = make(map[uuid.UUID]roachpb.TransactionFingerprintID, len(resp.ResolvedTxnIDs))

	for _, event := range resp.ResolvedTxnIDs {
		if event.TxnFingerprintID == roachpb.InvalidTransactionFingerprintID {
			inProgressTxnIDs[event.TxnID] = roachpb.InvalidTransactionFingerprintID
		} else {
			resolvedTxnIDs[event.TxnID] = event.TxnFingerprintID
		}
	}

	return resolvedTxnIDs, inProgressTxnIDs
}

// makeRPCRequestsFromBatch creates two TxnIDResolution RPC requests from the
// batch of contentionpb.ExtendedContentionEvent. If the event already contains
// a resolved transaction fingerprint ID, then the corresponding transaction ID
// is omitted from the RPC request payload.
func makeRPCRequestsFromBatch(
	batch []contentionpb.ExtendedContentionEvent,
) (blockingTxnIDReq, waitingTxnIDReq *serverpb.TxnIDResolutionRequest) {
	blockingTxnIDReq = &serverpb.TxnIDResolutionRequest{
		CoordinatorID: strconv.Itoa(int(batch[0].BlockingEvent.TxnMeta.CoordinatorNodeID)),
		TxnIDs:        make([]uuid.UUID, 0, len(batch)),
	}
	waitingTxnIDReq = &serverpb.TxnIDResolutionRequest{
		CoordinatorID: "local",
		TxnIDs:        make([]uuid.UUID, 0, len(batch)),
	}

	for i := range batch {
		if batch[i].BlockingTxnFingerprintID == roachpb.InvalidTransactionFingerprintID {
			blockingTxnIDReq.TxnIDs = append(blockingTxnIDReq.TxnIDs, batch[i].BlockingEvent.TxnMeta.ID)
		}
		if batch[i].WaitingTxnFingerprintID == roachpb.InvalidTransactionFingerprintID {
			waitingTxnIDReq.TxnIDs = append(waitingTxnIDReq.TxnIDs, batch[i].WaitingTxnID)
		}
	}

	return blockingTxnIDReq, waitingTxnIDReq
}
