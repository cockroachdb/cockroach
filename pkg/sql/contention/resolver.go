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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// resolverQueue's main responsibility is to resolve the transaction IDs
// present in the contention events into transaction fingerprint IDs (a.k.a
// Txn ID resolution). When a contention event is collected, it is likely that
// the two conflicting transactions originated from different nodes. This
// implies that in order to perform resolution, multiple RPC calls need
// to be issued to other nodes to query their transaction ID caches. These
// operations can be very expensive due to network calls. In order to amortize
// the network cost, the resolver batches up the contention events in the queue
// with same coordinator ID, and send only one RPC request per remote node
// when the resolution is explicitly requested. (That is, upon the invocation
// of resolve() method).
type resolverQueue interface {
	// enqueue queues a block of unresolved contention events into resolverQueue
	// to be resolved later.
	enqueue([]contentionpb.ExtendedContentionEvent)

	// resolve attempts to resolve the pending unresolved contention events.
	resolve(context.Context) error

	// dequeue returns all the resolved contention events and remove them from the
	// resolverQueue.
	dequeue() []contentionpb.ExtendedContentionEvent
}

const (
	// retryBudgetForMissingResult is set to a relatively low value. This is
	// because when a txnID entry is missing on remote node, there are two
	// scenarios:
	// 1. the txnID is in the concurrentWriteBuffer of the remote node and
	//    the worker goroutine hasn't had the chance to processed it yet.
	//    In CRDB, when a transaction starts executing, it immediately writes a
	//    provisional entry into txnID cache before even executing its first
	//    statement. (This means, before the transaction even has a chance of
	//    causing contention, it has made itself aware to other node that it
	//    exists). Due to the asynchronous nature of txnID cache, there's always
	//    non-zero probability that it takes a while before the transaction's
	//    write operation to complete. (E.g. this can happen when the cluster QPS
	//    is very low and it didn't generate enough transaction to fill the write
	//    buffer of the txnID cache). Since when the TxnIDResolution handler
	//    encounters a missing txnID entry, it will forces a txnID cache flush.
	//    This means that next time when the resolver attempt to issue RPC
	//    request, the txnID entry should be present in the txnID cache in the
	//    remote node. If the txnID cache entry is still missing upon subsequent
	//    RPC call, then this leads us to the next scenario.
	// 2. the txnID entry is permanently lost. This can happen due to TxnID cache
	//    eviction (very not ideal situation, indicating that the cluster is
	//    very overloaded or a transaction has been running way too long) or
	//    in-memory data corruption (shouldn't happen in normal circumstances,
	//    since access to txnID cache is all synchronized). In this case, no
	//    amount of retries will be able to resolve the txnID.
	retryBudgetForMissingResult = uint32(1)

	// retryBudgetForRPCFailure is the number of times the resolverQueue will
	// retry resolving until giving up. This needs to be a finite number to handle
	// the case where the node is permanently removed from the cluster.
	retryBudgetForRPCFailure = uint32(3)
)

// ResolverEndpoint is an alias for the TxnIDResolution RPC endpoint in the
// status server.
type ResolverEndpoint func(context.Context, *serverpb.TxnIDResolutionRequest) (*serverpb.TxnIDResolutionResponse, error)

type resolverQueueImpl struct {
	mu struct {
		syncutil.RWMutex

		unresolvedEvents []contentionpb.ExtendedContentionEvent
		resolvedEvents   []contentionpb.ExtendedContentionEvent

		retryRecord map[uuid.UUID]uint32
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
	s.mu.retryRecord = make(map[uuid.UUID]uint32, sizeHint)

	return s
}

func (s *resolverQueueImpl) enqueue(block []contentionpb.ExtendedContentionEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.unresolvedEvents = append(s.mu.unresolvedEvents, block...)
}

func (s *resolverQueueImpl) resolve(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	queueCpy := make([]contentionpb.ExtendedContentionEvent, len(s.mu.unresolvedEvents))
	copy(queueCpy, s.mu.unresolvedEvents)

	// Clear the queue.
	s.mu.unresolvedEvents = s.mu.unresolvedEvents[:0]

	// We sort the queue by the CoordinatorNodeID.
	sort.Slice(queueCpy, func(i, j int) bool {
		return queueCpy[i].Event.TxnMeta.CoordinatorNodeID < queueCpy[j].Event.TxnMeta.CoordinatorNodeID
	})

	currentBatch, remaining := readUntilNextCoordinatorID(queueCpy)
	var allErrors error
	for {
		if len(currentBatch) == 0 {
			break
		}

		// TODO(azhng): we can inject random sleep/wait here to slow down our
		//  outbound RPC request rate. This can be down proactively by self-throttle
		//  the outbound RPC rate at a given cluster setting, or reactively
		//  by observing some node's load metrics (e.g. QPS value) and start
		//  self-throttling once that QPS value exceed certain value.

		req := makeRPCRequestFromBatch(currentBatch)
		resp, err := s.resolverEndpoint(ctx, req)
		if err != nil {
			s.maybeRequeueBatchLocked(currentBatch, retryBudgetForRPCFailure)
			// Read next batch of unresolved contention events.
			currentBatch, remaining = readUntilNextCoordinatorID(remaining)
			allErrors = errors.CombineErrors(allErrors, err)
			continue
		}
		resolvedTxnIDs := resolvedTxnIDsToMap(resp.ResolvedTxnIDs)

		for _, event := range currentBatch {
			if txnFingerprintID, ok := resolvedTxnIDs[event.Event.TxnMeta.ID]; ok {
				// If the coordinator node indicates that it is aware of the requested
				// txnID but does not yet have the corresponding txnFingerprintID,
				// (e.g. when the transaction is still executing), we re-queue
				// the contention event, so we will check in with the coordinator node
				// again later. In this case, we don't want to update the retry
				// record since we are confident that the txnID entry on the coordinator
				// node has not yet being evicted.
				if txnFingerprintID == roachpb.InvalidTransactionFingerprintID {
					s.mu.unresolvedEvents = append(s.mu.unresolvedEvents, event)
					// Clear any retry record if there is any.
					delete(s.mu.retryRecord, event.Event.TxnMeta.ID)
					continue
				}
				// If we successfully resolve the contention event, we add it to the
				// result slice.
				event.TxnFingerprintID = txnFingerprintID
				s.mu.resolvedEvents = append(s.mu.resolvedEvents, event)

				// We clear the retry records of the event if there is any.
				delete(s.mu.retryRecord, event.Event.TxnMeta.ID)
			} else {
				s.maybeRequeueEventForRetryLocked(event, retryBudgetForMissingResult)
			}
		}

		currentBatch, remaining = readUntilNextCoordinatorID(remaining)
	}

	return allErrors
}

func (s *resolverQueueImpl) dequeue() []contentionpb.ExtendedContentionEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	value := s.mu.resolvedEvents
	s.mu.resolvedEvents = s.mu.resolvedEvents[:0]
	return value
}

func (s *resolverQueueImpl) maybeRequeueBatchLocked(
	batch []contentionpb.ExtendedContentionEvent, defaultBudget uint32,
) {
	for _, event := range batch {
		s.maybeRequeueEventForRetryLocked(event, defaultBudget)
	}
}

func (s *resolverQueueImpl) maybeRequeueEventForRetryLocked(
	event contentionpb.ExtendedContentionEvent, defaultBudget uint32,
) (requeued bool) {
	// If we fail to resolve the result, we look up this event's retry
	// record. If its retry budget is exhausted, we discard it. Else, we
	// re-queue the event for retry and decrement its retry budget for the
	// event.
	remainingRetryBudget, ok := s.mu.retryRecord[event.Event.TxnMeta.ID]
	if !ok {
		s.mu.unresolvedEvents = append(s.mu.unresolvedEvents, event)
		s.mu.retryRecord[event.Event.TxnMeta.ID] = defaultBudget
		return true /* requeued */
	}

	if remainingRetryBudget-1 == 0 {
		delete(s.mu.retryRecord, event.Event.TxnMeta.ID)
		return false /* requeued */
	}

	s.mu.retryRecord[event.Event.TxnMeta.ID]--
	s.mu.unresolvedEvents = append(s.mu.unresolvedEvents, event)
	return true /* requeued */
}

func readUntilNextCoordinatorID(
	sortedEvents []contentionpb.ExtendedContentionEvent,
) (eventsForFirstCoordinator, remaining []contentionpb.ExtendedContentionEvent) {
	if len(sortedEvents) == 0 {
		return nil /* eventsForFirstCoordinator */, nil /* remaining */
	}

	currentCoordinatorID := sortedEvents[0].Event.TxnMeta.CoordinatorNodeID
	for idx, event := range sortedEvents {
		if event.Event.TxnMeta.CoordinatorNodeID != currentCoordinatorID {
			return sortedEvents[:idx], sortedEvents[idx:]
		}
	}

	return sortedEvents, nil /* remaining */
}

func resolvedTxnIDsToMap(
	resolvedTxnIDs []contentionpb.ResolvedTxnID,
) map[uuid.UUID]roachpb.TransactionFingerprintID {
	result := make(map[uuid.UUID]roachpb.TransactionFingerprintID, len(resolvedTxnIDs))

	for _, resolvedTxnID := range resolvedTxnIDs {
		result[resolvedTxnID.TxnID] = resolvedTxnID.TxnFingerprintID
	}

	return result
}

func makeRPCRequestFromBatch(
	batch []contentionpb.ExtendedContentionEvent,
) *serverpb.TxnIDResolutionRequest {
	req := &serverpb.TxnIDResolutionRequest{
		CoordinatorNodeID: batch[0].Event.TxnMeta.CoordinatorNodeID,
		TxnIDs:            make([]uuid.UUID, 0, len(batch)),
	}

	for _, event := range batch {
		req.TxnIDs = append(req.TxnIDs, event.Event.TxnMeta.ID)
	}

	return req
}
