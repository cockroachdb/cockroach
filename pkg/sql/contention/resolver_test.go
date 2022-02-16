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
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type testData struct {
	contentionpb.ResolvedTxnID
	coordinatorNodeID string
}

func TestResolver(t *testing.T) {
	statusServer := newFakeStatusServerCluster()
	resolver := newResolver(statusServer.txnIDResolution, 0 /* sizeHint */)
	ctx := context.Background()

	t.Run("normal_resolution", func(t *testing.T) {
		tcs := []testData{
			{
				ResolvedTxnID: contentionpb.ResolvedTxnID{
					TxnID:            uuid.FastMakeV4(),
					TxnFingerprintID: 100,
				},
				coordinatorNodeID: "1",
			},
			{
				ResolvedTxnID: contentionpb.ResolvedTxnID{
					TxnID:            uuid.FastMakeV4(),
					TxnFingerprintID: 101,
				},
				coordinatorNodeID: "1",
			},
			{
				ResolvedTxnID: contentionpb.ResolvedTxnID{
					TxnID:            uuid.FastMakeV4(),
					TxnFingerprintID: 102,
				},
				coordinatorNodeID: "1",
			},
			{
				ResolvedTxnID: contentionpb.ResolvedTxnID{
					TxnID:            uuid.FastMakeV4(),
					TxnFingerprintID: 200,
				},
				coordinatorNodeID: "2",
			},
			{
				ResolvedTxnID: contentionpb.ResolvedTxnID{
					TxnID:            uuid.FastMakeV4(),
					TxnFingerprintID: 201,
				},
				coordinatorNodeID: "2",
			},
			{
				ResolvedTxnID: contentionpb.ResolvedTxnID{
					TxnID:            uuid.FastMakeV4(),
					TxnFingerprintID: 300,
				},
				coordinatorNodeID: "3",
			},
		}
		rand.Shuffle(len(tcs), func(i, j int) {
			tcs[i], tcs[j] = tcs[j], tcs[i]
		})
		populateFakeStatusServerCluster(statusServer, tcs)
		input, expected := generateUnresolvedContentionEventsFromTestData(t, tcs)
		resolver.enqueue(input)
		actual, err := resolver.dequeue(ctx)
		require.NoError(t, err)

		expected = sortResolvedContentionEvents(expected)
		actual = sortResolvedContentionEvents(actual)

		require.Equal(t, expected, actual)
		require.Empty(t, resolver.mu.unresolvedEvents)
	})

	t.Run("retry_after_encountering_provisional_value", func(t *testing.T) {
		// Reset the status server from previous test.
		statusServer.clear()
		activeTxnID := uuid.FastMakeV4()
		tcs := []testData{
			{
				ResolvedTxnID: contentionpb.ResolvedTxnID{
					TxnID:            uuid.FastMakeV4(),
					TxnFingerprintID: 101,
				},
				coordinatorNodeID: "3",
			},
			{
				// This is a provisional entry in TxnID Cache, representing that
				// the transaction is open and its fingerprint ID is not yet
				// available.
				ResolvedTxnID: contentionpb.ResolvedTxnID{
					TxnID:            activeTxnID,
					TxnFingerprintID: roachpb.InvalidTransactionFingerprintID,
				},
				coordinatorNodeID: "1",
			},
			{
				ResolvedTxnID: contentionpb.ResolvedTxnID{
					TxnID:            uuid.FastMakeV4(),
					TxnFingerprintID: 102,
				},
				coordinatorNodeID: "1",
			},
		}

		populateFakeStatusServerCluster(statusServer, tcs)
		input, expected := generateUnresolvedContentionEventsFromTestData(t, tcs)
		resolver.enqueue(input)
		actual, err := resolver.dequeue(ctx)
		require.NoError(t, err)

		expected = sortResolvedContentionEvents(expected)
		actual = sortResolvedContentionEvents(actual)
		require.Equal(t, expected, actual)

		require.Equal(t, 1 /* expected */, len(resolver.mu.unresolvedEvents),
			"expected resolver to retry resolution for active txns, "+
				"but it did not")
		require.True(t, activeTxnID.Equal(resolver.mu.unresolvedEvents[0].BlockingEvent.TxnMeta.ID))
		require.Empty(t, resolver.mu.remainingRetries,
			"expected resolver not to create retry record for active txns, "+
				"but it did")

		// Create 1 new entry to simulate another new txn that finished execution,
		// and update the existing entry to have a valid transaction fingerprint id.
		newTxns := []testData{
			{
				ResolvedTxnID: contentionpb.ResolvedTxnID{
					TxnID:            uuid.FastMakeV4(),
					TxnFingerprintID: 2000,
				},
				coordinatorNodeID: "2",
			},
			{
				ResolvedTxnID: contentionpb.ResolvedTxnID{
					TxnID:            activeTxnID,
					TxnFingerprintID: 1000,
				},
				coordinatorNodeID: "1",
			},
		}
		populateFakeStatusServerCluster(statusServer, newTxns)
		newInput, newExpected := generateUnresolvedContentionEventsFromTestData(t, newTxns)
		// The txn with 'activeTxnID' is already present in the resolver. Omit it
		// from the input.
		newInput = newInput[:1]
		newExpected = sortResolvedContentionEvents(newExpected)

		// Enqueue the new contention event into the resolver.
		resolver.enqueue(newInput)

		actual, err = resolver.dequeue(ctx)
		require.NoError(t, err)
		actual = sortResolvedContentionEvents(actual)
		require.Equal(t, newExpected, actual)

		// Nothing should be left inside the resolver after we are done.
		require.Empty(t, resolver.mu.unresolvedEvents)
		require.Empty(t, resolver.mu.remainingRetries)
		require.Empty(t, resolver.mu.resolvedEvents)
	})

	t.Run("retry_after_missing_value", func(t *testing.T) {
		statusServer.clear()
		missingTxnID := uuid.FastMakeV4()
		tcs := []testData{
			{
				ResolvedTxnID: contentionpb.ResolvedTxnID{
					TxnID:            uuid.FastMakeV4(),
					TxnFingerprintID: 101,
				},
				coordinatorNodeID: "3",
			},
			{
				ResolvedTxnID: contentionpb.ResolvedTxnID{
					TxnID:            uuid.FastMakeV4(),
					TxnFingerprintID: 102,
				},
				coordinatorNodeID: "1",
			},
			{
				ResolvedTxnID: contentionpb.ResolvedTxnID{
					TxnID:            missingTxnID,
					TxnFingerprintID: 1000,
				},
				coordinatorNodeID: "1",
			},
		}
		// Explicitly omit the last missing txnID from status server to simulate an
		// evicted txnID from txnIDCache.
		populateFakeStatusServerCluster(statusServer, tcs[:2])
		input, expected := generateUnresolvedContentionEventsFromTestData(t, tcs)
		missingTxnMissingRetryKey := input[2].Hash()

		resolver.enqueue(input)
		actual, err := resolver.dequeue(ctx)
		require.NoError(t, err)

		// Assert that we have only resolved the first two txnIDs, the third txnID
		// should not be resolved.
		require.Equal(t,
			sortResolvedContentionEvents(expected[:2]), sortResolvedContentionEvents(actual))
		require.Equal(t, 1, len(resolver.mu.remainingRetries))
		remainingRetryBudget, foundRetryRecord := resolver.mu.remainingRetries[missingTxnMissingRetryKey]
		require.True(t, foundRetryRecord)
		require.Equal(t, retryBudgetForMissingResult, remainingRetryBudget)

		actual, err = resolver.dequeue(ctx)
		require.NoError(t, err)
		require.Empty(t, actual)

		// Attempt to resolve again, simulating the firing of the resolution
		// interval timer. This attempt will fail again, which cause the
		// event to be discarded.
		actual, err = resolver.dequeue(ctx)
		require.NoError(t, err)
		require.Empty(t, actual)

		// The retry record should be cleared.
		require.Empty(t, resolver.mu.remainingRetries)

		// The unresolved event should not be re-queued.
		require.Empty(t, resolver.mu.unresolvedEvents)
	})

	t.Run("handle_transient_rpc_failure", func(t *testing.T) {
		missingTxnID1 := uuid.FastMakeV4()
		statusServer.clear()
		tcs := []testData{
			{
				ResolvedTxnID: contentionpb.ResolvedTxnID{
					TxnID:            uuid.FastMakeV4(),
					TxnFingerprintID: 201,
				},
				coordinatorNodeID: "2",
			},
			{
				ResolvedTxnID: contentionpb.ResolvedTxnID{
					TxnID:            missingTxnID1,
					TxnFingerprintID: 301,
				},
				coordinatorNodeID: "3",
			},
			{
				ResolvedTxnID: contentionpb.ResolvedTxnID{
					TxnID:            uuid.FastMakeV4(),
					TxnFingerprintID: 100,
				},
				coordinatorNodeID: "1",
			},
			{
				ResolvedTxnID: contentionpb.ResolvedTxnID{
					TxnID:            uuid.FastMakeV4(),
					TxnFingerprintID: 101,
				},
				coordinatorNodeID: "1",
			},
		}
		populateFakeStatusServerCluster(statusServer, tcs)

		injectedErr := errors.New("injected error")
		// Take down node 3.
		statusServer.setStatusServerError(
			"3" /* coordinatorNode */, injectedErr)

		input, expected := generateUnresolvedContentionEventsFromTestData(t, tcs)
		missingTxnRemainingRetryKey1 := input[1].Hash()

		expected = sortResolvedContentionEvents(expected)
		expectedWithOnlyResultsFromAvailableNodes := make([]contentionpb.ExtendedContentionEvent, 0, len(expected))
		for _, event := range expected {
			if event.BlockingEvent.TxnMeta.CoordinatorNodeID != 3 {
				expectedWithOnlyResultsFromAvailableNodes = append(expectedWithOnlyResultsFromAvailableNodes, event)
			}
		}

		resolver.enqueue(input)
		actual, err := resolver.dequeue(ctx)
		require.ErrorIs(t, err, injectedErr)

		require.Equal(t, 1, len(resolver.mu.unresolvedEvents),
			"expected unresolved contention events be re-queued "+
				"when experience RPC errors, but the events were dropped")

		actual = sortResolvedContentionEvents(actual)
		require.Equal(t, expectedWithOnlyResultsFromAvailableNodes, actual)
		require.Equal(t, 1, len(resolver.mu.unresolvedEvents),
			"expected unresolved contention events be re-queued after "+
				"encountering RPC failure, but it was dropped")
		require.Equal(t, 1, len(resolver.mu.remainingRetries),
			"expected to have a retry record after RPC failure, but the "+
				"retry record is missing")
		remainingRetryBudget, found := resolver.mu.remainingRetries[missingTxnRemainingRetryKey1]
		require.True(t, found)
		require.Equal(t, retryBudgetForRPCFailure, remainingRetryBudget)

		// Throw in a second unresolved contention event where the RPC failure
		// is happening.
		missingTxnID2 := uuid.FastMakeV4()
		tcs = []testData{
			{
				ResolvedTxnID: contentionpb.ResolvedTxnID{
					TxnID:            missingTxnID2,
					TxnFingerprintID: 202,
				},
				coordinatorNodeID: "2",
			},
		}
		populateFakeStatusServerCluster(statusServer, tcs)
		// Take down node 2.
		statusServer.setStatusServerError(
			"2" /* coordinatorNodeID */, injectedErr)
		input2, expected2 := generateUnresolvedContentionEventsFromTestData(t, tcs)
		missingTxnRemainingRetryKey2 := input2[0].Hash()

		resolver.enqueue(input2)
		require.Equal(t, 2, len(resolver.mu.unresolvedEvents))
		// Attempt resolving again, this should still fail.
		require.ErrorIs(t, resolver.resolveLocked(ctx), injectedErr)
		require.Equal(t, 2, len(resolver.mu.remainingRetries))

		remainingRetryBudget, found = resolver.mu.remainingRetries[missingTxnRemainingRetryKey1]
		require.True(t, found)
		require.Equal(t, retryBudgetForRPCFailure-1, remainingRetryBudget,
			"expect retry budget be decremented after consecutive failures, "+
				"but it was not")

		remainingRetryBudget, found = resolver.mu.remainingRetries[missingTxnRemainingRetryKey2]
		require.True(t, found)
		require.Equal(t, retryBudgetForRPCFailure, remainingRetryBudget)

		// Perform 2 consecutive resolution to cause the `missingTxnID1` to be
		// dropped.
		require.ErrorIs(t, resolver.resolveLocked(ctx), injectedErr)
		require.ErrorIs(t, resolver.resolveLocked(ctx), injectedErr)

		require.Equal(t, 1, len(resolver.mu.unresolvedEvents))
		require.Equal(t, 1, len(resolver.mu.remainingRetries))
		require.Empty(t, resolver.mu.resolvedEvents)
		require.True(t, resolver.mu.unresolvedEvents[0].BlockingEvent.TxnMeta.ID.Equal(missingTxnID2))

		// Lift all injected RPC errors to simulate nodes coming back online.
		statusServer.clearErrors()
		actual, err = resolver.dequeue(ctx)
		require.NoError(t, err)
		require.Equal(t, expected2, actual)
		require.Empty(t, resolver.mu.remainingRetries)
		require.Empty(t, resolver.mu.unresolvedEvents)
	})
}

func sortResolvedContentionEvents(
	events []contentionpb.ExtendedContentionEvent,
) []contentionpb.ExtendedContentionEvent {
	sort.Slice(events, func(i, j int) bool {
		return events[i].BlockingTxnFingerprintID < events[j].BlockingTxnFingerprintID
	})
	return events
}

func generateUnresolvedContentionEventsFromTestData(
	t *testing.T, tcs []testData,
) (
	input []contentionpb.ExtendedContentionEvent,
	expected []contentionpb.ExtendedContentionEvent,
) {
	for _, tc := range tcs {
		event := contentionpb.ExtendedContentionEvent{}
		event.BlockingEvent.TxnMeta.ID = tc.TxnID
		coordinatorID, err := strconv.Atoi(tc.coordinatorNodeID)
		require.NoError(t, err)
		event.BlockingEvent.TxnMeta.CoordinatorNodeID = int32(coordinatorID)
		event.WaitingTxnID = tc.TxnID
		input = append(input, event)

		if tc.TxnFingerprintID != roachpb.InvalidTransactionFingerprintID {
			resolvedEvent := contentionpb.ExtendedContentionEvent{}
			resolvedEvent.BlockingEvent = event.BlockingEvent
			resolvedEvent.BlockingTxnFingerprintID = tc.TxnFingerprintID
			resolvedEvent.WaitingTxnID = event.WaitingTxnID
			resolvedEvent.WaitingTxnFingerprintID = tc.TxnFingerprintID
			expected = append(expected, resolvedEvent)
		}
	}
	return input, expected
}

func populateFakeStatusServerCluster(f fakeStatusServerCluster, tcs []testData) {
	for _, tc := range tcs {
		f.setTxnIDEntry(tc.coordinatorNodeID, tc.TxnID, tc.TxnFingerprintID)
		// TODO(azhng): wip: this is kind of a hack heh?
		f.setTxnIDEntry("local", tc.TxnID, tc.TxnFingerprintID)
	}
}
