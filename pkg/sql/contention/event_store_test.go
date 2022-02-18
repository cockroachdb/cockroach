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
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestEventStore(t *testing.T) {
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	st := cluster.MakeTestingClusterSettings()
	// Disable automatic txn id resolution to prevent interference.
	TxnIDResolutionInterval.Override(ctx, &st.SV, time.Hour)
	statusServer := newFakeStatusServerCluster()

	now := timeutil.Now()
	store := newEventStore(st, statusServer.txnIDResolution, func() time.Time {
		return now
	} /* timeSrc */)
	store.start(ctx, stopper)

	// Minimum generate 10 contention events, up to 310 events.
	testSize := rand.Intn(300) + 10

	// Minimum create 2 coordinators, up to 12 nodes.
	numOfCoordinators := rand.Intn(10) + 2
	t.Logf("initializing %d events with %d distinct coordinators",
		testSize, numOfCoordinators)

	// Randomize input.
	data := randomlyGenerateTestData(testSize, numOfCoordinators)
	populateFakeStatusServerCluster(statusServer, data)

	input, expected := generateUnresolvedContentionEventsFromTestData(t, data, now)
	expectedMap := eventSliceToMap(expected)

	for _, event := range input {
		store.addEvent(event)
	}

	// The contention event should immediately be available to be read from
	// the event store after it is being processed by the intake goroutine.
	// Since we don't have direct control over when intake goroutine processes the
	// events, we wrap the assertion logic in the retry loop.
	testutils.SucceedsWithin(t, func() error {
		store.guard.ForceSync()
		numOfEntries := 0

		if err := store.ForEachEvent(func(actual *contentionpb.ExtendedContentionEvent) error {
			numOfEntries++
			expectedEvent, ok := expectedMap[actual.BlockingEvent.TxnMeta.ID]
			if !ok {
				return errors.Newf("expected to found contention event "+
					"with txnID=%s, but it was not found", actual.BlockingEvent.TxnMeta.ID.String())
			}
			if !actual.CollectionTs.Equal(expectedEvent.CollectionTs) {
				return errors.Newf("expected collection timestamp for the event to "+
					"be at least %s, but it is %s",
					expectedEvent.CollectionTs.String(), actual.CollectionTs.String())
			}
			if actual.BlockingTxnFingerprintID != roachpb.InvalidTransactionFingerprintID {
				return errors.Newf("expect blocking txn fingerprint id to be invalid, "+
					"but it is %d", actual.BlockingTxnFingerprintID)
			}
			if actual.WaitingTxnFingerprintID != roachpb.InvalidTransactionFingerprintID {
				return errors.Newf("expect waiting txn fingerprint id to be invalid, "+
					"but it is %d", actual.WaitingTxnFingerprintID)
			}
			return nil
		}); err != nil {
			return err
		}

		if numOfEntries != len(expectedMap) {
			return errors.Newf("expect to encounter %d events, but only %d events "+
				"were encountered", len(expectedMap), numOfEntries)
		}

		return nil
	}, 3*time.Second)

	// Since we are using the fake status server, there should not be any
	// errors.
	require.NoError(t, store.flushAndResolve(ctx))

	// Now that we've resolved all the txn fingerprint IDs, the event store has
	// all the information we expect.
	require.NoError(t, store.ForEachEvent(
		func(actual *contentionpb.ExtendedContentionEvent) error {
			expectedEvent, ok := expectedMap[actual.BlockingEvent.TxnMeta.ID]
			require.True(t, ok, "expected to found resolved contention event "+
				"with txnID=%s, but it was not found", actual.BlockingEvent.TxnMeta.ID.String())
			require.Equal(t, expectedEvent, *actual)
			return nil
		}))
}

func BenchmarkEventStoreIntake(b *testing.B) {
	ctx := context.Background()

	st := cluster.MakeTestingClusterSettings()
	statusServer := newFakeStatusServerCluster()

	e := roachpb.ContentionEvent{}
	b.SetBytes(int64(e.Size()))

	run := func(b *testing.B, store *eventStore, numOfConcurrentWriter int) {
		input := make([]contentionpb.ExtendedContentionEvent, 0, b.N)
		for i := 0; i < b.N; i++ {
			event := contentionpb.ExtendedContentionEvent{}
			event.BlockingEvent.TxnMeta.ID = uuid.FastMakeV4()
			input = append(input, event)
		}
		starter := make(chan struct{})

		b.ResetTimer()

		var wg sync.WaitGroup

		for writerIdx := 0; writerIdx < numOfConcurrentWriter; writerIdx++ {
			wg.Add(1)

			go func(writerIdx int) {
				defer wg.Done()

				<-starter

				numOfOps := b.N / numOfConcurrentWriter

				// Each writer reads from its own section of the input slice. Since the
				// input slice contains b.N values, each writer performs b.N / numOfOps
				// of inserts, this means each writer will be using
				// [inputOffset : inputOffset + numOfOps) section of the input slice.
				inputOffset := numOfOps * writerIdx

				for i := 0; i < numOfOps; i++ {
					store.addEvent(input[inputOffset+i])
				}
			}(writerIdx)
		}

		close(starter)
		wg.Wait()
	}

	for _, numOfConcurrentWriter := range []int{1, 24, 48} {
		b.Run(fmt.Sprintf("concurrentWriter=%d", numOfConcurrentWriter), func(b *testing.B) {
			store := newEventStore(st, statusServer.txnIDResolution, timeutil.Now)
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)

			store.start(ctx, stopper)

			run(b, store, numOfConcurrentWriter)
		})
	}
}

func eventSliceToMap(
	events []contentionpb.ExtendedContentionEvent,
) map[uuid.UUID]contentionpb.ExtendedContentionEvent {
	result := make(map[uuid.UUID]contentionpb.ExtendedContentionEvent)

	for _, ev := range events {
		result[ev.BlockingEvent.TxnMeta.ID] = ev
	}

	return result
}

func randomlyGenerateTestData(testSize int, numOfCoordinator int) []testData {
	tcs := make([]testData, 0, testSize)
	for i := 0; i < testSize; i++ {
		tcs = append(tcs, testData{
			blockingTxn: contentionpb.ResolvedTxnID{
				TxnID:            uuid.FastMakeV4(),
				TxnFingerprintID: roachpb.TransactionFingerprintID(math.MaxUint64 - uint64(i)),
			},
			waitingTxn: contentionpb.ResolvedTxnID{
				TxnID:            uuid.FastMakeV4(),
				TxnFingerprintID: roachpb.TransactionFingerprintID(math.MaxUint64/2 - uint64(i)),
			},
			coordinatorNodeID: strconv.Itoa(rand.Intn(numOfCoordinator)),
		})
	}

	return tcs
}
