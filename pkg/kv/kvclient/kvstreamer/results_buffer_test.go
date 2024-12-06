// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstreamer

import (
	"context"
	"math"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// TestInOrderResultsBuffer verifies that the inOrderResultsBuffer returns the
// results in the correct order (with increasing 'Position' values, or with
// increasing 'subRequestIdx' values when 'Position' values are equal).
// Additionally, it randomly asks the buffer to spill to disk.
func TestInOrderResultsBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	rng, _ := randutil.NewTestRand()
	st := cluster.MakeTestingClusterSettings()
	tempEngine, _, err := storage.NewTempEngine(
		ctx,
		base.DefaultTestTempStorageConfig(st),
		base.DefaultTestStoreSpec,
		nil, /* statsCollector */
	)
	require.NoError(t, err)
	defer tempEngine.Close()
	memMonitor := mon.NewMonitor(mon.Options{
		Name:     mon.MakeMonitorName("test-mem"),
		Res:      mon.MemoryResource,
		Settings: st,
	})
	memMonitor.Start(ctx, nil, mon.NewStandaloneBudget(math.MaxInt64))
	defer memMonitor.Stop(ctx)
	memAcc := memMonitor.MakeBoundAccount()
	diskMonitor := mon.NewMonitor(mon.Options{
		Name:     mon.MakeMonitorName("test-disk"),
		Res:      mon.DiskResource,
		Settings: st,
	})
	diskMonitor.Start(ctx, nil, mon.NewStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)

	budget := newBudget(mon.NewStandaloneUnlimitedAccount(), math.MaxInt /* limitBytes */)
	diskBuffer := TestResultDiskBufferConstructor(tempEngine, memAcc, diskMonitor)
	b := newInOrderResultsBuffer(budget, diskBuffer)
	defer b.close(ctx)

	for run := 0; run < 10; run++ {
		numExpectedResponses := rng.Intn(20) + 1
		require.NoError(t, b.init(ctx, numExpectedResponses))

		// Generate a set of results.
		var results []Result
		for i := 0; i < numExpectedResponses; i++ {
			// Randomly choose between Get and Scan responses.
			if rng.Float64() < 0.5 {
				get := makeResultWithGetResp(rng, rng.Float64() < 0.1 /* empty */)
				get.memoryTok.toRelease = rng.Int63n(100)
				get.Position = i
				results = append(results, get)
			} else {
				// Randomize the number of ranges the original Scan request
				// touches.
				numRanges := 1
				if rng.Float64() < 0.5 {
					numRanges = rng.Intn(10) + 1
				}
				for j := 0; j < numRanges; j++ {
					scan := makeResultWithScanResp(rng)
					scan.scanComplete = j+1 == numRanges
					scan.memoryTok.toRelease = rng.Int63n(100)
					scan.Position = i
					scan.subRequestIdx = int32(j)
					scan.subRequestDone = true
					results = append(results, scan)
				}
			}
		}

		// Randomize the order in which the results are added into the buffer.
		addOrder := make([]int, len(results))
		for i := range addOrder {
			addOrder[i] = i
		}
		rng.Shuffle(len(addOrder), func(i, j int) {
			addOrder[i], addOrder[j] = addOrder[j], addOrder[i]
		})

		var received []Result
		for {
			r, allComplete, err := b.get(ctx)
			require.NoError(t, err)
			received = append(received, r...)
			if allComplete {
				break
			}

			budget.mu.Lock()
			b.Lock()
			numToAdd := rng.Intn(len(addOrder)) + 1
			for i := 0; i < numToAdd; i++ {
				r := results[addOrder[0]]
				require.NoError(t, budget.consumeLocked(ctx, r.memoryTok.toRelease, false /* allowDebt */))
				b.addLocked(r)
				addOrder = addOrder[1:]
			}
			b.doneAddingLocked(ctx)
			b.Unlock()
			budget.mu.Unlock()

			// With 50% probability, try spilling some of the buffered results
			// to disk.
			if rng.Float64() < 0.5 {
				spillingPriority := rng.Intn(numExpectedResponses)
				var spillableSize int64
				for _, buffered := range b.(*inOrderResultsBuffer).buffered {
					if !buffered.onDisk && buffered.Position > spillingPriority {
						spillableSize += buffered.memoryTok.toRelease
					}
				}

				if spillableSize > 0 {
					budget.mu.Lock()
					// With 50% probability, ask the buffer to spill more than
					// possible.
					if rng.Float64() < 0.5 {
						ok, err := b.spill(ctx, 2*spillableSize, spillingPriority)
						require.False(t, ok)
						require.NoError(t, err)
					} else {
						ok, err := b.spill(ctx, spillableSize/2, spillingPriority)
						require.True(t, ok)
						require.NoError(t, err)
					}
					budget.mu.Unlock()
				}
			}
		}
		require.Equal(t, results, received)

		// Simulate releasing all returned results at once to prepare the buffer
		// for the next run.
		require.Equal(t, len(results), b.numUnreleased())
		for range received {
			b.releaseOne()
		}
	}
}

func makeResultWithGetResp(rng *rand.Rand, empty bool) Result {
	var r Result
	r.GetResp = &kvpb.GetResponse{}
	if !empty {
		rawBytes := make([]byte, rng.Intn(20)+1)
		rng.Read(rawBytes)
		r.GetResp.Value = &roachpb.Value{
			RawBytes: rawBytes,
			Timestamp: hlc.Timestamp{
				WallTime: rng.Int63(),
				Logical:  rng.Int31(),
			},
		}
	}
	return r
}

func makeResultWithScanResp(rng *rand.Rand) Result {
	var r Result
	// Sometimes generate zero-length batchResponses.
	batchResponses := make([][]byte, rng.Intn(20))
	for i := range batchResponses {
		batchResponse := make([]byte, rng.Intn(20)+1)
		rng.Read(batchResponse)
		batchResponses[i] = batchResponse
	}
	r.ScanResp = &kvpb.ScanResponse{
		BatchResponses: batchResponses,
	}
	return r
}
