// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvstreamer

import (
	"context"
	"math"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
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
// results in the correct order (with increasing 'position' values).
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
	)
	require.NoError(t, err)
	defer tempEngine.Close()
	diskMonitor := mon.NewMonitor(
		"test-disk",
		mon.DiskResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	diskMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)

	budget := newBudget(nil /* acc */, math.MaxInt /* limitBytes */)
	diskBuffer := TestResultDiskBufferConstructor(tempEngine, diskMonitor)
	b := newInOrderResultsBuffer(budget, diskBuffer)
	defer b.close(ctx)

	for run := 0; run < 10; run++ {
		numExpectedResponses := rng.Intn(20) + 1
		require.NoError(t, b.init(ctx, numExpectedResponses))

		// Generate a set of results.
		var results []Result
		var addOrder []int
		for i := 0; i < numExpectedResponses; i++ {
			// Randomly choose between Get and Scan responses.
			if rng.Float64() < 0.5 {
				get := makeResultWithGetResp(rng, rng.Float64() < 0.1 /* empty */)
				get.position = i
				results = append(results, get)
			} else {
				scan := makeResultWithScanResp(rng)
				// TODO(yuzefovich): once lookup joins are supported, make Scan
				// responses spanning multiple ranges for a single original Scan
				// request.
				scan.ScanResp.Complete = true
				scan.position = i
				results = append(results, scan)
			}
			results[len(results)-1].memoryTok.toRelease = rng.Int63n(100)
			addOrder = append(addOrder, i)
		}

		// Randomize the order in which the results are added into the buffer.
		rng.Shuffle(len(addOrder), func(i, j int) {
			addOrder[i], addOrder[j] = addOrder[j], addOrder[i]
		})

		var received []Result
		var addOrderIdx int
		for {
			r, allComplete, err := b.get(ctx)
			require.NoError(t, err)
			received = append(received, r...)
			if allComplete {
				break
			}

			numToAdd := rng.Intn(len(results)-addOrderIdx) + 1
			toAdd := make([]Result, numToAdd)
			for i := range toAdd {
				toAdd[i] = results[addOrder[addOrderIdx]]
				addOrderIdx++
			}
			b.add(toAdd)

			// With 50% probability, try spilling some of the buffered results
			// to disk.
			if rng.Float64() < 0.5 {
				spillingPriority := rng.Intn(numExpectedResponses)
				var spillableSize int64
				for _, buffered := range b.(*inOrderResultsBuffer).buffered {
					if !buffered.onDisk && buffered.position > spillingPriority {
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

func fillEnqueueKeys(r *Result, rng *rand.Rand) {
	r.EnqueueKeysSatisfied = make([]int, rng.Intn(20)+1)
	for i := range r.EnqueueKeysSatisfied {
		r.EnqueueKeysSatisfied[i] = rng.Int()
	}
}

func makeResultWithGetResp(rng *rand.Rand, empty bool) Result {
	var r Result
	r.GetResp = &roachpb.GetResponse{}
	if !empty {
		rawBytes := make([]byte, rng.Intn(20)+1)
		rng.Read(rawBytes)
		r.GetResp.Value = &roachpb.Value{
			RawBytes: rawBytes,
			Timestamp: hlc.Timestamp{
				WallTime:  rng.Int63(),
				Logical:   rng.Int31(),
				Synthetic: rng.Float64() < 0.5,
			},
		}
	}
	fillEnqueueKeys(&r, rng)
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
	r.ScanResp.ScanResponse = &roachpb.ScanResponse{
		BatchResponses: batchResponses,
	}
	fillEnqueueKeys(&r, rng)
	return r
}
