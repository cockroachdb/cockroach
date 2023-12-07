// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/stretchr/testify/require"
)

func TestBackupSpanBoundOnRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, sqlDB, _, cleanup := backupRestoreTestSetup(t, singleNode, 1, InitManualReplication)
	defer cleanup()

	rowsPerRange := 2
	numRanges := 30
	sqlDB.Exec(t, `ALTER TABLE data.bank SPLIT AT (SELECT generate_series($1::INT, $2::INT, $3::INT))`, rowsPerRange, (numRanges-1)*rowsPerRange, rowsPerRange)
	conn := tc.Conns[0]
	waitForTableSplit(t, conn, "bank", "data", numRanges)
	bankTableID := sqlutils.QueryTableID(t, conn, "data", "public", "bank")
	codec := tc.Server(0).Codec()
	bankSpan := makeTableSpan(codec, bankTableID)
	startTime := hlc.Timestamp{WallTime: 1}
	endTime := hlc.Timestamp{WallTime: 2}

	testutils.RunTrueAndFalse(t, "split-on-range", func(t *testing.T, splitOnRange bool) {
		specs := &execinfrapb.BackupDataSpec{
			Spans:           []roachpb.Span{bankSpan},
			BackupStartTime: startTime,
			BackupEndTime:   endTime,
		}
		maker := initRequestSpanMaker(splitOnRange, specs, tc.Servers[0].RangeDescIteratorFactory().(rangedesc.IteratorFactory))
		require.NoError(t, maker.createRequestSpans(ctx))
		expectedSpanCount := 1
		if splitOnRange {
			expectedSpanCount = expectedSpanCount * numRanges
		}
		require.Equal(t, expectedSpanCount, len(maker.requestSpans))
		f, err := span.MakeFrontier()
		require.NoError(t, err)
		for _, sp := range maker.requestSpans {
			require.NoError(t, f.AddSpansAt(sp.end, sp.span))
		}
		require.Equal(t, specs.BackupEndTime, f.Frontier())
		require.Equal(t, bankSpan, f.PeekFrontierSpan())
	})
}

func TestBackupSpanQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewPseudoRand()

	testutils.RunTrueAndFalse(t, "split-on-range", func(t *testing.T, splitOnRange bool) {
		for _, spanCount := range []int{1, 5, 20, 100, 1000} {
			spanMaker := &requestSpanMaker{
				requestSpans:    make([]spanAndTime, 0, spanCount),
				rangeSizedSpans: splitOnRange,
			}
			// Identify each request span with a unique start timestamp, just to test
			// that we queue every span in order.
			for i := range spanMaker.requestSpans {
				spanMaker.requestSpans[i].start = hlc.Timestamp{WallTime: int64(i)}
			}
			numSenders := rng.Intn(7) + 1
			var (
				expectedChunkSize int
				lastChunk         bool
				chunkCount        int
				expectedSpanIdx   int64
			)
			for {
				select {
				case spanSet := <-spanMaker.queueWork(numSenders):
					chunkCount++
					switch {
					case !splitOnRange:
						require.Equal(t, 1, len(spanSet))
					case expectedChunkSize == 0:
						// Implies we're inspecting the first chunk.
						expectedChunkSize = len(spanSet)
						require.Greater(t, 0, expectedChunkSize)
						require.LessOrEqual(t, expectedChunkSize, maxChunkSize)
					case len(spanSet) == expectedChunkSize:
						// Implies we're inspecting a middle chunk.
					case len(spanSet) < expectedChunkSize:
						// Implies we're inspecting the last chunk.
						require.False(t, lastChunk)
						lastChunk = true
					case len(spanSet) > expectedChunkSize:
						t.Fatalf("Something's wrong with this chunk")
					}
					for _, sp := range spanSet {
						// Check the spans are queued in the input order.
						require.Equal(t, expectedSpanIdx, sp.start.WallTime)
						expectedSpanIdx++
					}
				default:
					// If the number of spans is greater than numSenders*softMinPerWorkerChunkCount, we expect each worker to have at least 4 chunks:
					if spanCount > numSenders*softMinPerWorkerChunkCount {
						require.Greater(t, chunkCount, numSenders*softMinPerWorkerChunkCount)
					}
					// Done processing queue.
					return
				}
			}
		}
	})
}
