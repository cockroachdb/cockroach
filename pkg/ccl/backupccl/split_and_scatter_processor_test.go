// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowflow"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

type mockScatterer struct {
	sync.Mutex
	curNode  int
	numNodes int
}

// This mock implementation of the split and scatterer simulates a scattering of
// ranges.
func (s *mockScatterer) splitAndScatterKey(
	_ context.Context, _ *kv.DB, _ *storageccl.KeyRewriter, _ roachpb.Key,
) (roachpb.NodeID, error) {
	s.Lock()
	defer s.Unlock()
	targetNodeID := roachpb.NodeID(s.curNode + 1)
	s.curNode = (s.curNode + 1) % s.numNodes
	return targetNodeID, nil
}

// TestSplitAndScatterProcessor does not test the underlying split and scatter
// requests. Those requests are mocked out with a deterministic scatterer. This
// test ensures that given a certain scattering, spans are directed to the
// correct stream.
func TestSplitAndScatterProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testcluster.StartTestCluster(t, 3 /* nodes */, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())
	kvDB := tc.Server(0).DB()

	// TODO: Test fallback to default stream.
	testCases := []struct {
		procSpec execinfrapb.SplitAndScatterSpec
		// Specifies the number of nodes/number of output processors.
		numStreams int
		// expectedDistribution is a mapping from stream to the expected number of
		// rows we expect to receive on this stream.
		expectedDistribution map[int]int
		// If there is more than one chunk in the test case, we may not be able to
		// deterministically determine where each row ends up since chunks are
		// scattered in parallel with the entries, but we can expect a range of
		// distributions. For an example, see the second test case below.
		allowedDelta int
	}{
		{
			procSpec: execinfrapb.SplitAndScatterSpec{
				NodeToStream: map[roachpb.NodeID]int32{
					1: 0,
					2: 1,
					3: 2,
					4: 3,
				},
				Chunks: []execinfrapb.SplitAndScatterSpec_RestoreEntryChunk{
					{
						Entries: []execinfrapb.RestoreSpanEntry{
							{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
							{Span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
							{Span: roachpb.Span{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")}},
							{Span: roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}},
							{Span: roachpb.Span{Key: roachpb.Key("i"), EndKey: roachpb.Key("j")}},
							{Span: roachpb.Span{Key: roachpb.Key("k"), EndKey: roachpb.Key("l")}},
							{Span: roachpb.Span{Key: roachpb.Key("m"), EndKey: roachpb.Key("n")}},
						},
					},
				},
			},
			numStreams: 4,
			// Expect a round-robin distribution, but the first scatter request will
			// go to scattering the chunk.
			expectedDistribution: map[int]int{
				0: 1,
				1: 2,
				2: 2,
				3: 2,
			},
		},
		{
			procSpec: execinfrapb.SplitAndScatterSpec{
				NodeToStream: map[roachpb.NodeID]int32{
					1: 0,
					2: 1,
					3: 2,
					4: 3,
				},
				Chunks: []execinfrapb.SplitAndScatterSpec_RestoreEntryChunk{
					{
						Entries: []execinfrapb.RestoreSpanEntry{
							{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
							{Span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
							{Span: roachpb.Span{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")}},
							{Span: roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}},
							{Span: roachpb.Span{Key: roachpb.Key("i"), EndKey: roachpb.Key("j")}},
							{Span: roachpb.Span{Key: roachpb.Key("k"), EndKey: roachpb.Key("l")}},
							{Span: roachpb.Span{Key: roachpb.Key("m"), EndKey: roachpb.Key("n")}},
						},
					},
					{
						Entries: []execinfrapb.RestoreSpanEntry{
							{Span: roachpb.Span{Key: roachpb.Key("o"), EndKey: roachpb.Key("p")}},
							{Span: roachpb.Span{Key: roachpb.Key("q"), EndKey: roachpb.Key("r")}},
							{Span: roachpb.Span{Key: roachpb.Key("s"), EndKey: roachpb.Key("t")}},
						},
					},
				},
			},
			numStreams: 4,
			expectedDistribution: map[int]int{
				0: 2,
				1: 2,
				2: 2,
				3: 2,
			},
			// Allow each stream to have received 1-3 rows.
			// We have 2 chunks and 10 entries across 4 streams, we expect every
			// stream to get between 1 and 3 entries. We have 12 scatters, when
			// distributed in a round robin evenly produces the distribution (3,3,3,3)
			// and the chunk scatters are not counted, so we'll see 2 streams only
			// receive 2 rows or 1 stream only receive 1 row, rather than all 3.
			allowedDelta: 1,
		},
	}

	var defaultStream int32 = 0
	var testRangeRouterSpec = execinfrapb.OutputRouterSpec_RangeRouterSpec{
		Spans: []execinfrapb.OutputRouterSpec_RangeRouterSpec_Span{},
		Encodings: []execinfrapb.OutputRouterSpec_RangeRouterSpec_ColumnEncoding{
			{
				Column:   0,
				Encoding: sqlbase.DatumEncoding_ASCENDING_KEY,
			},
		},
		DefaultDest: &defaultStream,
	}

	ctx := context.Background()
	routerSpec := execinfrapb.OutputRouterSpec{
		Type:            execinfrapb.OutputRouterSpec_BY_RANGE,
		RangeRouterSpec: testRangeRouterSpec,
	}

	for _, c := range testCases {
		// TODO: Also count the entries.
		t.Run(fmt.Sprintf("%d-streams/%d-chunks", c.numStreams, len(c.procSpec.Chunks)), func(t *testing.T) {
			bufs := make([]*distsqlutils.RowBuffer, c.numStreams)
			recvs := make([]execinfra.RowReceiver, c.numStreams)
			routerSpec.Streams = make([]execinfrapb.StreamEndpointSpec, c.numStreams)

			for i := 0; i < c.numStreams; i++ {
				bufs[i] = &distsqlutils.RowBuffer{}
				recvs[i] = bufs[i]
				routerSpec.Streams[i] = execinfrapb.StreamEndpointSpec{StreamID: execinfrapb.StreamID(i)}
			}

			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)

			testDiskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
			defer testDiskMonitor.Stop(ctx)

			flowCtx := execinfra.FlowCtx{
				Cfg: &execinfra.ServerConfig{
					Settings:    st,
					DB:          kvDB,
					DiskMonitor: testDiskMonitor,
				},
				EvalCtx: &evalCtx,
			}

			colTypes := splitAndScatterOutputTypes
			var wg sync.WaitGroup
			out, err := rowflow.MakeTestRouter(ctx, &flowCtx, &routerSpec, recvs, colTypes, &wg)
			require.NoError(t, err)

			proc, err := newSplitAndScatterProcessor(&flowCtx, 0 /* processorID */, c.procSpec, out)
			require.NoError(t, err)
			ssp, ok := proc.(*splitAndScatterProcessor)
			if !ok {
				t.Fatal("expected the processor that's created to be a split and scatter processor")
			}

			// Inject a mock scatterer.
			ssp.scatterer = &mockScatterer{numNodes: c.numStreams}

			ssp.Run(context.Background())
			wg.Wait()

			// Ensure that all the outputs are properly closed.
			for _, buf := range bufs {
				if !buf.ProducerClosed() {
					t.Fatalf("output RowReceiver not closed")
				}
			}

			// Check that the rows are distributed the way that we expect.
			streamDistribution := make(map[int]int)
			receivedEntriesCount := 0
			for streamID, buf := range bufs {
				for {
					row := buf.NextNoMeta(t)
					if row == nil {
						break
					}

					receivedEntriesCount += 1
					_, ok := streamDistribution[streamID]
					if ok {
						streamDistribution[streamID] += 1
					} else {
						streamDistribution[streamID] = 1
					}
				}
			}
			require.InDeltaMapValues(t, c.expectedDistribution, streamDistribution, float64(c.allowedDelta))

			// Check that the number of entries that we receive is the same as the
			// number we specified.
			expectedEntries := 0
			for _, chunk := range c.procSpec.Chunks {
				expectedEntries += len(chunk.Entries)
			}
			require.Equal(t, expectedEntries, receivedEntriesCount)
		})
	}
}
