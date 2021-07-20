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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowflow"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

type mockScatterer struct {
	syncutil.Mutex
	curNode  int
	numNodes int
}

var _ splitAndScatterer = &mockScatterer{}

// This mock implementation of the split and scatterer simulates a scattering of
// ranges.
func (s *mockScatterer) scatter(
	_ context.Context, _ keys.SQLCodec, _ roachpb.Key,
) (roachpb.NodeID, error) {
	s.Lock()
	defer s.Unlock()
	targetNodeID := roachpb.NodeID(s.curNode + 1)
	s.curNode = (s.curNode + 1) % s.numNodes
	return targetNodeID, nil
}

func (s *mockScatterer) split(_ context.Context, _ keys.SQLCodec, _ roachpb.Key) error {
	return nil
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

	testCases := []struct {
		name     string
		procSpec execinfrapb.SplitAndScatterSpec
		// The number of output streams.
		numStreams int
		// The number of output nodes. We expect this to be equal to the number of
		// streams. If it is less than the number of streams, we don't expect any
		// outputs to some of the nodes, if it's more than the number of streams we
		// expect extra rows to be sent to the 0th stream.
		numNodes int
		// expectedDistribution is a mapping from stream to the expected number of
		// rows we expect to receive on this stream.
		expectedDistribution map[int]int
	}{
		{
			name: "chunks-roundrobin",
			procSpec: execinfrapb.SplitAndScatterSpec{
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
			numNodes:   4,
			expectedDistribution: map[int]int{
				0: 7, // all entries from chunk 1
				1: 3, // all entries from chunk 2
				2: 0,
				3: 0,
			},
		},
		{
			name: "more-chunks-than-processors-with-redirect",
			procSpec: execinfrapb.SplitAndScatterSpec{
				Chunks: []execinfrapb.SplitAndScatterSpec_RestoreEntryChunk{
					{
						Entries: []execinfrapb.RestoreSpanEntry{
							{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
						},
					},
					{
						Entries: []execinfrapb.RestoreSpanEntry{
							{Span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}},
						},
					},
					{
						Entries: []execinfrapb.RestoreSpanEntry{
							{Span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
						},
					},
					{
						Entries: []execinfrapb.RestoreSpanEntry{
							{Span: roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("e")}},
						},
					},
					{
						Entries: []execinfrapb.RestoreSpanEntry{
							{Span: roachpb.Span{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")}},
						},
					},
					{
						Entries: []execinfrapb.RestoreSpanEntry{
							{Span: roachpb.Span{Key: roachpb.Key("f"), EndKey: roachpb.Key("g")}},
						},
					},
					{
						Entries: []execinfrapb.RestoreSpanEntry{
							{Span: roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}},
						},
					},
					{
						Entries: []execinfrapb.RestoreSpanEntry{
							{Span: roachpb.Span{Key: roachpb.Key("h"), EndKey: roachpb.Key("i")}},
						},
					},
				},
			},
			numStreams: 4,
			numNodes:   5,
			// Expect a round-robin distribution, but the first scatter request will
			// go to scattering the chunk. We also expect an entry scattered to the
			// 5th node will go to stream 0 (default stream), since that node does not
			// have a corresponding stream. This may be the case where we don't want
			// to plan a specific processor on a node (perhaps due to incompatible
			// distsql versions).
			expectedDistribution: map[int]int{
				0: 3, // Entry 1, Entry 5 (redirected here), Entry 6
				1: 2, // Entry 2, Entry 7
				2: 2, // Entry 3, Entry 8
				3: 1, // Entry 4
				// 4: 0 // Entry 5 gets redirected to stream 0 since stream 4 does not exist.
			},
		},
	}

	ctx := context.Background()

	for _, c := range testCases {
		testName := c.name
		if len(testName) == 0 {
			testName = fmt.Sprintf("%d-streams/%d-chunks", c.numStreams, len(c.procSpec.Chunks))
		}
		t.Run(testName, func(t *testing.T) {
			defaultStream := int32(0)
			rangeRouterSpec := execinfrapb.OutputRouterSpec_RangeRouterSpec{
				Encodings: []execinfrapb.OutputRouterSpec_RangeRouterSpec_ColumnEncoding{
					{
						Column:   0,
						Encoding: descpb.DatumEncoding_ASCENDING_KEY,
					},
				},
				DefaultDest: &defaultStream,
			}
			for stream := 0; stream < c.numStreams; stream++ {
				// In this test, nodes are 1 indexed.
				startBytes, endBytes, err := routingSpanForNode(roachpb.NodeID(stream + 1))
				require.NoError(t, err)

				span := execinfrapb.OutputRouterSpec_RangeRouterSpec_Span{
					Start:  startBytes,
					End:    endBytes,
					Stream: int32(stream),
				}
				rangeRouterSpec.Spans = append(rangeRouterSpec.Spans, span)
			}

			routerSpec := execinfrapb.OutputRouterSpec{
				Type:            execinfrapb.OutputRouterSpec_BY_RANGE,
				RangeRouterSpec: rangeRouterSpec,
			}
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
					Settings: st,
					DB:       kvDB,
					Codec:    keys.SystemSQLCodec,
				},
				EvalCtx:     &evalCtx,
				DiskMonitor: testDiskMonitor,
			}

			colTypes := splitAndScatterOutputTypes
			var wg sync.WaitGroup
			out, err := rowflow.MakeTestRouter(ctx, &flowCtx, &routerSpec, recvs, colTypes, &wg)
			require.NoError(t, err)

			post := execinfrapb.PostProcessSpec{}
			proc, err := newSplitAndScatterProcessor(&flowCtx, 0 /* processorID */, c.procSpec, &post, out)
			require.NoError(t, err)
			ssp, ok := proc.(*splitAndScatterProcessor)
			if !ok {
				t.Fatal("expected the processor that's created to be a split and scatter processor")
			}

			// Inject a mock scatterer.
			ssp.scatterer = &mockScatterer{numNodes: c.numNodes}

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
				streamDistribution[streamID] = 0
				for {
					row := buf.NextNoMeta(t)
					if row == nil {
						break
					}

					receivedEntriesCount++
					streamDistribution[streamID]++
				}
			}
			require.Equal(t, c.expectedDistribution, streamDistribution)

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
