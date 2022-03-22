// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

type partitionToEvent map[string][]streamingccl.Event

func TestStreamIngestionFrontierProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3 /* nodes */, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())
	kvDB := tc.Server(0).DB()

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	testDiskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer testDiskMonitor.Stop(ctx)

	registry := tc.Server(0).JobRegistry().(*jobs.Registry)
	flowCtx := execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			Settings:    st,
			DB:          kvDB,
			JobRegistry: registry,
		},
		EvalCtx:     &evalCtx,
		DiskMonitor: testDiskMonitor,
	}

	out := &distsqlutils.RowBuffer{}
	post := execinfrapb.PostProcessSpec{}

	var spec execinfrapb.StreamIngestionDataSpec
	// The stream address needs to be set with a scheme we support, but this test
	// will mock out the actual client.
	spec.StreamAddress = "randomgen://test/"
	pa1 := "randomgen://test1/"
	pa2 := "randomgen://test2/"

	v := roachpb.MakeValueFromString("value_1")
	v.Timestamp = hlc.Timestamp{WallTime: 1}
	sampleKV := roachpb.KeyValue{Key: roachpb.Key("key_1"), Value: v}

	for _, tc := range []struct {
		name                      string
		events                    partitionToEvent
		expectedFrontierTimestamp hlc.Timestamp
		frontierStartTime         hlc.Timestamp
	}{
		{
			name: "same-resolved-ts-across-partitions",
			events: partitionToEvent{pa1: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 1}),
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 4}),
			}, pa2: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 1}),
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 4}),
			}},
			expectedFrontierTimestamp: hlc.Timestamp{WallTime: 4},
		},
		{
			// No progress should be reported to the job since partition 2 has not
			// emitted a resolved ts.
			name: "no-checkpoints",
			events: partitionToEvent{pa1: []streamingccl.Event{
				streamingccl.MakeKVEvent(sampleKV),
			}, pa2: []streamingccl.Event{
				streamingccl.MakeKVEvent(sampleKV),
			}},
		},
		{
			// No progress should be reported to the job since partition 2 has not
			// emitted a resolved ts.
			name: "no-checkpoint-from-one-partition",
			events: partitionToEvent{pa1: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 1}),
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 4}),
			}, pa2: []streamingccl.Event{}},
		},
		{
			name: "one-partition-ahead-of-the-other",
			events: partitionToEvent{pa1: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 1}),
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 4}),
			}, pa2: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 1}),
			}},
			expectedFrontierTimestamp: hlc.Timestamp{WallTime: 1},
		},
		{
			name: "some-interleaved-timestamps",
			events: partitionToEvent{pa1: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 2}),
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 4}),
			}, pa2: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 3}),
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 5}),
			}},
			expectedFrontierTimestamp: hlc.Timestamp{WallTime: 4},
		},
		{
			name: "some-interleaved-logical-timestamps",
			events: partitionToEvent{pa1: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 1, Logical: 2}),
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 1, Logical: 4}),
			}, pa2: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 1, Logical: 1}),
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 2}),
			}},
			expectedFrontierTimestamp: hlc.Timestamp{WallTime: 1, Logical: 4},
		},
		{
			// The frontier should error out as it receives a checkpoint with a ts
			// lower than its start time.
			name: "checkpoint-lower-than-start-ts",
			events: partitionToEvent{pa1: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 1, Logical: 4}),
			}, pa2: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 1, Logical: 2}),
			}},
			frontierStartTime: hlc.Timestamp{WallTime: 1, Logical: 3},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec.PartitionAddresses = []string{pa1, pa2}
			spec.PartitionIds = []string{pa1, pa2}
			spec.PartitionSpecs = []string{pa1, pa2}
			proc, err := newStreamIngestionDataProcessor(&flowCtx, 0 /* processorID */, spec, &post, out)
			require.NoError(t, err)
			sip, ok := proc.(*streamIngestionProcessor)
			if !ok {
				t.Fatal("expected the processor that's created to be a stream ingestion processor")
			}

			// Inject a mock client with the events being tested against.
			sip.forceClientForTests = &mockStreamClient{
				partitionEvents: tc.events,
			}
			defer func() {
				require.NoError(t, sip.forceClientForTests.Close())
			}()

			// Create a frontier processor.
			var frontierSpec execinfrapb.StreamIngestionFrontierSpec
			pa1Key := roachpb.Key(pa1)
			pa2Key := roachpb.Key(pa2)
			frontierSpec.StreamAddress = spec.StreamAddress
			frontierSpec.TrackedSpans = []roachpb.Span{{Key: pa1Key, EndKey: pa1Key.Next()}, {Key: pa2Key,
				EndKey: pa2Key.Next()}}

			if !tc.frontierStartTime.IsEmpty() {
				frontierSpec.HighWaterAtStart = tc.frontierStartTime
			}

			// Create a mock ingestion job.
			record := jobs.Record{
				Description: "fake ingestion job",
				Username:    security.TestUserName(),
				Details:     jobspb.StreamIngestionDetails{StreamAddress: "foo"},
				// We don't use this so it does not matter what we set it too, as long
				// as it is non-nil.
				Progress: jobspb.ImportProgress{},
			}
			record.CreatedBy = &jobs.CreatedByInfo{
				Name: "ingestion",
			}

			frontierPost := execinfrapb.PostProcessSpec{}
			frontierOut := distsqlutils.RowBuffer{}
			frontierProc, err := newStreamIngestionFrontierProcessor(&flowCtx, 0, /* processorID*/
				frontierSpec, sip, &frontierPost, &frontierOut)
			require.NoError(t, err)
			fp, ok := frontierProc.(*streamIngestionFrontier)
			if !ok {
				t.Fatal("expected the processor that's created to be a stream ingestion frontier")
			}
			ctxWithCancel, cancel := context.WithCancel(ctx)
			defer cancel()
			fp.Run(ctxWithCancel)

			if !frontierOut.ProducerClosed() {
				t.Fatal("producer for StreamFrontierProcessor not closed")
			}

			var prevTimestamp hlc.Timestamp
			for {
				row, meta := frontierOut.Next()
				if meta != nil {
					if !tc.frontierStartTime.IsEmpty() {
						require.True(t, testutils.IsError(meta.Err, fmt.Sprintf("got a resolved timestamp ."+
							"* that is less than the frontier processor start time %s",
							tc.frontierStartTime.String())))
						return
					}
					t.Fatalf("unexpected meta record returned by frontier processor: %+v\n", *meta)
				}
				t.Logf("WOAH, row: %v", row)
				if row == nil {
					break
				}
				datum := row[0].Datum
				protoBytes, ok := datum.(*tree.DBytes)
				require.True(t, ok)

				var ingestedTimestamp hlc.Timestamp
				require.NoError(t, protoutil.Unmarshal([]byte(*protoBytes), &ingestedTimestamp))
				// Ensure that the rows emitted by the frontier never regress the ts.
				if !prevTimestamp.IsEmpty() {
					require.True(t, prevTimestamp.Less(ingestedTimestamp))
				}
				prevTimestamp = ingestedTimestamp
			}
			// Check the final ts recorded by the frontier.
			require.Equal(t, tc.expectedFrontierTimestamp, prevTimestamp)
		})
	}
}
