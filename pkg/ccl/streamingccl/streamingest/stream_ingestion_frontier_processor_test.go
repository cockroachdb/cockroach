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
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
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
	evalCtx := eval.MakeTestingEvalContext(st)

	testDiskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer testDiskMonitor.Stop(ctx)

	registry := tc.Server(0).JobRegistry().(*jobs.Registry)
	flowCtx := execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			Settings:          st,
			DB:                kvDB,
			JobRegistry:       registry,
			BulkSenderLimiter: limit.MakeConcurrentRequestLimiter("test", math.MaxInt),
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

	pa1StartKey := roachpb.Key("key_1")
	pa1Span := roachpb.Span{Key: pa1StartKey, EndKey: pa1StartKey.Next()}
	pa2StartKey := roachpb.Key("key_2")
	pa2Span := roachpb.Span{Key: pa2StartKey, EndKey: pa2StartKey.Next()}

	v := roachpb.MakeValueFromString("value_1")
	v.Timestamp = hlc.Timestamp{WallTime: 1}

	const tenantID = 20
	sampleKV := func() roachpb.KeyValue {
		key, err := keys.RewriteKeyToTenantPrefix(roachpb.Key("key_1"),
			keys.MakeTenantPrefix(roachpb.MakeTenantID(tenantID)))
		require.NoError(t, err)
		return roachpb.KeyValue{Key: key, Value: v}
	}
	sampleCheckpoint := func(span roachpb.Span, ts int64) []jobspb.ResolvedSpan {
		return []jobspb.ResolvedSpan{{Span: span, Timestamp: hlc.Timestamp{WallTime: ts}}}
	}
	sampleCheckpointWithLogicTS := func(span roachpb.Span, ts int64, logicalTs int32) []jobspb.ResolvedSpan {
		return []jobspb.ResolvedSpan{{Span: span, Timestamp: hlc.Timestamp{WallTime: ts, Logical: logicalTs}}}
	}

	for _, tc := range []struct {
		name                      string
		events                    partitionToEvent
		expectedFrontierTimestamp hlc.Timestamp
		frontierStartTime         hlc.Timestamp
		jobCheckpoint             []jobspb.ResolvedSpan
	}{
		{
			name: "same-resolved-ts-across-partitions",
			events: partitionToEvent{pa1: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(pa1Span, 1)),
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(pa1Span, 4)),
			}, pa2: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(pa2Span, 1)),
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(pa2Span, 4)),
			}},
			expectedFrontierTimestamp: hlc.Timestamp{WallTime: 4},
		},
		{
			// No progress should be reported to the job since partition 2 has not
			// emitted a resolved ts.
			name: "no-partition-checkpoints",
			events: partitionToEvent{pa1: []streamingccl.Event{
				streamingccl.MakeKVEvent(sampleKV()),
			}, pa2: []streamingccl.Event{
				streamingccl.MakeKVEvent(sampleKV()),
			}},
		},
		{
			// No progress should be reported to the job since partition 2 has not
			// emitted a resolved ts.
			name: "no-checkpoint-from-one-partition",
			events: partitionToEvent{pa1: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(pa1Span, 1)),
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(pa1Span, 4)),
			}, pa2: []streamingccl.Event{}},
		},
		{
			name: "one-partition-ahead-of-the-other",
			events: partitionToEvent{pa1: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(pa1Span, 1)),
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(pa1Span, 4)),
			}, pa2: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(pa2Span, 1)),
			}},
			expectedFrontierTimestamp: hlc.Timestamp{WallTime: 1},
		},
		{
			name: "some-interleaved-timestamps",
			events: partitionToEvent{pa1: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(pa1Span, 2)),
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(pa1Span, 4)),
			}, pa2: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(pa2Span, 3)),
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(pa2Span, 5)),
			}},
			expectedFrontierTimestamp: hlc.Timestamp{WallTime: 4},
		},
		{
			name: "some-interleaved-logical-timestamps",
			events: partitionToEvent{pa1: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(sampleCheckpointWithLogicTS(pa1Span, 1, 2)),
				streamingccl.MakeCheckpointEvent(sampleCheckpointWithLogicTS(pa1Span, 1, 4)),
			}, pa2: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(sampleCheckpointWithLogicTS(pa2Span, 1, 1)),
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(pa2Span, 2)),
			}},
			expectedFrontierTimestamp: hlc.Timestamp{WallTime: 1, Logical: 4},
		},
		{
			// The frontier should error out as it receives a checkpoint with a ts
			// lower than its start time.
			name: "partition-checkpoint-lower-than-start-ts",
			events: partitionToEvent{pa1: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(sampleCheckpointWithLogicTS(pa1Span, 1, 4)),
			}, pa2: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(sampleCheckpointWithLogicTS(pa2Span, 1, 2)),
			}},
			frontierStartTime: hlc.Timestamp{WallTime: 1, Logical: 3},
		},
		{
			name: "existing-job-checkpoint",
			events: partitionToEvent{pa1: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(pa1Span, 5)),
			}, pa2: []streamingccl.Event{
				streamingccl.MakeCheckpointEvent(sampleCheckpoint(pa2Span, 2)),
			}},
			frontierStartTime: hlc.Timestamp{WallTime: 1},
			jobCheckpoint: []jobspb.ResolvedSpan{
				{Span: pa1Span, Timestamp: hlc.Timestamp{WallTime: 4}},
				{Span: pa2Span, Timestamp: hlc.Timestamp{WallTime: 3}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name != "existing-job-checkpoint" {
				return
			}
			spec.PartitionSpecs = map[string]execinfrapb.StreamIngestionPartitionSpec{
				pa1: {
					PartitionID:       pa1,
					SubscriptionToken: pa1,
					Address:           pa1,
					Spans:             []roachpb.Span{pa1Span},
				},
				pa2: {
					PartitionID:       pa2,
					SubscriptionToken: pa2,
					Address:           pa2,
					Spans:             []roachpb.Span{pa2Span},
				},
			}
			spec.TenantRekey = execinfrapb.TenantRekey{
				OldID: roachpb.MakeTenantID(tenantID),
				NewID: roachpb.MakeTenantID(tenantID + 10),
			}
			spec.Checkpoint.ResolvedSpans = tc.jobCheckpoint
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
				require.NoError(t, sip.forceClientForTests.Close(ctx))
			}()

			// Create a frontier processor.
			var frontierSpec execinfrapb.StreamIngestionFrontierSpec
			frontierSpec.StreamAddress = spec.StreamAddress
			frontierSpec.TrackedSpans = []roachpb.Span{pa1Span, pa2Span}
			frontierSpec.Checkpoint.ResolvedSpans = tc.jobCheckpoint

			if !tc.frontierStartTime.IsEmpty() {
				frontierSpec.HighWaterAtStart = tc.frontierStartTime
			}

			// Create a mock ingestion job.
			record := jobs.Record{
				Description: "fake ingestion job",
				Username:    username.TestUserName(),
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

			minCheckpointTs := hlc.Timestamp{}
			for _, resolvedSpan := range tc.jobCheckpoint {
				if minCheckpointTs.IsEmpty() || resolvedSpan.Timestamp.Less(minCheckpointTs) {
					minCheckpointTs = resolvedSpan.Timestamp
				}

				// Ensure that the frontier is at least at the checkpoint for this span
				require.True(t, resolvedSpan.Timestamp.LessEq(frontierForSpans(fp.frontier, resolvedSpan.Span)))
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
				// Ensure that the rows emitted by the frontier never regress the ts or
				// appear prior to a checkpoint.
				if !prevTimestamp.IsEmpty() {
					require.True(t, prevTimestamp.Less(ingestedTimestamp))
					require.True(t, minCheckpointTs.Less(prevTimestamp))
				}
				t.Logf("Setting prev timestamp to (%+v)", ingestedTimestamp)
				prevTimestamp = ingestedTimestamp
			}
			// Check the final ts recorded by the frontier.
			require.Equal(t, tc.expectedFrontierTimestamp, prevTimestamp)
		})
	}
}
