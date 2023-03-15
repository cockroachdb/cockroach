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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type partitionToEvent map[string][]streamingccl.Event

func TestStreamIngestionFrontierProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.WithIssue(t, 87145, "flaky test")

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3 /* nodes */, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: &jobs.TestingKnobs{
					// We create a job record to track persistence but we don't want it to
					// be adopted as the processors are being manually executed in the test.
					DisableAdoptions: true,
				},
				// DisableAdoptions needs this.
				UpgradeManager: &upgradebase.TestingKnobs{
					DontUseJobs: true,
				},
			},
		},
	})
	defer tc.Stopper().Stop(context.Background())

	st := cluster.MakeTestingClusterSettings()
	JobCheckpointFrequency.Override(ctx, &st.SV, 200*time.Millisecond)
	streamingccl.StreamReplicationConsumerHeartbeatFrequency.Override(ctx, &st.SV, 100*time.Millisecond)

	evalCtx := eval.MakeTestingEvalContext(st)

	testDiskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer testDiskMonitor.Stop(ctx)

	registry := tc.Server(0).JobRegistry().(*jobs.Registry)
	flowCtx := execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			Settings:          st,
			DB:                tc.Server(0).InternalDB().(descs.DB),
			JobRegistry:       registry,
			BulkSenderLimiter: limit.MakeConcurrentRequestLimiter("test", math.MaxInt),
		},
		EvalCtx:     &evalCtx,
		Mon:         evalCtx.TestingMon,
		DiskMonitor: testDiskMonitor,
	}

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
			keys.MakeTenantPrefix(roachpb.MustMakeTenantID(tenantID)))
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
			frontierStartTime:         hlc.Timestamp{WallTime: 1, Logical: 3},
			expectedFrontierTimestamp: hlc.Timestamp{WallTime: 1, Logical: 3},
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
			expectedFrontierTimestamp: hlc.Timestamp{WallTime: 3},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topology := streamclient.Topology{
				Partitions: []streamclient.PartitionInfo{
					{
						ID:                pa1,
						SubscriptionToken: []byte(pa1),
						SrcAddr:           streamingccl.PartitionAddress(pa1),
						Spans:             []roachpb.Span{pa1Span},
					},
					{
						ID:                pa2,
						SubscriptionToken: []byte(pa2),
						SrcAddr:           streamingccl.PartitionAddress(pa2),
						Spans:             []roachpb.Span{pa2Span},
					},
				},
			}

			spec.PartitionSpecs = map[string]execinfrapb.StreamIngestionPartitionSpec{}
			for _, partition := range topology.Partitions {
				spec.PartitionSpecs[partition.ID] = execinfrapb.StreamIngestionPartitionSpec{
					PartitionID:       partition.ID,
					SubscriptionToken: string(partition.SubscriptionToken),
					Address:           string(partition.SrcAddr),
					Spans:             partition.Spans,
				}
			}
			spec.TenantRekey = execinfrapb.TenantRekey{
				OldID: roachpb.MustMakeTenantID(tenantID),
				NewID: roachpb.MustMakeTenantID(tenantID + 10),
			}
			spec.PreviousHighWaterTimestamp = tc.frontierStartTime
			if tc.frontierStartTime.IsEmpty() {
				spec.InitialScanTimestamp = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
			}
			spec.Checkpoint.ResolvedSpans = tc.jobCheckpoint
			proc, err := newStreamIngestionDataProcessor(ctx, &flowCtx, 0 /* processorID */, spec, &post)
			require.NoError(t, err)
			sip, ok := proc.(*streamIngestionProcessor)
			if !ok {
				t.Fatal("expected the processor that's created to be a stream ingestion processor")
			}

			// Inject a mock client with the events being tested against.
			doneCh := make(chan struct{})
			defer close(doneCh)
			sip.forceClientForTests = &mockStreamClient{
				partitionEvents: tc.events,
				doneCh:          doneCh,
			}
			defer func() {
				require.NoError(t, sip.forceClientForTests.Close(ctx))
			}()

			jobID := registry.MakeJobID()

			t.Logf("Using JobID: %v", jobID)

			// Create a frontier processor.
			var frontierSpec execinfrapb.StreamIngestionFrontierSpec
			frontierSpec.StreamAddresses = topology.StreamAddresses()
			frontierSpec.TrackedSpans = []roachpb.Span{pa1Span, pa2Span}
			frontierSpec.Checkpoint.ResolvedSpans = tc.jobCheckpoint
			frontierSpec.JobID = int64(jobID)

			if !tc.frontierStartTime.IsEmpty() {
				frontierSpec.HighWaterAtStart = tc.frontierStartTime
			}

			// Create a mock ingestion job.
			record := jobs.Record{
				JobID:       jobID,
				Description: "fake ingestion job",
				Username:    username.TestUserName(),
				Details:     jobspb.StreamIngestionDetails{StreamAddress: spec.StreamAddress},
				// We don't use this so it does not matter what we set it too, as long
				// as it is non-nil.
				Progress: jobspb.StreamIngestionProgress{},
			}
			record.CreatedBy = &jobs.CreatedByInfo{
				Name: "ingestion",
			}
			_, err = registry.CreateJobWithTxn(ctx, record, record.JobID, nil)
			require.NoError(t, err)

			frontierPost := execinfrapb.PostProcessSpec{}
			frontierOut := distsqlutils.RowBuffer{}
			frontierProc, err := newStreamIngestionFrontierProcessor(
				ctx, &flowCtx, 0 /* processorID*/, frontierSpec, sip, &frontierPost,
			)
			require.NoError(t, err)
			fp, ok := frontierProc.(*streamIngestionFrontier)
			if !ok {
				t.Fatal("expected the processor that's created to be a stream ingestion frontier")
			}
			ctxWithCancel, cancel := context.WithCancel(ctx)
			defer cancel()

			client := streamclient.GetRandomStreamClientSingletonForTesting()
			defer func() {
				require.NoError(t, client.Close(context.Background()))
			}()

			client.ClearInterceptors()

			// Record heartbeats in a list and terminate the client once the expected
			// frontier timestamp has been reached
			heartbeats := make([]hlc.Timestamp, 0)
			client.RegisterHeartbeatInterception(func(heartbeatTs hlc.Timestamp) {
				heartbeats = append(heartbeats, heartbeatTs)
				if tc.expectedFrontierTimestamp.LessEq(heartbeatTs) {
					doneCh <- struct{}{}
				}

				// Ensure we never heartbeat a value later than what is persisted
				job, err := registry.LoadJob(ctx, jobID)
				require.NoError(t, err)
				progress := job.Progress().Progress
				if progress == nil {
					if !heartbeatTs.Equal(spec.InitialScanTimestamp) {
						t.Fatalf("heartbeat %v should equal start time of %v", heartbeatTs, spec.InitialScanTimestamp)
					}
				} else {
					persistedHighwater := *progress.(*jobspb.Progress_HighWater).HighWater
					require.True(t, heartbeatTs.LessEq(persistedHighwater))
				}
			})

			fp.Run(ctxWithCancel, &frontierOut)

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

			// Wait until the frontier terminates
			_, meta := frontierOut.Next()
			if meta != nil {
				if !tc.frontierStartTime.IsEmpty() {
					require.True(t, testutils.IsError(meta.Err, fmt.Sprintf("got a resolved timestamp ."+
						"* that is less than the frontier processor start time %s",
						tc.frontierStartTime.String())))
					return
				}
				t.Fatalf("unexpected meta record returned by frontier processor: %+v\n", *meta)
			}

			// Ensure that the rows emitted by the frontier never regress the ts or
			// appear prior to a checkpoint.
			var prevTimestamp hlc.Timestamp
			for _, heartbeatTs := range heartbeats {
				if !prevTimestamp.IsEmpty() {
					require.True(t, prevTimestamp.LessEq(heartbeatTs))
					require.True(t, minCheckpointTs.LessEq(heartbeatTs))
				}
				prevTimestamp = heartbeatTs
			}

			// Check the final ts recorded by the frontier.
			require.Equal(t, tc.expectedFrontierTimestamp, prevTimestamp)
		})
	}
}
