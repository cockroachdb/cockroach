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
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// mockStreamClient will always return the given slice of events when consuming
// a stream partition.
type mockStreamClient struct {
	partitionEvents []streamingccl.Event
}

var _ streamclient.Client = &mockStreamClient{}

// GetTopology implements the StreamClient interface.
func (m *mockStreamClient) GetTopology(
	_ streamingccl.StreamAddress,
) (streamingccl.Topology, error) {
	panic("unimplemented mock method")
}

// ConsumePartition implements the StreamClient interface.
func (m *mockStreamClient) ConsumePartition(
	_ streamingccl.PartitionAddress, _ time.Time,
) (chan streamingccl.Event, error) {
	eventCh := make(chan streamingccl.Event, len(m.partitionEvents))

	for _, event := range m.partitionEvents {
		eventCh <- event
	}
	close(eventCh)

	return eventCh, nil
}

func TestStreamIngestionProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3 /* nodes */, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())
	kvDB := tc.Server(0).DB()

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

	var wg sync.WaitGroup
	out := &distsqlutils.RowBuffer{}
	post := execinfrapb.PostProcessSpec{}

	var spec execinfrapb.StreamIngestionDataSpec
	spec.PartitionAddresses = []streamingccl.PartitionAddress{"s3://my_streams/stream/partition1", "s3://my_streams/stream/partition2"}
	proc, err := newStreamIngestionDataProcessor(&flowCtx, 0 /* processorID */, spec, &post, out)
	require.NoError(t, err)
	sip, ok := proc.(*streamIngestionProcessor)
	if !ok {
		t.Fatal("expected the processor that's created to be a split and scatter processor")
	}

	// Inject a mock client.
	v := roachpb.MakeValueFromString("value_1")
	v.Timestamp = hlc.Timestamp{WallTime: 1}
	sampleKV := roachpb.KeyValue{Key: roachpb.Key("key_1"), Value: v}
	sip.client = &mockStreamClient{
		partitionEvents: []streamingccl.Event{
			streamingccl.MakeKVEvent(sampleKV),
			streamingccl.MakeKVEvent(sampleKV),
			streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 1}),
			streamingccl.MakeKVEvent(sampleKV),
			streamingccl.MakeKVEvent(sampleKV),
			streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 4}),
		},
	}

	sip.Run(context.Background())
	wg.Wait()

	// Ensure that all the outputs are properly closed.
	if !out.ProducerClosed() {
		t.Fatalf("output RowReceiver not closed")
	}

	// Compare the set of results since the ordering is not guaranteed.
	expectedRows := map[string]struct{}{
		"s3://my_streams/stream/partition1{-\\x00} 0.000000001,0": {},
		"s3://my_streams/stream/partition1{-\\x00} 0.000000004,0": {},
		"s3://my_streams/stream/partition2{-\\x00} 0.000000001,0": {},
		"s3://my_streams/stream/partition2{-\\x00} 0.000000004,0": {},
	}
	actualRows := make(map[string]struct{})
	for {
		row := out.NextNoMeta(t)
		if row == nil {
			break
		}
		datum := row[0].Datum
		protoBytes, ok := datum.(*tree.DBytes)
		require.True(t, ok)

		var resolvedSpan jobspb.ResolvedSpan
		require.NoError(t, protoutil.Unmarshal([]byte(*protoBytes), &resolvedSpan))

		actualRows[fmt.Sprintf("%s %s", resolvedSpan.Span, resolvedSpan.Timestamp)] = struct{}{}
	}

	require.Equal(t, expectedRows, actualRows)
}
