// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingccl

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// mockStreamClient will always return the given slice of events when consuming
// a stream partition.
type mockStreamClient struct {
	partitionEvents []streamclient.Event
}

var _ streamclient.StreamClient = &mockStreamClient{}

// GetTopology implements the StreamClient interface.
func (m *mockStreamClient) GetTopology(
	_ streamclient.StreamAddress,
) (streamclient.Topology, error) {
	panic("unimplemented mock method")
}

// ConsumePartition implements the StreamClient interface.
func (m *mockStreamClient) ConsumePartition(
	_ streamclient.PartitionAddress, _ time.Time,
) (chan streamclient.Event, error) {
	eventCh := make(chan streamclient.Event, len(m.partitionEvents))

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
	spec.PartitionAddress = []streamclient.PartitionAddress{"s3://my_streams/stream/partition1", "s3://my_streams/stream/partition2"}
	proc, err := newStreamIngestionDataProcessor(&flowCtx, 0 /* processorID */, spec, &post, out)
	require.NoError(t, err)
	sip, ok := proc.(*streamIngestionProcessor)
	if !ok {
		t.Fatal("expected the processor that's created to be a split and scatter processor")
	}

	v := roachpb.MakeValueFromString("value_1")
	v.Timestamp = hlc.Timestamp{WallTime: 1}
	sampleKV := roachpb.KeyValue{Key: roachpb.Key("key_1"), Value: v}
	// Inject a mock client.
	sip.client = &mockStreamClient{
		partitionEvents: []streamclient.Event{
			streamclient.MakeKVEvent(sampleKV),
			streamclient.MakeKVEvent(sampleKV),
			streamclient.MakeCheckpointEvent(timeutil.FromUnixMicros(1)),
			streamclient.MakeKVEvent(sampleKV),
			streamclient.MakeKVEvent(sampleKV),
			streamclient.MakeCheckpointEvent(timeutil.FromUnixMicros(4)),
		},
	}

	sip.Run(context.Background())
	wg.Wait()

	// Ensure that all the outputs are properly closed.
	if !out.ProducerClosed() {
		t.Fatalf("output RowReceiver not closed")
	}

	// Check that the rows are distributed the way that we expect.
	expectedRows := map[string]struct{}{
		"['s3://my_streams/stream/partition1' '00:00:00.000001+00:00:00']": {},
		"['s3://my_streams/stream/partition1' '00:00:00.000004+00:00:00']": {},
		"['s3://my_streams/stream/partition2' '00:00:00.000001+00:00:00']": {},
		"['s3://my_streams/stream/partition2' '00:00:00.000004+00:00:00']": {},
	}
	actualRows := make(map[string]struct{}, 0)
	for {
		row := out.NextNoMeta(t)
		if row == nil {
			break
		}
		actualRows[row.String(streamIngestionResultTypes)] = struct{}{}
	}

	require.Equal(t, expectedRows, actualRows)
}
