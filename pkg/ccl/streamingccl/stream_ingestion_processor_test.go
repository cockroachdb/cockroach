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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
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
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// mockStreamClient will return the slice of events associated to the stream
// partition being consumed. Stream partitions are identified by unique
// partition addresses.
type mockStreamClient struct {
	partitionEvents map[streamclient.PartitionAddress][]streamclient.Event
}

var _ streamclient.Client = &mockStreamClient{}

// GetTopology implements the StreamClient interface.
func (m *mockStreamClient) GetTopology(
	_ streamclient.StreamAddress,
) (streamclient.Topology, error) {
	panic("unimplemented mock method")
}

// ConsumePartition implements the StreamClient interface.
func (m *mockStreamClient) ConsumePartition(
	address streamclient.PartitionAddress, _ time.Time,
) (chan streamclient.Event, error) {
	var events []streamclient.Event
	var ok bool
	if events, ok = m.partitionEvents[address]; !ok {
		return nil, errors.Newf("No events found for paritition %s", address)
	}

	eventCh := make(chan streamclient.Event, len(events))

	for _, event := range events {
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
	pa1 := streamclient.PartitionAddress("s3://my_streams/stream/partition1")
	pa2 := streamclient.PartitionAddress("s3://my_streams/stream/partition2")
	spec.PartitionAddress = []streamclient.PartitionAddress{pa1, pa2}
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
	events := []streamclient.Event{
		streamclient.MakeKVEvent(sampleKV),
		streamclient.MakeKVEvent(sampleKV),
		streamclient.MakeCheckpointEvent(hlc.Timestamp{WallTime: 1}),
		streamclient.MakeKVEvent(sampleKV),
		streamclient.MakeKVEvent(sampleKV),
		streamclient.MakeCheckpointEvent(hlc.Timestamp{WallTime: 4}),
	}
	sip.client = &mockStreamClient{
		partitionEvents: map[streamclient.PartitionAddress][]streamclient.Event{pa1: events, pa2: events},
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
