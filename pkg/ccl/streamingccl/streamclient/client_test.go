// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamclient

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type testStreamClient struct{}

var _ Client = testStreamClient{}

// Create implements the Client interface.
func (sc testStreamClient) Create(ctx context.Context, target roachpb.TenantID) (StreamID, error) {
	return StreamID(1), nil
}

// Plan implements the Client interface.
func (sc testStreamClient) Plan(ctx context.Context, ID StreamID) (Topology, error) {
	return Topology([]PartitionInfo{
		{SrcAddr: streamingccl.PartitionAddress("test://host1")},
		{SrcAddr: streamingccl.PartitionAddress("test://host2")},
	}), nil
}

// Heartbeat implements the Client interface.
func (sc testStreamClient) Heartbeat(ctx context.Context, ID StreamID, _ hlc.Timestamp) error {
	return nil
}

// Subscribe implements the Client interface.
func (sc testStreamClient) Subscribe(
	ctx context.Context, stream StreamID, spec SubscriptionToken, checkpoint hlc.Timestamp,
) (chan streamingccl.Event, chan error, error) {
	sampleKV := roachpb.KeyValue{
		Key: []byte("key_1"),
		Value: roachpb.Value{
			RawBytes:  []byte("value_1"),
			Timestamp: hlc.Timestamp{WallTime: 1},
		},
	}

	events := make(chan streamingccl.Event, 2)
	events <- streamingccl.MakeKVEvent(sampleKV)
	events <- streamingccl.MakeCheckpointEvent(hlc.Timestamp{WallTime: 100})
	close(events)

	return events, nil, nil
}

// ExampleClientUsage serves as documentation to indicate how a stream
// client could be used.
func ExampleClient() {
	client := testStreamClient{}

	id, err := client.Create(context.Background(), roachpb.MakeTenantID(1))
	if err != nil {
		panic(err)
	}

	var ingested struct {
		ts hlc.Timestamp
		syncutil.Mutex
	}

	done := make(chan struct{})

	grp := ctxgroup.WithContext(context.Background())
	grp.GoCtx(func(ctx context.Context) error {
		ticker := time.NewTicker(time.Second * 30)
		for {
			select {
			case <-done:
				return nil
			case <-ticker.C:
				ingested.Lock()
				ts := ingested.ts
				ingested.Unlock()

				if err := client.Heartbeat(ctx, id, ts); err != nil {
					return err
				}
			}
		}
	})

	grp.GoCtx(func(ctx context.Context) error {
		defer close(done)
		ingested.Lock()
		ts := ingested.ts
		ingested.Unlock()

		topology, err := client.Plan(context.Background(), id)
		if err != nil {
			panic(err)
		}

		for _, partition := range topology {
			// TODO(dt): use Subscribe helper and partition.SrcAddr
			eventCh, _ /* errCh */, err := client.Subscribe(context.Background(), id, partition.SubscriptionToken, ts)
			if err != nil {
				panic(err)
			}

			// This example looks for the closing of the channel to terminate the test,
			// but an ingestion job should look for another event such as the user
			// cutting over to the new cluster to move to the next stage.
			for event := range eventCh {
				switch event.Type() {
				case streamingccl.KVEvent:
					kv := event.GetKV()
					fmt.Printf("%s->%s@%d\n", kv.Key.String(), string(kv.Value.RawBytes), kv.Value.Timestamp.WallTime)
				case streamingccl.CheckpointEvent:
					ingested.Lock()
					ingested.ts.Forward(*event.GetResolved())
					ingested.Unlock()
					fmt.Printf("resolved %d\n", event.GetResolved().WallTime)
				case streamingccl.GenerationEvent:
					fmt.Printf("received generation event")
				default:
					panic(fmt.Sprintf("unexpected event type %v", event.Type()))
				}
			}
		}
		return nil
	})

	if err := grp.Wait(); err != nil {
		panic(err)
	}

	// Output:
	// "key_1"->value_1@1
	// resolved 100
	// "key_1"->value_1@1
	// resolved 100
}
