// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"google.golang.org/grpc"
)

func TestTransportMoveToFront(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rd1 := roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 1, ReplicaID: 1}
	rd2 := roachpb.ReplicaDescriptor{NodeID: 2, StoreID: 2, ReplicaID: 2}
	rd3 := roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3, ReplicaID: 3}
	gt := grpcTransport{replicas: []roachpb.ReplicaDescriptor{rd1, rd2, rd3}}

	verifyOrder := func(replicas []roachpb.ReplicaDescriptor) {
		file, line, _ := caller.Lookup(1)
		for i, r := range gt.replicas {
			if r != replicas[i] {
				t.Fatalf("%s:%d: expected order %+v; got mismatch at index %d: %+v",
					file, line, replicas, i, r)
			}
		}
	}

	verifyOrder([]roachpb.ReplicaDescriptor{rd1, rd2, rd3})

	// Move replica 2 to the front.
	gt.MoveToFront(rd2)
	verifyOrder([]roachpb.ReplicaDescriptor{rd2, rd1, rd3})

	// Now replica 3.
	gt.MoveToFront(rd3)
	verifyOrder([]roachpb.ReplicaDescriptor{rd3, rd1, rd2})

	// Advance the client index and move replica 3 back to front.
	gt.nextReplicaIdx++
	gt.MoveToFront(rd3)
	verifyOrder([]roachpb.ReplicaDescriptor{rd3, rd1, rd2})
	if gt.nextReplicaIdx != 0 {
		t.Fatalf("expected client index 0; got %d", gt.nextReplicaIdx)
	}

	// Advance the client index again and verify replica 3 can
	// be moved to front for a second retry.
	gt.nextReplicaIdx++
	gt.MoveToFront(rd3)
	verifyOrder([]roachpb.ReplicaDescriptor{rd3, rd1, rd2})
	if gt.nextReplicaIdx != 0 {
		t.Fatalf("expected client index 0; got %d", gt.nextReplicaIdx)
	}

	// Move replica 2 to the front.
	gt.MoveToFront(rd2)
	verifyOrder([]roachpb.ReplicaDescriptor{rd2, rd1, rd3})

	// Advance client index and move rd1 front; should be no change.
	gt.nextReplicaIdx++
	gt.MoveToFront(rd1)
	verifyOrder([]roachpb.ReplicaDescriptor{rd2, rd1, rd3})

	// Advance client index and and move rd1 to front. Should move
	// client index back for a retry.
	gt.nextReplicaIdx++
	gt.MoveToFront(rd1)
	verifyOrder([]roachpb.ReplicaDescriptor{rd2, rd1, rd3})
	if gt.nextReplicaIdx != 1 {
		t.Fatalf("expected client index 1; got %d", gt.nextReplicaIdx)
	}

	// Advance client index once more; verify second retry.
	gt.nextReplicaIdx++
	gt.MoveToFront(rd2)
	verifyOrder([]roachpb.ReplicaDescriptor{rd1, rd2, rd3})
	if gt.nextReplicaIdx != 1 {
		t.Fatalf("expected client index 1; got %d", gt.nextReplicaIdx)
	}
}

// TestSpanImport tests that the gRPC transport ingests trace information that
// came from gRPC responses (via tracingpb.RecordedSpan on the batch responses).
func TestSpanImport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	metrics := makeDistSenderMetrics()
	gt := grpcTransport{
		opts: SendOptions{
			metrics: &metrics,
		},
	}
	server := mockInternalClient{}
	// Let's spice things up and simulate an error from the server.
	expectedErr := "my expected error"
	server.pErr = roachpb.NewErrorf(expectedErr /* nolint:fmtsafe */)

	recCtx, getRec, cancel := tracing.ContextWithRecordingSpan(
		ctx, tracing.NewTracer(), "test")
	defer cancel()

	server.tr = tracing.SpanFromContext(recCtx).Tracer()

	br, err := gt.sendBatch(recCtx, roachpb.NodeID(1), &server, roachpb.BatchRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !testutils.IsPError(br.Error, expectedErr) {
		t.Fatalf("expected err: %s, got: %q", expectedErr, br.Error)
	}
	expectedMsg := "mockInternalClient processing batch"
	if tracing.FindMsgInRecording(getRec(), expectedMsg) == -1 {
		t.Fatalf("didn't find expected message in trace: %s", expectedMsg)
	}
}

// mockInternalClient is an implementation of roachpb.InternalClient.
// It simulates aspects of how the Node normally handles tracing in gRPC calls.
type mockInternalClient struct {
	tr   *tracing.Tracer
	pErr *roachpb.Error
}

var _ roachpb.InternalClient = &mockInternalClient{}

func (*mockInternalClient) ResetQuorum(
	context.Context, *roachpb.ResetQuorumRequest, ...grpc.CallOption,
) (*roachpb.ResetQuorumResponse, error) {
	panic("unimplemented")
}

// Batch is part of the roachpb.InternalClient interface.
func (m *mockInternalClient) Batch(
	ctx context.Context, in *roachpb.BatchRequest, opts ...grpc.CallOption,
) (*roachpb.BatchResponse, error) {
	sp := m.tr.StartSpan("mock", tracing.WithForceRealSpan())
	defer sp.Finish()
	sp.SetVerbose(true)
	ctx = tracing.ContextWithSpan(ctx, sp)

	log.Eventf(ctx, "mockInternalClient processing batch")
	br := &roachpb.BatchResponse{}
	br.Error = m.pErr
	if rec := sp.GetRecording(); rec != nil {
		br.CollectedSpans = append(br.CollectedSpans, rec...)
	}
	return br, nil
}

// RangeLookup implements the roachpb.InternalClient interface.
func (m *mockInternalClient) RangeLookup(
	ctx context.Context, rl *roachpb.RangeLookupRequest, _ ...grpc.CallOption,
) (*roachpb.RangeLookupResponse, error) {
	return nil, fmt.Errorf("unsupported RangeLookup call")
}

// RangeFeed is part of the roachpb.InternalClient interface.
func (m *mockInternalClient) RangeFeed(
	ctx context.Context, in *roachpb.RangeFeedRequest, opts ...grpc.CallOption,
) (roachpb.Internal_RangeFeedClient, error) {
	return nil, fmt.Errorf("unsupported RangeFeed call")
}

// GossipSubscription is part of the roachpb.InternalClient interface.
func (m *mockInternalClient) GossipSubscription(
	ctx context.Context, args *roachpb.GossipSubscriptionRequest, _ ...grpc.CallOption,
) (roachpb.Internal_GossipSubscriptionClient, error) {
	return nil, fmt.Errorf("unsupported GossipSubscripion call")
}

func (m *mockInternalClient) Join(
	context.Context, *roachpb.JoinNodeRequest, ...grpc.CallOption,
) (*roachpb.JoinNodeResponse, error) {
	return nil, fmt.Errorf("unsupported Join call")
}
