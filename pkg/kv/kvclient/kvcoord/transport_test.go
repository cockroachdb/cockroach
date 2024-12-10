// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestTransportMoveToFront(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rd1 := roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 1, ReplicaID: 1}
	rd2 := roachpb.ReplicaDescriptor{NodeID: 2, StoreID: 2, ReplicaID: 2}
	rd3 := roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3, ReplicaID: 3}
	rd3Incoming := roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3, ReplicaID: 3,
		Type: roachpb.VOTER_INCOMING}
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

	// Move rd3 to front, even if the replica type differs.
	gt.MoveToFront(rd3Incoming)
	verifyOrder([]roachpb.ReplicaDescriptor{rd1, rd3, rd2})
	require.Equal(t, 1, gt.nextReplicaIdx)
}

func TestTransportReset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rd1 := roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 1, ReplicaID: 1}
	rd2 := roachpb.ReplicaDescriptor{NodeID: 2, StoreID: 2, ReplicaID: 2}
	rd3 := roachpb.ReplicaDescriptor{NodeID: 3, StoreID: 3, ReplicaID: 3}
	gt := grpcTransport{replicas: []roachpb.ReplicaDescriptor{rd1, rd2, rd3}}

	// Reset should be a noop when positioned at start.
	require.Equal(t, rd1, gt.NextReplica())
	gt.Reset()
	require.Equal(t, rd1, gt.NextReplica())

	// Reset should move back to front when in the middle.
	gt.SkipReplica()
	require.Equal(t, rd2, gt.NextReplica())
	gt.Reset()
	require.Equal(t, rd1, gt.NextReplica())

	// Reset should move back to front when exhausted.
	gt.SkipReplica()
	gt.SkipReplica()
	gt.SkipReplica()
	require.True(t, gt.IsExhausted())
	gt.Reset()
	require.False(t, gt.IsExhausted())
	require.Equal(t, rd1, gt.NextReplica())

	// MoveToFront will reorder replicas by moving the replica to the next index.
	// Reset moves to the start of the modified ordering.
	gt.SkipReplica()
	gt.SkipReplica()
	require.True(t, gt.MoveToFront(rd1))
	gt.Reset()
	require.Equal(t, rd2, gt.NextReplica())
	gt.SkipReplica()
	require.Equal(t, rd1, gt.NextReplica())
	gt.SkipReplica()
	require.Equal(t, rd3, gt.NextReplica())
	gt.SkipReplica()
	require.True(t, gt.IsExhausted())
}

// TestSpanImport tests that the gRPC transport ingests trace information that
// came from gRPC responses (via tracingpb.RecordedSpan on the batch responses).
func TestSpanImport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	metrics := MakeDistSenderMetrics(roachpb.Locality{})
	gt := grpcTransport{
		opts: SendOptions{
			metrics: &metrics,
		},
	}
	server := mockInternalClient{}
	// Let's spice things up and simulate an error from the server.
	expectedErr := "my expected error"
	server.pErr = kvpb.NewErrorf(expectedErr /* nolint:fmtsafe */)

	recCtx, getRecAndFinish := tracing.ContextWithRecordingSpan(
		ctx, tracing.NewTracer(), "test")
	defer getRecAndFinish()

	server.tr = tracing.SpanFromContext(recCtx).Tracer()

	br, err := gt.sendBatch(recCtx, roachpb.NodeID(1), &server, &kvpb.BatchRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !testutils.IsPError(br.Error, expectedErr) {
		t.Fatalf("expected err: %s, got: %q", expectedErr, br.Error)
	}
	expectedMsg := "mockInternalClient processing batch"
	if tracing.FindMsgInRecording(getRecAndFinish(), expectedMsg) == -1 {
		t.Fatalf("didn't find expected message in trace: %s", expectedMsg)
	}
}

func TestResponseVerifyFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	metrics := MakeDistSenderMetrics(roachpb.Locality{})
	gt := grpcTransport{
		opts: SendOptions{
			metrics: &metrics,
		},
	}

	ba := &kvpb.BatchRequest{}
	req := kvpb.NewScan(roachpb.KeyMin, roachpb.KeyMax)
	ba.Add(req)
	br := ba.CreateReply()
	resp := br.Responses[0].GetInner().(*kvpb.ScanResponse)
	val := roachpb.MakeValueFromString("hi")
	val.InitChecksum(roachpb.Key("not the right key"))
	resp.Rows = append(resp.Rows, roachpb.KeyValue{
		Key:   roachpb.Key("x"),
		Value: val,
	})
	require.Error(t, resp.Verify(req)) // we set this up to fail

	server := mockInternalClient{
		br: br,
	}

	_, err := gt.sendBatch(ctx, roachpb.NodeID(1), &server, ba)
	require.ErrorContains(t, err, "invalid checksum")
}

// mockInternalClient is an implementation of kvpb.InternalClient.
// It simulates aspects of how the Node normally handles tracing in gRPC calls.
type mockInternalClient struct {
	tr   *tracing.Tracer
	br   *kvpb.BatchResponse
	pErr *kvpb.Error
}

var _ kvpb.InternalClient = &mockInternalClient{}

func (*mockInternalClient) ResetQuorum(
	context.Context, *kvpb.ResetQuorumRequest, ...grpc.CallOption,
) (*kvpb.ResetQuorumResponse, error) {
	panic("unimplemented")
}

// Batch is part of the kvpb.InternalClient interface.
func (m *mockInternalClient) Batch(
	ctx context.Context, in *kvpb.BatchRequest, opts ...grpc.CallOption,
) (*kvpb.BatchResponse, error) {
	var sp *tracing.Span
	if m.tr != nil {
		sp = m.tr.StartSpan("mock", tracing.WithRecording(tracingpb.RecordingVerbose))
		defer sp.Finish()
		ctx = tracing.ContextWithSpan(ctx, sp)
	}

	log.Eventf(ctx, "mockInternalClient processing batch")
	br := m.br
	if br == nil {
		br = &kvpb.BatchResponse{}
	}
	br.Error = m.pErr

	if sp != nil {
		if rec := sp.GetConfiguredRecording(); rec != nil {
			br.CollectedSpans = append(br.CollectedSpans, rec...)
		}
	}
	return br, nil
}

func (m *mockInternalClient) BatchStream(
	ctx context.Context, opts ...grpc.CallOption,
) (kvpb.Internal_BatchStreamClient, error) {
	return nil, fmt.Errorf("unsupported BatchStream call")
}

// RangeLookup implements the kvpb.InternalClient interface.
func (m *mockInternalClient) RangeLookup(
	ctx context.Context, rl *kvpb.RangeLookupRequest, _ ...grpc.CallOption,
) (*kvpb.RangeLookupResponse, error) {
	return nil, fmt.Errorf("unsupported RangeLookup call")
}

func (m *mockInternalClient) MuxRangeFeed(
	ctx context.Context, opts ...grpc.CallOption,
) (kvpb.Internal_MuxRangeFeedClient, error) {
	return nil, fmt.Errorf("unsupported MuxRangeFeed call")
}

// GossipSubscription is part of the kvpb.InternalClient interface.
func (m *mockInternalClient) GossipSubscription(
	ctx context.Context, args *kvpb.GossipSubscriptionRequest, _ ...grpc.CallOption,
) (kvpb.Internal_GossipSubscriptionClient, error) {
	return nil, fmt.Errorf("unsupported GossipSubscripion call")
}

func (m *mockInternalClient) Join(
	context.Context, *kvpb.JoinNodeRequest, ...grpc.CallOption,
) (*kvpb.JoinNodeResponse, error) {
	return nil, fmt.Errorf("unsupported Join call")
}

func (m *mockInternalClient) TokenBucket(
	ctx context.Context, in *kvpb.TokenBucketRequest, _ ...grpc.CallOption,
) (*kvpb.TokenBucketResponse, error) {
	return nil, fmt.Errorf("unsupported TokenBucket call")
}

func (m *mockInternalClient) GetSpanConfigs(
	_ context.Context, _ *roachpb.GetSpanConfigsRequest, _ ...grpc.CallOption,
) (*roachpb.GetSpanConfigsResponse, error) {
	return nil, fmt.Errorf("unsupported GetSpanConfigs call")
}

func (m *mockInternalClient) SpanConfigConformance(
	_ context.Context, _ *roachpb.SpanConfigConformanceRequest, _ ...grpc.CallOption,
) (*roachpb.SpanConfigConformanceResponse, error) {
	return nil, fmt.Errorf("unsupported SpanConfigConformance call")
}

func (m *mockInternalClient) GetAllSystemSpanConfigsThatApply(
	context.Context, *roachpb.GetAllSystemSpanConfigsThatApplyRequest, ...grpc.CallOption,
) (*roachpb.GetAllSystemSpanConfigsThatApplyResponse, error) {
	return nil, fmt.Errorf("unsupported GetAllSystemSpanConfigsThatApply call")
}

func (m *mockInternalClient) UpdateSpanConfigs(
	_ context.Context, _ *roachpb.UpdateSpanConfigsRequest, _ ...grpc.CallOption,
) (*roachpb.UpdateSpanConfigsResponse, error) {
	return nil, fmt.Errorf("unsupported UpdateSpanConfigs call")
}

func (m *mockInternalClient) TenantSettings(
	context.Context, *kvpb.TenantSettingsRequest, ...grpc.CallOption,
) (kvpb.Internal_TenantSettingsClient, error) {
	return nil, fmt.Errorf("unsupported TenantSettings call")
}

func (n *mockInternalClient) GetRangeDescriptors(
	context.Context, *kvpb.GetRangeDescriptorsRequest, ...grpc.CallOption,
) (kvpb.Internal_GetRangeDescriptorsClient, error) {
	return nil, fmt.Errorf("unsupported GetRangeDescriptors call")
}
