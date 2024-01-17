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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

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
	metrics := makeDistSenderMetrics()
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

// RangeLookup implements the kvpb.InternalClient interface.
func (m *mockInternalClient) RangeLookup(
	ctx context.Context, rl *kvpb.RangeLookupRequest, _ ...grpc.CallOption,
) (*kvpb.RangeLookupResponse, error) {
	return nil, fmt.Errorf("unsupported RangeLookup call")
}

// RangeFeed is part of the kvpb.InternalClient interface.
func (m *mockInternalClient) RangeFeed(
	ctx context.Context, in *kvpb.RangeFeedRequest, opts ...grpc.CallOption,
) (kvpb.Internal_RangeFeedClient, error) {
	return nil, fmt.Errorf("unsupported RangeFeed call")
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
