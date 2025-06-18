// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package interceptorutil

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/gogo/protobuf/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// BatchMethodName is the method name of Internal.Batch RPC.
const BatchMethodName = "/cockroach.roachpb.Internal/Batch"

// BatchStreamMethodName is the method name of the Internal.BatchStream RPC.
const BatchStreamMethodName = "/cockroach.roachpb.Internal/BatchStream"

// sendKVBatchMethodName is the method name for adminServer.SendKVBatch.
const sendKVBatchMethodName = "/cockroach.server.serverpb.Admin/SendKVBatch"

// SetupFlowMethodName is the method name of DistSQL.SetupFlow RPC.
const SetupFlowMethodName = "/cockroach.sql.distsqlrun.DistSQL/SetupFlow"
const flowStreamMethodName = "/cockroach.sql.distsqlrun.DistSQL/FlowStream"

// methodExcludedFromTracing returns true if a call to the given RPC method does
// not need to propagate tracing info. Some RPCs (Internal.Batch,
// DistSQL.SetupFlow) have dedicated fields for passing along the tracing
// context in the request, which is more efficient than letting the RPC
// interceptors deal with it. Others (DistSQL.FlowStream) are simply exempt from
// tracing because it's not worth it.
func MethodExcludedFromTracing(method string) bool {
	return method == BatchMethodName ||
		method == BatchStreamMethodName ||
		method == sendKVBatchMethodName ||
		method == SetupFlowMethodName ||
		method == flowStreamMethodName
}

func injectSpanMeta(
	ctx context.Context, tracer *tracing.Tracer, clientSpan *tracing.Span,
) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	} else {
		md = md.Copy()
	}
	tracer.InjectMetaInto(clientSpan.Meta(), tracing.MetadataCarrier{MD: md})
	return metadata.NewOutgoingContext(ctx, md)
}

// CreateClientSpan contains the common tracing logic shared between gRPC and DRPC client interceptors.
// It creates a span, optionally injects it into the metadata (if needed), and returns the updated context
// and span for use by the specific interceptor implementations.
func CreateClientSpan(
	ctx context.Context, method string, tracer *tracing.Tracer, init func(*tracing.Span),
) (context.Context, *tracing.Span, bool) {
	// Local RPCs don't need any special tracing, since the caller's context
	// will be used on the "server".
	_, localRequest := grpcutil.IsLocalRequestContext(ctx)
	if localRequest {
		return ctx, nil, true
	}
	parent := tracing.SpanFromContext(ctx)
	if !tracing.SpanInclusionFuncForClient(parent) {
		return ctx, nil, true
	}

	clientSpan := tracer.StartSpan(
		method,
		tracing.WithParent(parent),
		tracing.WithClientSpanKind,
	)
	if init != nil {
		init(clientSpan)
	}

	// For most RPCs we pass along tracing info as metadata. Some select
	// RPCs carry the tracing in the request protos, which is more efficient.
	if !MethodExcludedFromTracing(method) {
		ctx = injectSpanMeta(ctx, tracer, clientSpan)
	}

	return ctx, clientSpan, false
}

// CreateStreamClientSpan contains the common tracing logic shared between gRPC and DRPC
// streaming client interceptors. It creates a span for the stream, and optionally injects it into the
// metadata (if needed), then returns the updated context and span for use by the specific interceptor
// implementations.
func CreateStreamClientSpan(
	ctx context.Context, method string, tracer *tracing.Tracer, init func(*tracing.Span),
) (context.Context, *tracing.Span, bool) {
	// Local RPCs don't need any special tracing, since the caller's context
	// will be used on the "server".
	_, localRequest := grpcutil.IsLocalRequestContext(ctx)
	if localRequest {
		return ctx, nil, true
	}
	parent := tracing.SpanFromContext(ctx)
	if !tracing.SpanInclusionFuncForClient(parent) {
		return ctx, nil, true
	}

	// Create a span that will live for the life of the stream.
	clientSpan := tracer.StartSpan(
		method,
		tracing.WithParent(parent),
		tracing.WithClientSpanKind,
	)
	if init != nil {
		init(clientSpan)
	}

	if !MethodExcludedFromTracing(method) {
		ctx = injectSpanMeta(ctx, tracer, clientSpan)
	}

	return ctx, clientSpan, false
}

// SetGRPCErrorTag sets an error tag on the span.
// TODO update this function to work with drpc
func SetGRPCErrorTag(sp *tracing.Span, err error) {
	if err == nil {
		return
	}
	s, _ := status.FromError(err)
	sp.SetTag("response_code", attribute.IntValue(int(codes.Error)))
	sp.SetOtelStatus(codes.Error, s.Message())
}

// testStructuredImpl is a testing implementation of Structured event.
type TestStructuredImpl struct {
	*types.StringValue
}

func NewTestStructured(s string) *TestStructuredImpl {
	return &TestStructuredImpl{
		&types.StringValue{Value: s},
	}
}

func (t *TestStructuredImpl) String() string {
	return fmt.Sprintf("structured=%s", t.Value)
}
