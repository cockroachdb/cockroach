// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracingutil

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"google.golang.org/grpc/metadata"
	"storj.io/drpc/drpcmetadata"
)

// BatchMethodName is the method name of Internal.Batch RPC.
const BatchMethodName = "/cockroach.roachpb.Internal/Batch"

// BatchStreamMethodName is the method name of the Internal.BatchStream RPC.
const BatchStreamMethodName = "/cockroach.roachpb.Internal/BatchStream"

// sendKVBatchMethodName is the method name for adminServer.SendKVBatch.
const SendKVBatchMethodName = "/cockroach.server.serverpb.Admin/SendKVBatch"

// SetupFlowMethodName is the method name of DistSQL.SetupFlow RPC.
const SetupFlowMethodName = "/cockroach.sql.distsqlrun.DistSQL/SetupFlow"
const FlowStreamMethodName = "/cockroach.sql.distsqlrun.DistSQL/FlowStream"

// KVBatchMethodName is the method name of KVBatch.Batch RPC.
const KVBatchMethodName = "/cockroach.roachpb.KVBatch/Batch"

// KVBatchStreamMethodName is the method name of KVBatch.BatchStream RPC.
const KVBatchStreamMethodName = "/cockroach.roachpb.KVBatch/BatchStream"

// methodExcludedFromTracing returns true if a call to the given RPC method does
// not need to propagate tracing info. Some RPCs (Internal.Batch,
// DistSQL.SetupFlow) have dedicated fields for passing along the tracing
// context in the request, which is more efficient than letting the RPC
// interceptors deal with it. Others (DistSQL.FlowStream) are simply exempt from
// tracing because it's not worth it.
func MethodExcludedFromTracing(method string) bool {
	return method == BatchMethodName ||
		method == BatchStreamMethodName ||
		method == SendKVBatchMethodName ||
		method == SetupFlowMethodName ||
		method == FlowStreamMethodName ||
		method == KVBatchMethodName ||
		method == KVBatchStreamMethodName
}

// ShouldSkipClientTracing determines whether tracing should be skipped.
func ShouldSkipClientTracing(ctx context.Context) bool {
	// Local RPCs don't need any special tracing, since the caller's context
	// will be used on the "server".
	_, localRequest := grpcutil.IsLocalRequestContext(ctx)
	if localRequest {
		return true
	}
	parent := tracing.SpanFromContext(ctx)
	return !tracing.SpanInclusionFuncForClient(parent)
}

func InjectSpanMeta(
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

// InjectDRPCSpanMeta injects the span metadata into a DRPC context.
func InjectDRPCSpanMeta(
	ctx context.Context, tracer *tracing.Tracer, clientSpan *tracing.Span,
) context.Context {
	md, ok := drpcmetadata.Get(ctx)
	if ok {
		copied := make(map[string]string, len(md))
		for k, v := range md {
			copied[k] = v
		}
		md = copied
	} else {
		md = make(map[string]string)
	}
	tracer.InjectMetaInto(clientSpan.Meta(), tracing.DRPCMetadataCarrier{MD: md})
	return drpcmetadata.AddPairs(ctx, md)
}
