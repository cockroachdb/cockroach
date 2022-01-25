// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/redact"
)

// executeBatchRPCInternalFn is a helper function which executes a KV
// batch request with the help of a given request dispatcher.
// Its main purpose is to set up tracing properly for the request,
// with proper handling for the two separate cases:
// - when the request is processed locally, there's no
//   remote tracing and we trace in the local context;
// - when the request is sent to another node, there's remote
//   tracing and a trace output to be propagated to the
//   local context.
//
// The dispatcher can be e.g. distsender, in the case of the
// SendKVBatch RPC where the request is to be processed "above" the
// txn logic, or the store directly, when the batch is executed
// "under" the txn logic.
func executeBatchRPCInternalFn(
	ctx context.Context,
	tenID roachpb.TenantID,
	args *roachpb.BatchRequest,
	stopper *stop.Stopper,
	tracer *tracing.Tracer,
	spanMethodName string,
	taskName string,
	processorName redact.SafeString,
	senderName redact.SafeString,
	sender kv.Sender,
	finishCall func(dur time.Duration, perr *roachpb.Error),
) (*roachpb.BatchResponse, error) {
	var br *roachpb.BatchResponse
	if err := stopper.RunTaskWithErr(ctx, taskName, func(ctx context.Context) error {
		var finishSpan func(context.Context, *roachpb.BatchResponse)
		// Shadow ctx from the outer function. Written like this to pass the linter.
		ctx, finishSpan = setupSpanForIncomingRPC(ctx, tenID, args, tracer, spanMethodName)
		// NB: wrapped to delay br evaluation to its value when returning.
		defer func() { finishSpan(ctx, br) }()
		if log.HasSpanOrEvent(ctx) {
			log.Eventf(ctx, "%s received request: %s", processorName, args.Summary())
		}

		tStart := timeutil.Now()
		var pErr *roachpb.Error
		br, pErr = sender.Send(ctx, *args)
		if pErr != nil {
			br = &roachpb.BatchResponse{}
			log.VErrEventf(ctx, 3, "error from %s.Send: %s", senderName, pErr)
		}
		if br.Error != nil {
			panic(roachpb.ErrorUnexpectedlySet(sender, br))
		}
		if finishCall != nil {
			finishCall(timeutil.Since(tStart), pErr)
		}
		br.Error = pErr
		return nil
	}); err != nil {
		return nil, err
	}
	return br, nil
}

// setupSpanForIncomingRPC takes a context and returns a derived context with a
// new span in it. Depending on the input context, that span might be a root
// span or a child span. If it is a child span, it might be a child span of a
// local or a remote span. Note that supporting both the "child of local span"
// and "child of remote span" cases are important, as this RPC can be called
// either through the network or directly if the caller is local.
//
// It returns the derived context and a cleanup function to be
// called when servicing the RPC is done. The cleanup function will
// close the span and serialize any data recorded to that span into
// the BatchResponse. The cleanup function takes the BatchResponse
// in which the response is to serialized. The BatchResponse can
// be nil in case no response is to be returned to the rpc caller.
func setupSpanForIncomingRPC(
	ctx context.Context,
	tenID roachpb.TenantID,
	ba *roachpb.BatchRequest,
	tr *tracing.Tracer,
	spanMethodName string,
) (context.Context, func(context.Context, *roachpb.BatchResponse)) {
	var newSpan *tracing.Span
	parentSpan := tracing.SpanFromContext(ctx)
	localRequest := grpcutil.IsLocalRequestContext(ctx)
	// For non-local requests, we'll need to attach the recording to the outgoing
	// BatchResponse if the request is traced. We ignore whether the request is
	// traced or not here; if it isn't, the recording will be empty.
	needRecordingCollection := !localRequest
	if localRequest {
		// This is a local request which circumvented gRPC. Start a span now.
		ctx, newSpan = tracing.EnsureChildSpan(ctx, tr, spanMethodName, tracing.WithServerSpanKind)
	} else if parentSpan == nil {
		var remoteParent tracing.SpanMeta
		if !ba.TraceInfo.Empty() {
			remoteParent = tracing.SpanMetaFromProto(ba.TraceInfo)
		} else {
			// For backwards compatibility with 21.2, if tracing info was passed as
			// gRPC metadata, we use it.
			var err error
			remoteParent, err = tracing.ExtractSpanMetaFromGRPCCtx(ctx, tr)
			if err != nil {
				log.Warningf(ctx, "error extracting tracing info from gRPC: %s", err)
			}
		}

		ctx, newSpan = tr.StartSpanCtx(ctx, spanMethodName,
			tracing.WithRemoteParent(remoteParent),
			tracing.WithServerSpanKind)
	} else {
		// It's unexpected to find a span in the context for a non-local request.
		// Let's create a span for the RPC anyway.
		ctx, newSpan = tr.StartSpanCtx(ctx, spanMethodName,
			tracing.WithParent(parentSpan),
			tracing.WithServerSpanKind)
	}

	finishSpan := func(ctx context.Context, br *roachpb.BatchResponse) {
		var rec tracing.Recording
		// If we don't have a response, there's nothing to attach a trace to.
		// Nothing more for us to do.
		needRecordingCollection = needRecordingCollection && br != nil

		if !needRecordingCollection {
			newSpan.Finish()
			return
		}

		rec = newSpan.FinishAndGetRecording(newSpan.RecordingType())
		if rec != nil {
			// Decide if the trace for this RPC, if any, will need to be redacted. It
			// needs to be redacted if the response goes to a tenant. In case the request
			// is local, then the trace might eventually go to a tenant (and tenID might
			// be set), but it will go to the tenant only indirectly, through the response
			// of a parent RPC. In that case, that parent RPC is responsible for the
			// redaction.
			//
			// Tenants get a redacted recording, i.e. with anything
			// sensitive stripped out of the verbose messages. However,
			// structured payloads stay untouched.
			needRedaction := tenID != roachpb.SystemTenantID
			if needRedaction {
				if err := redactRecordingForTenant(tenID, rec); err != nil {
					log.Errorf(ctx, "error redacting trace recording: %s", err)
					rec = nil
				}
			}
			br.CollectedSpans = append(br.CollectedSpans, rec...)
		}
	}
	return ctx, finishSpan
}
