// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pprofutil

import (
	"context"
	"runtime/debug"
	runtimepprof "runtime/pprof"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type GoroutineLabelingInterceptor struct {
	labels []string
}

func NewGoroutineLabelingInterceptor(labels ...string) *GoroutineLabelingInterceptor {
	return &GoroutineLabelingInterceptor{labels: labels}
}

const (
	metadataGoroutineLabelKey = "crdb-lbls"

	labelServeRPC = "rpc-serve"
)

func appendGRPCIncomingLabels(ctx context.Context, dest []string) []string {
	md, _ := metadata.FromIncomingContext(ctx)
	return append(dest, md[metadataGoroutineLabelKey]...)
}

// func appendGoroutineLabels(ctx context.Context, dest []string) []string {
// 	runtimepprof.ForLabels(ctx, func(k, v string) bool {
// 		dest = append(dest, k, v)
// 		return true // more
// 	})
// 	return dest
// }

var e = log.Every(5 * time.Second)

// Only called server-side.
func appendRPCLabels(dest []string, rpcName string) []string {
	dest = append(dest, labelServeRPC, rpcName)

	if rpcName != "/cockroach.roachpb.Internal/Batch" {
		return dest
	}

	// Hacky stuff to track how many requests are slipping in without
	// a label. Assumes that appendRPCLables is called only when dest
	// already reflects the grpc metadata carried labels.
	var ok bool
	for k := range dest {
		if k%2 == 1 {
			continue
		}
		if dest[k] == "stmt.tag" {
			ok = true
			break
		}
	}
	if !ok {
		dest = append(dest, "stmt.tag", "not-found-at-server", "stmt.anonymized", "not-found-at-server")
		if e.ShouldLog() {
			debug.PrintStack()
		}
	}
	return dest
}

// Get the labels from the grpc metadata in ctx, add RPC-specific
// labels, add interceptor's hard-coded labels.
// Does not look at the goroutine labels already registered in the context;
// there shouldn't be any.
func (gl *GoroutineLabelingInterceptor) collectLabelsServer(
	ctx context.Context, rpcName string,
) []string {
	var labels []string
	labels = appendGRPCIncomingLabels(ctx, labels)
	labels = appendRPCLabels(labels, rpcName)
	labels = append(labels, gl.labels...)
	return labels
}

func (gl *GoroutineLabelingInterceptor) foo(
	ctx context.Context, rpcName string, f func(ctx context.Context),
) {
	panic("unused")
	// runtimepprof.Do(ctx, runtimepprof.Labels(gl.collectLabels(ctx, rpcName)...), f)
}

func (gl *GoroutineLabelingInterceptor) UnaryServerInterceptor(
	ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (interface{}, error) {
	var result interface{}
	var err error
	labels := gl.collectLabelsServer(ctx, info.FullMethod)
	runtimepprof.Do(ctx, runtimepprof.Labels(labels...), func(ctx context.Context) {
		result, err = handler(ctx, req)
	})
	return result, err
}

// wrappedServerStream is a thin wrapper around grpc.ServerStream that allows
// modifying its context.
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context overrides the nested grpc.ServerStream.Context().
func (ss *wrappedServerStream) Context() context.Context {
	return ss.ctx
}

func (gl *GoroutineLabelingInterceptor) StreamServerInterceptor(
	srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	ctx := ss.Context()
	labels := gl.collectLabelsServer(ctx, info.FullMethod)
	origSS := ss
	var err error
	runtimepprof.Do(ctx, runtimepprof.Labels(labels...), func(ctx context.Context) {
		err = handler(srv, &wrappedServerStream{
			ServerStream: origSS,
			ctx:          ctx,
		})
	})
	return err
}

func wrapOutgoingContext(ctx context.Context) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	} else {
		md = md.Copy() // since we will modify below
	}

	{
		var ok bool
		runtimepprof.ForLabels(ctx, func(k, v string) bool {
			md[metadataGoroutineLabelKey] = append(md[metadataGoroutineLabelKey], k, v)
			if k == "stmt.tag" {
				ok = true
			}
			return true // more
		})
		if !ok {
			if e.ShouldLog() {
				debug.PrintStack()
			}
			md[metadataGoroutineLabelKey] = append(
				md[metadataGoroutineLabelKey],
				"stmt.tag", "not-found-at-client", "stmt.anonymized", "not-found-at-client",
			)
		}
	}

	return metadata.NewOutgoingContext(ctx, md)
}

// ClientInterceptor returns a grpc.UnaryClientInterceptor suitable
// for use in a grpc.Dial call.
func (gl *GoroutineLabelingInterceptor) UnaryClientInterceptor(
	ctx context.Context,
	method string,
	req, resp interface{},
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	ctx = wrapOutgoingContext(ctx)
	return invoker(ctx, method, req, resp, cc, opts...)
}

// StreamClientInterceptor returns a grpc.StreamClientInterceptor suitable
// for use in a grpc.Dial call.
func (gl *GoroutineLabelingInterceptor) StreamClientInterceptor(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	ctx = wrapOutgoingContext(ctx)
	return streamer(ctx, desc, cc, method, opts...)
}

func AddLabels(ctx context.Context, labels ...string) (_ context.Context, undo func()) {
	origCtx := ctx
	ctx = runtimepprof.WithLabels(ctx, runtimepprof.Labels(labels...))
	runtimepprof.SetGoroutineLabels(ctx)
	return ctx, func() { runtimepprof.SetGoroutineLabels(origCtx) }
}
