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

func extractGoroutineLabels(ctx context.Context) []string {
	var sl []string
	runtimepprof.ForLabels(ctx, func(k, v string) bool {
		sl = append(sl, k, v)
		return true // more
	})
	md, _ := metadata.FromIncomingContext(ctx)
	return append(sl, md[metadataGoroutineLabelKey]...)
}

var e = log.Every(5 * time.Second)

func (gl *GoroutineLabelingInterceptor) collectLabels(
	ctx context.Context, rpcName string,
) []string {
	// NB: gl.labels intentionally takes precedence over anything
	// coming in from the other side.
	labels := extractGoroutineLabels(ctx)
	labels = append(labels, gl.labels...)
	if rpcName != "" {
		labels = append(labels, labelServeRPC, rpcName)
	}

	if rpcName == "/cockroach.roachpb.Internal/Batch" {
		var ok bool
		for k := range labels {
			if k%2 == 1 {
				continue
			}
			if labels[k] == "stmt.tag" {
				ok = true
				break
			}
		}
		if !ok {
			labels = append(labels, "stmt.tag", "missing-server", "stmt.anonymized", "missing-client")
			if e.ShouldLog() {
				debug.PrintStack()
			}
		}
	}
	return labels
}

func (gl *GoroutineLabelingInterceptor) Do(
	ctx context.Context, rpcName string, f func(ctx context.Context),
) {
	runtimepprof.Do(ctx, runtimepprof.Labels(gl.collectLabels(ctx, rpcName)...), f)
}

func (gl *GoroutineLabelingInterceptor) UnaryServerInterceptor(
	ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (interface{}, error) {
	var result interface{}
	var err error
	labels := gl.collectLabels(ctx, info.FullMethod)
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
	labels := gl.collectLabels(ctx, info.FullMethod)
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
	var found = true
	runtimepprof.ForLabels(ctx, func(k, v string) bool {
		md[metadataGoroutineLabelKey] = append(md[metadataGoroutineLabelKey], k, v)
		if k == "stmt.tag" {
			found = true
		}
		return true // more
	})
	if found == false {
		md[metadataGoroutineLabelKey] = append(
			md[metadataGoroutineLabelKey],
			"stmt.tag", "missing-client", "stmt.anonymized", "missing-client",
		)
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
