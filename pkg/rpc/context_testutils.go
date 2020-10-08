// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"google.golang.org/grpc"
)

// ContextTestingKnobs provides hooks to aid in testing the system. The testing
// knob functions are called at various points in the Context life cycle if they
// are non-nil.
type ContextTestingKnobs struct {

	// UnaryClientInterceptor if non-nil will be called at dial time to provide
	// the base unary interceptor for client connections.
	// This function may return a nil interceptor to avoid injecting behavior
	// for a given target and class.
	UnaryClientInterceptor func(target string, class ConnectionClass) grpc.UnaryClientInterceptor

	// StreamClient if non-nil will be called at dial time to provide
	// the base stream interceptor for client connections.
	// This function may return a nil interceptor to avoid injecting behavior
	// for a given target and class.
	StreamClientInterceptor func(target string, class ConnectionClass) grpc.StreamClientInterceptor

	// ArtificialLatencyMap if non-nil contains a map from target address
	// (server.RPCServingAddr() of a remote node) to artificial latency in
	// milliseconds to inject. Setting this will cause the server to pause for
	// the given amount of milliseconds on every network write.
	ArtificialLatencyMap map[string]int

	// ClusterID initializes the Context's ClusterID container to this value if
	// non-nil at construction time.
	ClusterID *uuid.UUID
}

// NewInsecureTestingContext creates an insecure rpc Context suitable for tests.
func NewInsecureTestingContext(clock *hlc.Clock, stopper *stop.Stopper) *Context {
	clusterID := uuid.MakeV4()
	return NewInsecureTestingContextWithClusterID(clock, stopper, clusterID)
}

// NewInsecureTestingContextWithClusterID creates an insecure rpc Context
// suitable for tests. The context is given the provided cluster ID.
func NewInsecureTestingContextWithClusterID(
	clock *hlc.Clock, stopper *stop.Stopper, clusterID uuid.UUID,
) *Context {
	return NewInsecureTestingContextWithKnobs(clock, stopper, ContextTestingKnobs{
		ClusterID: &clusterID,
	})
}

// NewInsecureTestingContextWithKnobs creates an insecure rpc Context
// suitable for tests configured with the provided knobs.
func NewInsecureTestingContextWithKnobs(
	clock *hlc.Clock, stopper *stop.Stopper, knobs ContextTestingKnobs,
) *Context {
	return NewContext(ContextOptions{
		TenantID:   roachpb.SystemTenantID,
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Config:     &base.Config{Insecure: true},
		Clock:      clock,
		Stopper:    stopper,
		Settings:   cluster.MakeTestingClusterSettings(),
		Knobs:      knobs,
	})
}
