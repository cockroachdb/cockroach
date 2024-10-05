// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"google.golang.org/grpc"
)

// ContextTestingKnobs provides hooks to aid in testing the system. The testing
// knob functions are called at various points in the Context life cycle if they
// are non-nil.
type ContextTestingKnobs struct {
	// StreamClient if non-nil will be called at dial time to provide
	// the base stream interceptor for client connections.
	// This function may return a nil interceptor to avoid injecting behavior
	// for a given target and class.
	//
	// Note that this is not called for streaming RPCs using the
	// internalClientAdapter - i.e. KV RPCs done against the local server.
	StreamClientInterceptor func(target string, class ConnectionClass) grpc.StreamClientInterceptor

	// UnaryClientInterceptor, if non-nil, will be called when invoking any
	// unary RPC.
	UnaryClientInterceptor func(target string, class ConnectionClass) grpc.UnaryClientInterceptor

	// InjectedLatencyOracle if non-nil contains a map from target address
	// (server.RPCServingAddr() of a remote node) to artificial latency in
	// milliseconds to inject. Setting this will cause the server to pause for
	// the given duration on every network write.
	InjectedLatencyOracle InjectedLatencyOracle

	// InjectedLatencyEnabled is used to turn on or off the InjectedLatencyOracle.
	InjectedLatencyEnabled func() bool

	// StorageClusterID initializes the Context's StorageClusterID container to
	// this value if non-nil at construction time.
	StorageClusterID *uuid.UUID

	// NoLoopbackDialer, when set, indicates that a test does not care
	// about the special loopback dial semantics.
	// If this is left unset, the test is responsible for ensuring
	// SetLoopbackDialer() has been called on the rpc.Context.
	// (This is done automatically by server.Server/server.SQLServerWrapper.)
	NoLoopbackDialer bool
}

// InjectedLatencyOracle is a testing mechanism used to inject artificial
// latency to an address.
type InjectedLatencyOracle interface {
	GetLatency(addr string) time.Duration
}

// NewInsecureTestingContext creates an insecure rpc Context suitable for tests.
func NewInsecureTestingContext(
	ctx context.Context, clock *hlc.Clock, stopper *stop.Stopper,
) *Context {
	clusterID := uuid.MakeV4()
	return NewInsecureTestingContextWithClusterID(ctx, clock, stopper, clusterID)
}

// NewInsecureTestingContextWithClusterID creates an insecure rpc Context
// suitable for tests. The context is given the provided storage cluster ID and
// will derive a logical cluster ID from it automatically.
func NewInsecureTestingContextWithClusterID(
	ctx context.Context, clock *hlc.Clock, stopper *stop.Stopper, storageClusterID uuid.UUID,
) *Context {
	return NewInsecureTestingContextWithKnobs(ctx,
		clock, stopper, ContextTestingKnobs{
			StorageClusterID: &storageClusterID,
		})
}

// NewInsecureTestingContextWithKnobs creates an insecure rpc Context
// suitable for tests configured with the provided knobs.
func NewInsecureTestingContextWithKnobs(
	ctx context.Context, clock *hlc.Clock, stopper *stop.Stopper, knobs ContextTestingKnobs,
) *Context {
	opts := DefaultContextOptions()
	opts.Insecure = true
	opts.Clock = clock.WallClock()
	opts.ToleratedOffset = clock.ToleratedOffset()
	opts.Settings = cluster.MakeTestingClusterSettings()
	opts.Knobs = knobs
	opts.Stopper = stopper

	return NewContext(ctx, opts)
}
