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
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
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

// Embed the partitionCheck function into the stream and check it when we are
// sending or receiving a message.
type disablingClientStream struct {
	grpc.ClientStream
	partitionCheck func() error
}

func (d disablingClientStream) SendMsg(m interface{}) error {
	if err := d.partitionCheck(); err != nil {
		return err
	}
	return d.ClientStream.SendMsg(m)
}

func (d disablingClientStream) RecvMsg(m interface{}) error {
	if err := d.partitionCheck(); err != nil {
		return err
	}
	return d.ClientStream.RecvMsg(m)
}

// Partitioner is used to create partial partitions between nodes at the GRPC
// layer. It uses StreamInterceptors to fail requests to nodes that are not
// connected. Usage of it is something like the following:
//
// var p rpc.Partitioner
//
//	for i := 0; i < numServers; i++ {
//	   p.RegisterTestingKnobs(id, partitions, ContextTestingKnobs{})
//	}
//
// TestCluster.Start()
//
//	for i := 0; i < numServers; i++ {
//	    p.RegisterNodeAddr()
//	}
//
// p.EnablePartition(true)
// ... run operations
//
// TODO(baptist): This could be enhanced to allow dynamic partition injection.
type Partitioner struct {
	partitionEnabled atomic.Bool
	nodeAddrMap      sync.Map
}

// EnablePartition will enable or disable the partition.
func (p *Partitioner) EnablePartition(enable bool) {
	p.partitionEnabled.Store(enable)
}

// RegisterNodeAddr is called after the cluster is started, but before
// EnablePartition is called on every node to register the mapping from the
// address of the node to the NodeID.
func (p *Partitioner) RegisterNodeAddr(addr string, id roachpb.NodeID) {
	if p.partitionEnabled.Load() {
		panic("Can not register node addresses with a partition enabled")
	}
	p.nodeAddrMap.Store(addr, id)
}

// RegisterTestingKnobs creates the testing knobs for this node. It will
// override both the Unary and Stream Interceptors to return errors once
// EnablePartition is called.
func (p *Partitioner) RegisterTestingKnobs(
	id roachpb.NodeID, partition [][2]roachpb.NodeID, knobs *ContextTestingKnobs,
) {
	// Structure the partition list for indexed lookup. We are partitioned from
	// the other node if we are found on either side of the pair.
	partitionedServers := make(map[roachpb.NodeID]bool)
	for _, p := range partition {
		if p[0] == id {
			partitionedServers[p[1]] = true
		}
		if p[1] == id {
			partitionedServers[p[0]] = true
		}
	}
	isPartitioned := func(addr string) error {
		if !p.partitionEnabled.Load() {
			return nil
		}
		id, ok := p.nodeAddrMap.Load(addr)
		if !ok {
			panic("address not mapped, call RegisterNodeAddr before enabling the partition" + addr)
		}
		if partitionedServers[id.(roachpb.NodeID)] {
			return errors.Newf("partitioned from %s, n%d", addr, id)
		}
		return nil
	}
	knobs.UnaryClientInterceptor =
		func(target string, class ConnectionClass) grpc.UnaryClientInterceptor {
			return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if err := isPartitioned(target); err != nil {
					return err
				}
				return invoker(ctx, method, req, reply, cc, opts...)
			}
		}
	knobs.StreamClientInterceptor =
		func(target string, class ConnectionClass) grpc.StreamClientInterceptor {
			return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				cs, err := streamer(ctx, desc, cc, method, opts...)
				if err != nil {
					return nil, err
				}
				return &disablingClientStream{
					ClientStream:   cs,
					partitionCheck: func() error { return isPartitioned(target) },
				}, nil
			}
		}
}
