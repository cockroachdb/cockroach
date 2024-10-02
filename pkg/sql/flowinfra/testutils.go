// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package flowinfra

import (
	"context"
	"net"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/logtags"
	"google.golang.org/grpc"
)

func newInsecureRPCContext(ctx context.Context, stopper *stop.Stopper) *rpc.Context {
	nc := &base.NodeIDContainer{}
	ctx = logtags.AddTag(ctx, "n", nc)
	opts := rpc.DefaultContextOptions()
	opts.Insecure = true
	opts.Stopper = stopper
	opts.Settings = cluster.MakeTestingClusterSettings()
	opts.NodeID = nc
	return rpc.NewContext(ctx, opts)
}

// StartMockDistSQLServer starts a MockDistSQLServer and returns the address on
// which it's listening.
// The cluster ID value returned is the storage cluster ID.
func StartMockDistSQLServer(
	ctx context.Context, clock *hlc.Clock, stopper *stop.Stopper, sqlInstanceID base.SQLInstanceID,
) (uuid.UUID, *MockDistSQLServer, net.Addr, error) {
	rpcContext := newInsecureRPCContext(ctx, stopper)
	rpcContext.NodeID.Set(ctx, roachpb.NodeID(sqlInstanceID))
	server, err := rpc.NewServer(ctx, rpcContext)
	if err != nil {
		return uuid.Nil, nil, nil, err
	}
	mock := newMockDistSQLServer()
	execinfrapb.RegisterDistSQLServer(server, mock)
	ln, err := netutil.ListenAndServeGRPC(stopper, server, util.IsolatedTestAddr)
	if err != nil {
		return uuid.Nil, nil, nil, err
	}
	return rpcContext.StorageClusterID.Get(), mock, ln.Addr(), nil
}

// MockDistSQLServer implements the DistSQLServer (gRPC) interface and allows
// clients to control the inbound streams.
type MockDistSQLServer struct {
	InboundStreams chan InboundStreamNotification
}

// InboundStreamNotification is the MockDistSQLServer's way to tell its clients
// that a new gRPC call has arrived and thus a stream has arrived. The rpc
// handler is blocked until Donec is signaled.
type InboundStreamNotification struct {
	Stream execinfrapb.DistSQL_FlowStreamServer
	Donec  chan<- error
}

// MockDistSQLServer implements the DistSQLServer interface.
var _ execinfrapb.DistSQLServer = &MockDistSQLServer{}

func newMockDistSQLServer() *MockDistSQLServer {
	return &MockDistSQLServer{
		InboundStreams: make(chan InboundStreamNotification),
	}
}

// SetupFlow is part of the DistSQLServer interface.
func (ds *MockDistSQLServer) SetupFlow(
	_ context.Context, req *execinfrapb.SetupFlowRequest,
) (*execinfrapb.SimpleResponse, error) {
	return nil, nil
}

// CancelDeadFlows is part of the DistSQLServer interface.
func (ds *MockDistSQLServer) CancelDeadFlows(
	_ context.Context, req *execinfrapb.CancelDeadFlowsRequest,
) (*execinfrapb.SimpleResponse, error) {
	return nil, nil
}

// FlowStream is part of the DistSQLServer interface.
func (ds *MockDistSQLServer) FlowStream(stream execinfrapb.DistSQL_FlowStreamServer) error {
	donec := make(chan error)
	ds.InboundStreams <- InboundStreamNotification{Stream: stream, Donec: donec}
	return <-donec
}

// MockDialer is a mocked implementation of the Outbox's `Dialer` interface.
// Used to create a connection with a client stream.
type MockDialer struct {
	// Addr is assumed to be obtained from flowinfra.StartMockDistSQLServer.
	Addr net.Addr
	mu   struct {
		syncutil.Mutex
		conn *grpc.ClientConn
	}
}

// DialNoBreaker establishes a grpc connection once.
func (d *MockDialer) DialNoBreaker(
	context.Context, roachpb.NodeID, rpc.ConnectionClass,
) (*grpc.ClientConn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.mu.conn != nil {
		return d.mu.conn, nil
	}
	var err error
	//lint:ignore SA1019 grpc.WithInsecure is deprecated
	d.mu.conn, err = grpc.Dial(d.Addr.String(), grpc.WithInsecure(), grpc.WithBlock())
	return d.mu.conn, err
}

// Close must be called after the test is done.
func (d *MockDialer) Close() {
	err := d.mu.conn.Close() // nolint:grpcconnclose
	if err != nil {
		panic(err)
	}
}
