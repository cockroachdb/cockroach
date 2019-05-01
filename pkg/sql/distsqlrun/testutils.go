// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlrun

import (
	"context"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func newInsecureRPCContext(stopper *stop.Stopper) *rpc.Context {
	return rpc.NewContext(
		log.AmbientContext{Tracer: tracing.NewTracer()},
		&base.Config{Insecure: true},
		hlc.NewClock(hlc.UnixNano, time.Nanosecond),
		stopper,
		&cluster.MakeTestingClusterSettings().Version,
	)
}

// StartMockDistSQLServer starts a MockDistSQLServer and returns the address on
// which it's listening.
func StartMockDistSQLServer(
	stopper *stop.Stopper, nodeID roachpb.NodeID,
) (uuid.UUID, *MockDistSQLServer, net.Addr, error) {
	rpcContext := newInsecureRPCContext(stopper)
	rpcContext.NodeID.Set(context.TODO(), nodeID)
	server := rpc.NewServer(rpcContext)
	mock := newMockDistSQLServer()
	distsqlpb.RegisterDistSQLServer(server, mock)
	ln, err := netutil.ListenAndServeGRPC(stopper, server, util.IsolatedTestAddr)
	if err != nil {
		return uuid.Nil, nil, nil, err
	}
	return rpcContext.ClusterID.Get(), mock, ln.Addr(), nil
}

// MockDistSQLServer implements the DistSQLServer (gRPC) interface and allows
// clients to control the inbound streams.
type MockDistSQLServer struct {
	InboundStreams   chan InboundStreamNotification
	runSyncFlowCalls chan RunSyncFlowCall
}

// InboundStreamNotification is the MockDistSQLServer's way to tell its clients
// that a new gRPC call has arrived and thus a stream has arrived. The rpc
// handler is blocked until Donec is signaled.
type InboundStreamNotification struct {
	Stream distsqlpb.DistSQL_FlowStreamServer
	Donec  chan<- error
}

// RunSyncFlowCall is the MockDistSQLServer's way to tell its clients that a
// RunSyncFlowCall has arrived. The rpc handler is blocked until Donec is
// signaled.
type RunSyncFlowCall struct {
	stream distsqlpb.DistSQL_RunSyncFlowServer
	donec  chan<- error
}

// MockDistSQLServer implements the DistSQLServer interface.
var _ distsqlpb.DistSQLServer = &MockDistSQLServer{}

func newMockDistSQLServer() *MockDistSQLServer {
	return &MockDistSQLServer{
		InboundStreams:   make(chan InboundStreamNotification),
		runSyncFlowCalls: make(chan RunSyncFlowCall),
	}
}

// RunSyncFlow is part of the DistSQLServer interface.
func (ds *MockDistSQLServer) RunSyncFlow(stream distsqlpb.DistSQL_RunSyncFlowServer) error {
	donec := make(chan error)
	ds.runSyncFlowCalls <- RunSyncFlowCall{stream: stream, donec: donec}
	return <-donec
}

// SetupFlow is part of the DistSQLServer interface.
func (ds *MockDistSQLServer) SetupFlow(
	_ context.Context, req *distsqlpb.SetupFlowRequest,
) (*distsqlpb.SimpleResponse, error) {
	return nil, nil
}

// FlowStream is part of the DistSQLServer interface.
func (ds *MockDistSQLServer) FlowStream(stream distsqlpb.DistSQL_FlowStreamServer) error {
	donec := make(chan error)
	ds.InboundStreams <- InboundStreamNotification{Stream: stream, Donec: donec}
	return <-donec
}
