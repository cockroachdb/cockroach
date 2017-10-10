// Copyright 2017 The Cockroach Authors.
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
//
//
// Utilities for tests in the distsqlrun package around mocking DistSQLServer
// gRPC clients and servers.
//

package distsqlrun

import (
	"net"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

var intType = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
var boolType = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL}
var decType = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_DECIMAL}
var strType = sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING}
var oneIntCol = []sqlbase.ColumnType{intType}
var twoIntCols = []sqlbase.ColumnType{intType, intType}
var threeIntCols = []sqlbase.ColumnType{intType, intType, intType}

// startMockDistSQLServer starts a MockDistSQLServer and returns the address on
// which it's listening.
func startMockDistSQLServer(stopper *stop.Stopper) (*MockDistSQLServer, net.Addr, error) {
	rpcContext := newInsecureRPCContext(stopper)
	server := rpc.NewServer(rpcContext)
	mock := newMockDistSQLServer()
	RegisterDistSQLServer(server, mock)
	ln, err := netutil.ListenAndServeGRPC(stopper, server, util.IsolatedTestAddr)
	if err != nil {
		return nil, nil, err
	}
	return mock, ln.Addr(), nil
}

func newInsecureRPCContext(stopper *stop.Stopper) *rpc.Context {
	return rpc.NewContext(
		log.AmbientContext{Tracer: tracing.NewTracer()},
		&base.Config{Insecure: true},
		hlc.NewClock(hlc.UnixNano, time.Nanosecond),
		stopper,
	)
}

// MockDistSQLServer implements the DistSQLServer (gRPC) interface and allows
// clients to control the inbound streams.
type MockDistSQLServer struct {
	inboundStreams   chan InboundStreamNotification
	runSyncFlowCalls chan RunSyncFlowCall
}

// InboundStreamNotification is the MockDistSQLServer's way to tell its clients
// that a new gRPC call has arrived and thus a stream has arrived. The rpc
// handler is blocked until donec is signaled.
type InboundStreamNotification struct {
	stream DistSQL_FlowStreamServer
	donec  chan<- error
}

type RunSyncFlowCall struct {
	stream DistSQL_RunSyncFlowServer
	donec  chan<- error
}

// MockDistSQLServer implements the DistSQLServer interface.
var _ DistSQLServer = &MockDistSQLServer{}

func newMockDistSQLServer() *MockDistSQLServer {
	return &MockDistSQLServer{
		inboundStreams:   make(chan InboundStreamNotification),
		runSyncFlowCalls: make(chan RunSyncFlowCall),
	}
}

// RunSyncFlow is part of the DistSQLServer interface.
func (ds *MockDistSQLServer) RunSyncFlow(stream DistSQL_RunSyncFlowServer) error {
	donec := make(chan error)
	ds.runSyncFlowCalls <- RunSyncFlowCall{stream: stream, donec: donec}
	return <-donec
}

// SetupFlow is part of the DistSQLServer interface.
func (ds *MockDistSQLServer) SetupFlow(
	_ context.Context, req *SetupFlowRequest,
) (*SimpleResponse, error) {
	return nil, nil
}

// FlowStream is part of the DistSQLServer interface.
func (ds *MockDistSQLServer) FlowStream(stream DistSQL_FlowStreamServer) error {
	donec := make(chan error)
	ds.inboundStreams <- InboundStreamNotification{stream: stream, donec: donec}
	return <-donec
}

// createDummyStream creates the server and client side of a FlowStream stream.
// This can be use by tests to pretend that then have received a FlowStream RPC.
// The stream can be used to send messages (ConsumerSignal's) on it (within a
// gRPC window limit since nobody's reading from the stream), for example
// Handshake messages.
//
// We do this by creating a mock server, dialing into it and capturing the
// server stream. The server-side RPC call will be blocked until the caller
// calls the returned cleanup function.
func createDummyStream() (
	serverStream DistSQL_FlowStreamServer,
	clientStream DistSQL_FlowStreamClient,
	cleanup func(),
	err error,
) {
	stopper := stop.NewStopper()
	mockServer, addr, err := startMockDistSQLServer(stopper)
	if err != nil {
		return nil, nil, nil, err
	}
	rpcCtx := newInsecureRPCContext(stopper)
	conn, err := rpcCtx.GRPCDial(addr.String())
	if err != nil {
		return nil, nil, nil, err
	}
	client := NewDistSQLClient(conn)
	clientStream, err = client.FlowStream(context.TODO())
	if err != nil {
		return nil, nil, nil, err
	}
	streamNotification := <-mockServer.inboundStreams
	serverStream = streamNotification.stream
	cleanup = func() {
		close(streamNotification.donec)
		stopper.Stop(context.TODO())
	}
	return serverStream, clientStream, cleanup, nil
}
