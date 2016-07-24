// Copyright 2016 The Cockroach Authors.
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
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsql

import (
	"io"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/pkg/errors"
)

// ServerContext encompasses the configuration required to create a
// DistSQLServer.
type ServerContext struct {
	Context    context.Context
	DB         *client.DB
	RPCContext *rpc.Context
}

// ServerImpl implements the server for the distributed SQL APIs.
type ServerImpl struct {
	ServerContext
	evalCtx      parser.EvalContext
	flowRegistry *flowRegistry
}

// flowStreamTimeout is the amount of time incoming streams wait for a flow to
// be set up before erroring out.
const flowStreamTimeout time.Duration = 2000 * time.Millisecond

var _ DistSQLServer = &ServerImpl{}

// NewServer instantiates a DistSQLServer.
func NewServer(ctx ServerContext) *ServerImpl {
	ds := &ServerImpl{
		ServerContext: ctx,
		evalCtx: parser.EvalContext{
			ReCache: parser.NewRegexpCache(512),
		},
		flowRegistry: makeFlowRegistry(),
	}
	return ds
}

// SetNodeID sets the NodeID for the server.
func (ds *ServerImpl) SetNodeID(nodeID roachpb.NodeID) {
	ds.ServerContext.Context = log.WithLogTagInt(ds.ServerContext.Context, "node", int(nodeID))
}

func (ds *ServerImpl) setupTxn(
	ctx context.Context,
	txnProto *roachpb.Transaction,
) *client.Txn {
	txn := client.NewTxn(ctx, *ds.DB)
	// TODO(radu): we should sanity check some of these fields
	txn.Proto = *txnProto
	return txn
}

// SetupSimpleFlow sets up a simple flow, connecting the simple response output
// stream to the given RowReceiver. The flow is not started.
func (ds *ServerImpl) SetupSimpleFlow(
	ctx context.Context, req *SetupFlowRequest, output RowReceiver,
) (*Flow, error) {
	txn := ds.setupTxn(ctx, &req.Txn)
	flowCtx := FlowCtx{
		Context: ds.ServerContext.Context,
		id:      req.Flow.FlowID,
		evalCtx: &ds.evalCtx,
		rpcCtx:  ds.RPCContext,
		txn:     txn,
	}

	f := newFlow(flowCtx, ds.flowRegistry, output)
	err := f.setupFlow(&req.Flow)
	if err != nil {
		log.Errorf(ds.Context, err.Error(), "", err)
		return nil, err
	}
	return f, nil
}

// RunSimpleFlow is part of the DistSQLServer interface.
func (ds *ServerImpl) RunSimpleFlow(
	req *SetupFlowRequest, stream DistSQL_RunSimpleFlowServer,
) error {
	ctx := ds.ServerContext.Context

	// Set up the outgoing mailbox for the stream.
	mbox := newOutboxSimpleFlowStream(stream)

	f, err := ds.SetupSimpleFlow(ctx, req, mbox)
	if err != nil {
		log.Errorf(ds.Context, err.Error(), "", err)
		return err
	}
	mbox.setFlowCtx(&f.FlowCtx)

	// TODO(radu): this stuff should probably be run through a stopper.
	mbox.start(&f.waitGroup)
	f.Start()
	f.Wait()
	f.Cleanup()
	return mbox.err
}

// SetupFlow is part of the DistSQLServer interface.
func (ds *ServerImpl) SetupFlow(ctx context.Context, req *SetupFlowRequest) (
	*SimpleResponse, error,
) {
	// Note: ctx will be canceled when the RPC completes, so we can't associate
	// it with the transaction.

	txn := ds.setupTxn(ds.ServerContext.Context, &req.Txn)
	flowCtx := FlowCtx{
		Context: ds.ServerContext.Context,
		id:      req.Flow.FlowID,
		evalCtx: &ds.evalCtx,
		rpcCtx:  ds.RPCContext,
		txn:     txn,
	}
	f := newFlow(flowCtx, ds.flowRegistry, nil)
	err := f.setupFlow(&req.Flow)
	if err != nil {
		log.Errorf(ds.Context, err.Error(), "", err)
		return nil, err
	}
	f.Start()
	// TODO(radu): firing off a goroutine just to call Cleanup is temporary. We
	// will have a flow scheduler that will be notified when the flow completes.
	go func() {
		f.Wait()
		f.Cleanup()
	}()
	return &SimpleResponse{}, nil
}

func (ds *ServerImpl) flowStreamInt(stream DistSQL_FlowStreamServer) error {
	// Receive the first message.
	msg, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return errors.Errorf("empty stream")
		}
		return err
	}
	if msg.Header == nil {
		return errors.Errorf("no header in first message")
	}
	flowID := msg.Header.FlowID
	f := ds.flowRegistry.LookupFlow(flowID, flowStreamTimeout)
	if f == nil {
		return errors.Errorf("flow %s not found", flowID)
	}
	rowChan, err := f.getInboundStream(msg.Header.StreamID)
	if err != nil {
		return err
	}
	return ProcessInboundStream(&f.FlowCtx, stream, msg, rowChan)
}

// FlowStream is part of the DistSQLServer interface.
func (ds *ServerImpl) FlowStream(stream DistSQL_FlowStreamServer) error {
	err := ds.flowStreamInt(stream)
	if err != nil {
		log.Errorf(ds.Context, err.Error(), "", err)
	}
	return err
}
