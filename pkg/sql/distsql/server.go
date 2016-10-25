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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// ServerConfig encompasses the configuration required to create a
// DistSQLServer.
type ServerConfig struct {
	log.AmbientContext

	DB         *client.DB
	RPCContext *rpc.Context
	Stopper    *stop.Stopper
}

// ServerImpl implements the server for the distributed SQL APIs.
type ServerImpl struct {
	ServerConfig
	evalCtx       parser.EvalContext
	flowRegistry  *flowRegistry
	flowScheduler *flowScheduler
}

var _ DistSQLServer = &ServerImpl{}

// NewServer instantiates a DistSQLServer.
func NewServer(cfg ServerConfig) *ServerImpl {
	ds := &ServerImpl{
		ServerConfig: cfg,
		evalCtx: parser.EvalContext{
			ReCache: parser.NewRegexpCache(512),
		},
		flowRegistry:  makeFlowRegistry(),
		flowScheduler: newFlowScheduler(cfg.AmbientContext, cfg.Stopper),
	}
	return ds
}

// Start launches workers for the server.
func (ds *ServerImpl) Start() {
	ds.flowScheduler.Start()
}

func (ds *ServerImpl) setupTxn(ctx context.Context, txnProto *roachpb.Transaction) *client.Txn {
	txn := client.NewTxn(ctx, *ds.DB)
	// TODO(radu): we should sanity check some of these fields
	txn.Proto = *txnProto
	return txn
}

func (ds *ServerImpl) setupFlow(
	ctx context.Context, req *SetupFlowRequest, simpleFlowConsumer RowReceiver,
) (*Flow, error) {
	sp, err := tracing.JoinOrNew(ds.AmbientContext.Tracer, req.TraceContext, "flow")
	if err != nil {
		return nil, err
	}
	ctx = opentracing.ContextWithSpan(ctx, sp)

	txn := ds.setupTxn(ctx, &req.Txn)
	flowCtx := FlowCtx{
		Context: ctx,
		id:      req.Flow.FlowID,
		evalCtx: &ds.evalCtx,
		rpcCtx:  ds.RPCContext,
		txn:     txn,
	}

	f := newFlow(flowCtx, ds.flowRegistry, simpleFlowConsumer)
	if err := f.setupFlow(&req.Flow); err != nil {
		log.Error(ctx, err)
		sp.Finish()
		return nil, err
	}
	return f, nil
}

// SetupSimpleFlow sets up a simple flow, connecting the simple response output
// stream to the given RowReceiver. The flow is not started.
// The flow will be associated with the given context.
func (ds *ServerImpl) SetupSimpleFlow(
	ctx context.Context, req *SetupFlowRequest, output RowReceiver,
) (*Flow, error) {
	return ds.setupFlow(ds.AnnotateCtx(ctx), req, output)
}

// RunSimpleFlow is part of the DistSQLServer interface.
func (ds *ServerImpl) RunSimpleFlow(
	req *SetupFlowRequest, stream DistSQL_RunSimpleFlowServer,
) error {
	// Set up the outgoing mailbox for the stream.
	mbox := newOutboxSimpleFlowStream(stream)
	ctx := ds.AnnotateCtx(stream.Context())

	f, err := ds.SetupSimpleFlow(ctx, req, mbox)
	if err != nil {
		log.Error(ctx, err)
		return err
	}
	mbox.setFlowCtx(&f.FlowCtx)

	if err := ds.Stopper.RunTask(func() {
		f.waitGroup.Add(1)
		mbox.start(&f.waitGroup)
		f.Start(func() {})
		f.Wait()
		f.Cleanup()
	}); err != nil {
		return err
	}
	return mbox.err
}

// SetupFlow is part of the DistSQLServer interface.
func (ds *ServerImpl) SetupFlow(_ context.Context, req *SetupFlowRequest) (*SimpleResponse, error) {
	// Note: the passed context will be canceled when this RPC completes, so we
	// can't associate it with the flow.
	ctx := ds.AnnotateCtx(context.TODO())
	f, err := ds.setupFlow(ctx, req, nil)
	if err != nil {
		return nil, err
	}
	if err := ds.flowScheduler.ScheduleFlow(f); err != nil {
		return nil, err
	}
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
	f, streamInfo, err := ds.flowRegistry.ConnectInboundStream(flowID, msg.Header.StreamID)
	if err != nil {
		return err
	}
	defer ds.flowRegistry.FinishInboundStream(streamInfo)
	return ProcessInboundStream(&f.FlowCtx, stream, msg, streamInfo.receiver)
}

// FlowStream is part of the DistSQLServer interface.
func (ds *ServerImpl) FlowStream(stream DistSQL_FlowStreamServer) error {
	ctx := ds.AnnotateCtx(stream.Context())
	err := ds.flowStreamInt(stream)
	if err != nil {
		log.Error(ctx, err)
	}
	return err
}
