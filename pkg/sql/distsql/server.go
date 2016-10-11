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
	Context    context.Context
	DB         *client.DB
	RPCContext *rpc.Context
	Stopper    *stop.Stopper
}

// ServerImpl implements the server for the distributed SQL APIs.
type ServerImpl struct {
	ServerConfig
	evalCtx      parser.EvalContext
	flowRegistry *flowRegistry
}

// flowStreamTimeout is the amount of time incoming streams wait for a flow to
// be set up before erroring out.
const flowStreamTimeout time.Duration = 2000 * time.Millisecond

var _ DistSQLServer = &ServerImpl{}

// NewServer instantiates a DistSQLServer.
func NewServer(cfg ServerConfig) *ServerImpl {
	if tracing.TracerFromCtx(cfg.Context) == nil {
		panic("Server Context should have a Tracer")
	}
	ds := &ServerImpl{
		ServerConfig: cfg,
		evalCtx: parser.EvalContext{
			ReCache: parser.NewRegexpCache(512),
		},
		flowRegistry: makeFlowRegistry(),
	}
	return ds
}

func (ds *ServerImpl) setupTxn(ctx context.Context, txnProto *roachpb.Transaction) *client.Txn {
	txn := client.NewTxn(ctx, *ds.DB)
	// TODO(radu): we should sanity check some of these fields
	txn.Proto = *txnProto
	return txn
}

func (ds *ServerImpl) logContext(ctx context.Context) context.Context {
	return log.WithLogTagsFromCtx(ctx, ds.ServerConfig.Context)
}

func (ds *ServerImpl) setupFlow(
	ctx context.Context, req *SetupFlowRequest, simpleFlowConsumer RowReceiver,
) (*Flow, error) {
	sp, err := tracing.JoinOrNew(tracing.TracerFromCtx(ctx), req.TraceContext, "flow")
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
	ctx = ds.logContext(ctx)
	return ds.setupFlow(ctx, req, output)
}

// RunSimpleFlow is part of the DistSQLServer interface.
func (ds *ServerImpl) RunSimpleFlow(
	req *SetupFlowRequest, stream DistSQL_RunSimpleFlowServer,
) error {
	// Set up the outgoing mailbox for the stream.
	mbox := newOutboxSimpleFlowStream(stream)

	f, err := ds.SetupSimpleFlow(ds.Context, req, mbox)
	if err != nil {
		log.Error(ds.Context, err)
		return err
	}
	mbox.setFlowCtx(&f.FlowCtx)

	if err := ds.Stopper.RunTask(func() {
		mbox.start(&f.waitGroup)
		f.Start()
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
	f, err := ds.setupFlow(ds.Context, req, nil)
	if err != nil {
		return nil, err
	}
	f.Start()
	// TODO(radu): firing off a goroutine just to call Cleanup is temporary. We
	// will have a flow scheduler that will be notified when the flow completes.
	ds.Stopper.RunWorker(func() {
		f.Wait()
		f.Cleanup()
	})
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
		log.Error(ds.Context, err)
	}
	return err
}
