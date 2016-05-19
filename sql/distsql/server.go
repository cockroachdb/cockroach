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
	"golang.org/x/net/context"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
)

// ServerContext encompasses the configuration required to create a
// DistSQLServer.
type ServerContext struct {
	DB *client.DB
}

// ServerImpl implements the server for the distributed SQL APIs.
type ServerImpl struct {
	ctx     ServerContext
	evalCtx parser.EvalContext
}

var _ DistSQLServer = &ServerImpl{}

// NewServer instantiates a DistSQLServer.
func NewServer(ctx ServerContext) *ServerImpl {
	ds := &ServerImpl{
		ctx: ctx,
		evalCtx: parser.EvalContext{
			ReCache: parser.NewRegexpCache(512),
			TmpDec:  new(inf.Dec),
		},
	}
	return ds
}

func (ds *ServerImpl) setupTxn(
	ctx context.Context,
	txnProto *roachpb.Transaction,
) *client.Txn {
	txn := client.NewTxn(ctx, *ds.ctx.DB)
	// TODO(radu): we should sanity check some of these fields
	txn.Proto = *txnProto
	return txn
}

// RunSimpleFlow is part of the DistSQLServer interface.
func (ds *ServerImpl) RunSimpleFlow(
	req *SetupFlowsRequest, stream DistSQL_RunSimpleFlowServer,
) error {
	if len(req.Flows) != 1 {
		return util.Errorf("expected exactly one flow, got %d", len(req.Flows))
	}

	f := &flow{evalCtx: ds.evalCtx}
	f.txn = ds.setupTxn(stream.Context(), &req.Txn)

	// Set up the outgoing mailbox for the stream.
	mbox := newOutbox(stream)
	f.simpleFlowMailbox = mbox

	flow := req.Flows[0]

	// TODO(radu): for now we expect exactly one processor (a table reader).
	if len(flow.Processors) != 1 {
		return util.Errorf("only single-processor flows supported")
	}
	reader, err := f.setupProcessor(&flow.Processors[0])
	if err != nil {
		return err
	}

	// TODO(radu): this stuff should probably be run through a stopper.
	f.waitGroup.Add(1)
	mbox.start(&f.waitGroup)
	// The reader will run in its own goroutine once we support
	// less trivial flows.
	reader.run()
	f.waitGroup.Wait()
	return mbox.err
}

// SetupFlows is part of the DistSQLServer interface.
func (ds *ServerImpl) SetupFlows(ctx context.Context, req *SetupFlowsRequest) (
	*SimpleResponse, error,
) {
	return nil, util.Errorf("not implemented")
}
