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
	"fmt"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"golang.org/x/net/context"
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
		ctx:     ctx,
		evalCtx: parser.EvalContext{ReCache: parser.NewRegexpCache(512)},
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

// SetupFlows is part of the DistSQLServer interface.
func (ds *ServerImpl) SetupFlows(ctx context.Context, req *SetupFlowsRequest) (
	*SetupFlowsResponse, error,
) {
	txn := ds.setupTxn(ctx, &req.Txn)
	for _, f := range req.Flows {
		// TODO(radu): for now we expect exactly one processor (a table reader)
		reader, err := NewTableReader(f.Processors[0].Core.TableReader, txn, ds.evalCtx)
		if err != nil {
			return nil, err
		}
		if err := reader.Run(); err != nil {
			fmt.Println(err)
		}
	}
	return &SetupFlowsResponse{}, nil
}

// TODO(radu): prevent varcheck from complaining about (yet) unused constants
var _ = StreamEndpointSpec_LOCAL
var _ = StreamEndpointSpec_REMOTE
var _ = StreamEndpointSpec_RPC_SYNC_RESP
var _ = OutputRouterSpec_MIRROR
var _ = OutputRouterSpec_BY_HASH
var _ = OutputRouterSpec_BY_RANGE
