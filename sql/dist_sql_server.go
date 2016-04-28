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

package sql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"golang.org/x/net/context"
)

// DistSQLServerContext encompasses the configuration required to create a
// DistSQLServer.
type DistSQLServerContext struct {
	DB *client.DB
}

type distSQLServerImpl struct {
	ctx     DistSQLServerContext
	evalCtx parser.EvalContext
}

var _ DistSQLServer = &distSQLServerImpl{}

// NewDistSQLServer instantiates a DistSQLServer.
func NewDistSQLServer(ctx DistSQLServerContext) DistSQLServer {
	ds := &distSQLServerImpl{
		ctx:     ctx,
		evalCtx: parser.EvalContext{ReCache: parser.NewRegexpCache(512)},
	}
	return ds
}

func (ds *distSQLServerImpl) setupTxn(
	ctx context.Context,
	txnProto *roachpb.Transaction,
) *client.Txn {
	txn := client.NewTxn(ctx, *ds.ctx.DB)
	// TODO(radu): we should sanity check some of these fields
	txn.Proto = *txnProto
	return txn
}

// SetupFlows is part of the DistSQLServer interface.
func (ds *distSQLServerImpl) SetupFlows(ctx context.Context, req *SetupFlowsRequest) (
	*EmptyResponse, error,
) {
	txn := ds.setupTxn(ctx, &req.Txn)
	for _, f := range req.Flows {
		reader, err := NewTableReader(f.Reader, txn, ds.evalCtx)
		if err != nil {
			return nil, err
		}
		pErr := reader.Run()
		if pErr != nil {
			fmt.Println(pErr)
		}
	}
	return &EmptyResponse{}, nil
}
