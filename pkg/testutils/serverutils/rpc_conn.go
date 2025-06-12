// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverutils

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"google.golang.org/grpc"
)

// grpcConn implements server.RPCConn that provides methods to create various
// RPC clients. This allows the CLI to interact with the server using gRPC
// without exposing the underlying connection details.
type grpcConn struct {
	conn *grpc.ClientConn
}

func FromGRPCConn(conn *grpc.ClientConn) RPCConn {
	return &grpcConn{conn: conn}
}

func (c *grpcConn) NewStatusClient() serverpb.StatusClient {
	return serverpb.NewStatusClient(c.conn)
}

func (c *grpcConn) NewAdminClient() serverpb.AdminClient {
	return serverpb.NewAdminClient(c.conn)
}

func (c *grpcConn) NewInitClient() serverpb.InitClient {
	return serverpb.NewInitClient(c.conn)
}

func (c *grpcConn) NewTimeSeriesClient() tspb.TimeSeriesClient {
	return tspb.NewTimeSeriesClient(c.conn)
}

func (c *grpcConn) NewInternalClient() kvpb.InternalClient {
	return kvpb.NewInternalClient(c.conn)
}

func (c *grpcConn) NewDistSQLClient() execinfrapb.DistSQLClient {
	return execinfrapb.NewDistSQLClient(c.conn)
}
