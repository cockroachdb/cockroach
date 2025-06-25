// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"storj.io/drpc"
)

// rpcConn defines a common interface for creating RPC clients. It hides the
// underlying RPC connection (gRPC or DRPC), making it easy to swap
// them without changing the caller code.
type rpcConn interface {
	NewStatusClient() serverpb.RPCStatusClient
	NewAdminClient() serverpb.RPCAdminClient
	NewInitClient() serverpb.RPCInitClient
	NewTimeSeriesClient() tspb.RPCTimeSeriesClient
	NewInternalClient() kvpb.RPCInternalClient
}

// grpcConn is an implementation of rpcConn that provides methods to create
// various RPC clients. This allows the CLI to interact with the server using
// gRPC without exposing the underlying connection details.
type grpcConn struct {
	conn *grpc.ClientConn
}

func (c *grpcConn) NewStatusClient() serverpb.RPCStatusClient {
	return serverpb.NewGRPCStatusClientAdapter(c.conn)
}

func (c *grpcConn) NewAdminClient() serverpb.RPCAdminClient {
	return serverpb.NewGRPCAdminClientAdapter(c.conn)
}

func (c *grpcConn) NewInitClient() serverpb.RPCInitClient {
	return serverpb.NewGRPCInitClientAdapter(c.conn)
}

func (c *grpcConn) NewTimeSeriesClient() tspb.RPCTimeSeriesClient {
	return tspb.NewGRPCTimeSeriesClientAdapter(c.conn)
}

func (c *grpcConn) NewInternalClient() kvpb.RPCInternalClient {
	return kvpb.NewGRPCInternalClientAdapter(c.conn)
}

// drpcConn is an implementation of rpcConn that provides methods to create
// various RPC clients. This allows the CLI to interact with the server using
// DRPC without exposing the underlying connection details.
type drpcConn struct {
	conn drpc.Conn
}

func (c *drpcConn) NewStatusClient() serverpb.RPCStatusClient {
	return serverpb.NewDRPCStatusClientAdapter(c.conn)
}

func (c *drpcConn) NewAdminClient() serverpb.RPCAdminClient {
	return serverpb.NewDRPCAdminClientAdapter(c.conn)
}

func (c *drpcConn) NewInitClient() serverpb.RPCInitClient {
	return serverpb.NewDRPCInitClientAdapter(c.conn)
}

func (c *drpcConn) NewTimeSeriesClient() tspb.RPCTimeSeriesClient {
	return tspb.NewDRPCTimeSeriesClientAdapter(c.conn)
}

func (c *drpcConn) NewInternalClient() kvpb.RPCInternalClient {
	return kvpb.NewDRPCInternalClientAdapter(c.conn)
}

func makeRPCClientConfig(cfg server.Config) rpc.ClientConnConfig {
	var knobs rpc.ContextTestingKnobs
	if sknobs := cfg.TestingKnobs.Server; sknobs != nil {
		knobs = sknobs.(*server.TestingKnobs).ContextTestingKnobs
	}
	return rpc.MakeClientConnConfigFromBaseConfig(
		*cfg.Config,
		cfg.Config.User,
		cfg.BaseConfig.Tracer,
		cfg.BaseConfig.Settings,
		nil, /* clock */
		knobs,
	)
}

func newClientConn(ctx context.Context, cfg server.Config) (rpcConn, func(), error) {
	ccfg := makeRPCClientConfig(cfg)
	if !rpcbase.TODODRPC {
		cc, finish, err := rpc.NewClientConn(ctx, ccfg)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to connect to the node")
		}
		return &grpcConn{conn: cc}, finish, nil
	}
	dc, finish, err := rpc.NewDRPCClientConn(ctx, ccfg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to connect to the node")
	}
	return &drpcConn{conn: dc}, finish, nil
}

// dialAdminClient dials a client connection and returns an AdminClient and a
// closure that must be invoked to free associated resources.
func dialAdminClient(
	ctx context.Context, cfg server.Config,
) (serverpb.RPCAdminClient, func(), error) {
	cc, finish, err := newClientConn(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	return cc.NewAdminClient(), finish, nil
}
