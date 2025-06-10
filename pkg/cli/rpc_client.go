// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

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

func getClientGRPCConn(ctx context.Context, cfg server.Config) (*grpc.ClientConn, func(), error) {
	ccfg := makeRPCClientConfig(cfg)
	return rpc.NewClientConn(ctx, ccfg)
}

// getAdminClient returns an AdminClient and a closure that must be invoked
// to free associated resources.
func getAdminClient(ctx context.Context, cfg server.Config) (serverpb.AdminClient, func(), error) {
	conn, finish, err := getClientGRPCConn(ctx, cfg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to connect to the node")
	}
	return serverpb.NewAdminClient(conn), finish, nil
}

// getStatusClient returns a StatusClient and a closure that must be invoked
// to free associated resources.
func getStatusClient(
	ctx context.Context, cfg server.Config,
) (serverpb.StatusClient, func(), error) {
	if !rpcbase.TODODRPC {
		conn, finish, err := getClientGRPCConn(ctx, cfg)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to connect to the node")
		}
		return serverpb.NewStatusClient(conn), finish, nil
	}
	return nil, nil, errors.New("DRPC not supported")
}

// client contains common RPC clients command-line tools need to interact
// with the server.
type client struct {
	statusClient     serverpb.StatusClient
	adminClient      serverpb.AdminClient
	initClient       serverpb.InitClient
	timeSeriesClient tspb.TimeSeriesClient
}

// getClients return common RPC clients command-line tools need to
// interact with the server. It also returns a closure that must be invoked
// to free associated resources.
func getClients(ctx context.Context, cfg server.Config) (*client, func(), error) {
	if !rpcbase.TODODRPC {
		conn, finish, err := getClientGRPCConn(ctx, cfg)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to connect to the node")
		}

		return &client{
			statusClient:     serverpb.NewStatusClient(conn),
			adminClient:      serverpb.NewAdminClient(conn),
			initClient:       serverpb.NewInitClient(conn),
			timeSeriesClient: tspb.NewTimeSeriesClient(conn),
		}, finish, nil
	}
	return nil, nil, errors.New("DRPC not supported")
}
