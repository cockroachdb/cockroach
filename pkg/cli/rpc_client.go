// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
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
	conn, finish, err := getClientGRPCConn(ctx, cfg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to connect to the node")
	}
	return serverpb.NewStatusClient(conn), finish, nil
}
