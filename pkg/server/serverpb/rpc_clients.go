// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverpb

import (
	context "context"

	roachpb "github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
)

// DialMigrationClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// MigrationClient.
func DialMigrationClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID,
) (MigrationClient, error) {
	if !rpcbase.TODODRPC {
		conn, err := nd.Dial(ctx, nodeID)
		if err != nil {
			return nil, err
		}
		return NewMigrationClient(conn), nil
	}
	return nil, nil
}

// DialAdminClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// AdminClient.
func DialAdminClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, dialOpts ...rpcbase.DialOption,
) (AdminClient, error) {
	if !rpcbase.TODODRPC {
		conn, err := nd.Dial(ctx, nodeID, dialOpts...)
		if err != nil {
			return nil, err
		}
		return NewAdminClient(conn), nil
	}
	return nil, nil
}

// DialStatusClient establishes a DRPC connection if enabled; otherwise,
// it falls back to gRPC. The established connection is used to create a
// StatusClient.
func DialStatusClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, dialOpts ...rpcbase.DialOption,
) (StatusClient, error) {
	if !rpcbase.TODODRPC {
		conn, err := nd.Dial(ctx, nodeID, dialOpts...)
		if err != nil {
			return nil, err
		}
		return NewStatusClient(conn), nil
	}
	return nil, nil
}
