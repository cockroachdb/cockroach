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
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) (MigrationClient, error) {
	if !rpcbase.TODODRPC {
		conn, err := nd.Dial(ctx, nodeID, class)
		if err != nil {
			return nil, err
		}
		return NewMigrationClient(conn), nil
	}
	return nil, nil
}

// DialStatusClientNoBreaker establishes a DRPC connection if enabled;
// otherwise, it falls back to gRPC. The established connection is used
// to create a StatusClient. This method is same as DialStatusClient, but it
// does not check the breaker before dialing the connection.
func DialStatusClientNoBreaker(
	nd rpcbase.NodeDialerNoBreaker,
	ctx context.Context,
	nodeID roachpb.NodeID,
	class rpcbase.ConnectionClass,
) (StatusClient, error) {
	if !rpcbase.TODODRPC {
		conn, err := nd.DialNoBreaker(ctx, nodeID, class)
		if err != nil {
			return nil, err
		}
		return NewStatusClient(conn), nil
	}
	return nil, nil
}

// DialStatusClient establishes a DRPC connection if enabled; otherwise, it
// falls back to gRPC. The established connection is used to create a
// StatusClient.
func DialStatusClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID,
) (StatusClient, error) {
	if !rpcbase.TODODRPC {
		conn, err := nd.Dial(ctx, nodeID, rpcbase.DefaultClass)
		if err != nil {
			return nil, err
		}
		return NewStatusClient(conn), nil
	}
	return nil, nil
}

// DialAdminClient establishes a DRPC connection if enabled; otherwise, it
// falls back to gRPC. The established connection is used to create a
// AdminClient.
func DialAdminClient(
	nd rpcbase.NodeDialer, ctx context.Context, nodeID roachpb.NodeID,
) (AdminClient, error) {
	if !rpcbase.TODODRPC {
		conn, err := nd.Dial(ctx, nodeID, rpcbase.DefaultClass)
		if err != nil {
			return nil, err
		}
		return NewAdminClient(conn), nil
	}
	return nil, nil
}

// DialAdminClientNoBreaker establishes a DRPC connection if enabled;
// otherwise, it falls back to gRPC. The established connection is used to
// create a AdminClient. This method is same as DialAdminClient, but it
// does not check the breaker before dialing the connection.
func DialAdminClientNoBreaker(
	nd rpcbase.NodeDialerNoBreaker, ctx context.Context, nodeID roachpb.NodeID,
) (AdminClient, error) {
	if !rpcbase.TODODRPC {
		conn, err := nd.DialNoBreaker(ctx, nodeID, rpcbase.DefaultClass)
		if err != nil {
			return nil, err
		}
		return NewAdminClient(conn), nil
	}
	return nil, nil
}
