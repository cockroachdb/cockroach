// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package future

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DialNodeClients creates gRPC clients for the Admin and Status services
// by dialing a specific node using the CockroachDB RPC infrastructure.
// This is the preferred method for dialing nodes within a CockroachDB cluster.
//
// This follows the same pattern as NewAPIInternalServer in pkg/server/apiinternal,
// but wraps the connection in regular client types that include grpc.CallOption support.
func DialNodeClients(
	ctx context.Context, nd rpcbase.NodeDialer, nodeID roachpb.NodeID, cs *cluster.Settings,
) (serverpb.AdminClient, serverpb.StatusClient, error) {
	// Dial the node using the default connection class
	conn, err := nd.Dial(ctx, nodeID, rpcbase.DefaultClass)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to dial node")
	}

	// Create regular client wrappers that support grpc.CallOption
	adminClient := serverpb.NewAdminClient(conn)
	statusClient := serverpb.NewStatusClient(conn)

	return adminClient, statusClient, nil
}

// DialRemoteClients creates gRPC clients for the Admin and Status services
// by dialing a remote address using simple gRPC dial.
// This is a simplified version for external tools that don't have access to
// the CockroachDB RPC infrastructure. For internal use, prefer DialNodeClients.
func DialRemoteClients(
	ctx context.Context, address string,
) (serverpb.AdminClient, serverpb.StatusClient, error) {
	// Dial the remote server with necessary options for CockroachDB
	// Include the max message size options that CockroachDB uses internally
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(1<<31 - 1), // math.MaxInt32
			grpc.MaxCallSendMsgSize(1<<31 - 1), // math.MaxInt32
		),
	}

	conn, err := grpc.DialContext(ctx, address, dialOpts...)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to dial remote server")
	}

	// Create clients from the connection
	adminClient := serverpb.NewAdminClient(conn)
	statusClient := serverpb.NewStatusClient(conn)

	return adminClient, statusClient, nil
}
