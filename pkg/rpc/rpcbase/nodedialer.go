// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpcbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"storj.io/drpc"
)

var envExperimentalDRPCEnabled = envutil.EnvOrDefaultBool("COCKROACH_EXPERIMENTAL_DRPC_ENABLED", false)

// ExperimentalDRPCEnabled determines whether a drpc server accepting BatchRequest
// is enabled. This server is experimental and completely unsuitable to production
// usage (for example, does not implement authorization checks).
var ExperimentalDRPCEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"rpc.experimental_drpc.enabled",
	"if true, use drpc to execute Batch RPCs (instead of gRPC)",
	envExperimentalDRPCEnabled,
	settings.WithValidateBool(func(values *settings.Values, b bool) error {
		// drpc support is highly experimental and should not be enabled in production.
		// Since authorization is not implemented, we only even host the server if the
		// env var is set or it's a CRDB test build. Consequently, these are prereqs
		// for setting the cluster setting.
		if b && !(envExperimentalDRPCEnabled || buildutil.CrdbTestBuild) {
			return errors.New("experimental drpc is not allowed in this environment")
		}
		return nil
	}))

// TODODRPC is a marker to identify each RPC client creation site that needs to
// be updated to support DRPC.
const TODODRPC = false

// NodeDialer interface defines methods for dialing peer nodes using their
// node IDs.
type NodeDialer interface {
	Dial(context.Context, roachpb.NodeID, ConnectionClass) (_ *grpc.ClientConn, err error)
	DRPCDial(context.Context, roachpb.NodeID, ConnectionClass) (_ drpc.Conn, err error)
}

// NodeDialerNoBreaker interface defines methods for dialing peer nodes using their
// node IDs. This interface is similar to NodeDialer but does not check the
// breaker before dialing.
type NodeDialerNoBreaker interface {
	DialNoBreaker(context.Context, roachpb.NodeID, ConnectionClass) (_ *grpc.ClientConn, err error)
	DRPCDialNoBreaker(context.Context, roachpb.NodeID, ConnectionClass) (_ drpc.Conn, err error)
}
