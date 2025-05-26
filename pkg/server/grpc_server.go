// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// grpcServer is a wrapper on top of a grpc.Server that includes an interceptor
// and a mode of operation that can instruct the interceptor to refuse certain
// RPCs.
type grpcServer struct {
	serveModeHandler
	*grpc.Server
	serverInterceptorsInfo rpc.ServerInterceptorInfo
}

func newGRPCServer(
	ctx context.Context, rpcCtx *rpc.Context, metricsRegistry *metric.Registry,
) (*grpcServer, error) {
	s := &grpcServer{}
	s.mode.set(modeInitializing)
	requestMetrics := rpc.NewRequestMetrics()
	metricsRegistry.AddMetricStruct(requestMetrics)
	srv, interceptorInfo, err := rpc.NewServerEx(
		ctx, rpcCtx, rpc.WithInterceptor(func(path string) error {
			return s.intercept(path)
		}), rpc.WithMetricsServerInterceptor(
			rpc.NewRequestMetricsInterceptor(requestMetrics, func(method string) bool {
				return shouldRecordRequestDuration(rpcCtx.Settings, method)
			},
			)))
	if err != nil {
		return nil, err
	}
	s.Server = srv
	s.serverInterceptorsInfo = interceptorInfo
	return s, nil
}

func (s *grpcServer) health(ctx context.Context) error {
	sm := s.mode.get()
	switch sm {
	case modeInitializing:
		return grpcstatus.Error(codes.Unavailable, "node is waiting for cluster initialization")
	case modeDraining:
		// grpc.mode is set to modeDraining when the Drain(DrainMode_CLIENT) has
		// been called (client connections are to be drained).
		return grpcstatus.Errorf(codes.Unavailable, "node is shutting down")
	case modeOperational:
		return nil
	default:
		return srverrors.ServerError(ctx, errors.Newf("unknown mode: %v", sm))
	}
}

var rpcsAllowedWhileBootstrapping = map[string]struct{}{
	"/cockroach.rpc.Heartbeat/Ping":             {},
	"/cockroach.gossip.Gossip/Gossip":           {},
	"/cockroach.server.serverpb.Init/Bootstrap": {},
	"/cockroach.server.serverpb.Admin/Health":   {},
}

// intercept implements filtering rules for each server state.
func (s *grpcServer) intercept(fullName string) error {
	if s.operational() {
		return nil
	}
	if _, allowed := rpcsAllowedWhileBootstrapping[fullName]; !allowed {
		return NewWaitingForInitError(fullName)
	}
	return nil
}

// NewWaitingForInitError creates an error indicating that the server cannot run
// the specified method until the node has been initialized.
func NewWaitingForInitError(methodName string) error {
	// NB: this error string is sadly matched in grpcutil.IsWaitingForInit().
	return grpcstatus.Errorf(codes.Unavailable, "node waiting for init; %s not available", methodName)
}

const (
	serverPrefix = "/cockroach.server"
	tsdbPrefix   = "/cockroach.ts"
)

// serverGRPCRequestMetricsEnabled is a cluster setting that enables the
// collection of gRPC request duration metrics. This uses export only
// metrics so the metrics are only exported to external sources such as
// /_status/vars and DataDog.
var serverGRPCRequestMetricsEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"server.grpc.request_metrics.enabled",
	"enables the collection of grpc metrics",
	false,
)

func shouldRecordRequestDuration(settings *cluster.Settings, method string) bool {
	return serverGRPCRequestMetricsEnabled.Get(&settings.SV) &&
		(strings.HasPrefix(method, serverPrefix) ||
			strings.HasPrefix(method, tsdbPrefix))
}
