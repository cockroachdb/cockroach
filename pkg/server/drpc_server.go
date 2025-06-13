// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"crypto/tls"
	"math"
	"net"

	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"storj.io/drpc"
	"storj.io/drpc/drpcerr"
	"storj.io/drpc/drpcmanager"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
	"storj.io/drpc/drpcwire"
)

// ErrDRPCDisabled is returned from hosts that in principle could but do not
// have the DRPC server enabled.
var ErrDRPCDisabled = errors.New("DRPC is not enabled")

type drpcServerI interface {
	Serve(ctx context.Context, lis net.Listener) error
}

type drpcMuxI interface {
	Register(srv interface{}, desc drpc.Description) error
}

type drpcServer struct {
	serveModeHandler
	srv                    drpcServerI
	mux                    drpcMuxI
	tlsCfg                 *tls.Config
	enabled                bool
	serverInterceptorsInfo []drpcserver.ServerInterceptor
}

var _ drpcServerI = (*drpcserver.Server)(nil)
var _ drpcServerI = (*drpcOffServer)(nil)

// TODO: Register DRPC Heartbeat service
func newDRPCServer(
	_ context.Context, rpcCtx *rpc.Context, metricsRegistry *metric.Registry,
) (*drpcServer, error) {
	d := &drpcServer{}
	d.mode.set(modeInitializing) // Set initial mode for interceptors

	var dsrv drpcServerI = &drpcOffServer{}
	var dmux drpcMuxI = &drpcOffServer{}
	var tlsCfg *tls.Config
	enabled := false
	var serverInterceptors []drpcserver.ServerInterceptor

	if rpc.ExperimentalDRPCEnabled.Get(&rpcCtx.Settings.SV) {
		enabled = true

		// 1. Stopper Interceptor
		serverInterceptors = append(serverInterceptors, drpcStopperInterceptor(rpcCtx))

		// 2. Metrics Interceptor
		requestMetrics := rpc.NewRequestMetrics()
		if metricsRegistry != nil {
			metricsRegistry.AddMetricStruct(requestMetrics)
		}
		metricsRecorder := &rpc.CommonMetricsRecorder{
			RequestMetrics: requestMetrics,
			ShouldRecord: func(method string) bool {
				// reusing the same feature flag as used in GRPC server here.
				// Idea is, if metric collection is enabled for GRPC, it should also be enabled for DRPC
				// for DRPC server to be on par with GRPC server in terms of metrics.
				return shouldRecordRequestDuration(rpcCtx.Settings, method)
			},
		}
		serverInterceptors = append(serverInterceptors, metricsRecorder.NewDRPCInterceptor())

		// 3. Access Control Interceptor
		accessControlInterceptor := func(
			ictx context.Context,
			methodName string,
			stream drpc.Stream,
			handler drpc.Handler,
		) error {
			if err := d.intercept(methodName); err != nil {
				return err
			}
			return handler.HandleRPC(stream, methodName)
		}
		serverInterceptors = append(serverInterceptors, accessControlInterceptor)

		mux := drpcmux.New()
		dsrv = drpcserver.NewWithOptions(mux, drpcserver.Options{
			Log: func(err error) {
				log.Warningf(context.Background(), "drpc server error %v", err)
			},
			// The reader's max buffer size defaults to 4mb, and if it is exceeded (such
			// as happens with AddSSTable) the RPCs fail.
			Manager: drpcmanager.Options{Reader: drpcwire.ReaderOptions{MaximumBufferSize: math.MaxInt}},
		}, drpcserver.WithChainServerInterceptor(serverInterceptors...))
		dmux = mux

		var err error
		tlsCfg, err = rpcCtx.GetServerTLSConfig()
		if err != nil {
			return nil, err
		}

	}

	d.srv = dsrv
	d.mux = dmux
	d.tlsCfg = tlsCfg
	d.enabled = enabled
	d.serverInterceptorsInfo = serverInterceptors
	return d, nil
}

// drpcOffServer is used for drpcServerI and drpcMuxI if the DRPC server is
// disabled. It immediately closes accepted connections and returns
// ErrDRPCDisabled.
type drpcOffServer struct{}

func (srv *drpcOffServer) Serve(_ context.Context, lis net.Listener) error {
	conn, err := lis.Accept()
	if err != nil {
		return err
	}
	_ = conn.Close()
	return ErrDRPCDisabled
}

func (srv *drpcOffServer) Register(interface{}, drpc.Description) error {
	return nil
}

func (s *drpcServer) health(ctx context.Context) error {
	sm := s.mode.get()
	switch sm {
	case modeInitializing:
		return drpcerr.WithCode(errors.New("node is waiting for cluster initialization"), uint64(codes.Unavailable))
	case modeDraining:
		return drpcerr.WithCode(errors.New("node is shutting down"), uint64(codes.Unavailable))
	case modeOperational:
		return nil
	default:
		return srverrors.ServerError(ctx, errors.Newf("unknown mode: %v", sm))
	}
}

// intercept implements filtering rules for each server state for dRPC.
// It is analogous to grpcServer.intercept.
func (s *drpcServer) intercept(fullName string) error {
	return intercept(&s.serveModeHandler, fullName, newDRPCWaitingForInitError)
}

// newDRPCWaitingForInitError creates a dRPC-specific error indicating that the
// server cannot run the specified method until the node has been initialized.
func newDRPCWaitingForInitError(methodName string) error {
	return drpcerr.WithCode(errors.Newf("node waiting for init; %s not available", methodName), uint64(codes.Unavailable))
}

// drpcStopperInterceptor wraps the dRPC handler execution in a Stopper task.
func drpcStopperInterceptor(rpcCtx *rpc.Context) drpcserver.ServerInterceptor {
	return func(ctx context.Context, rpcName string, stream drpc.Stream, handler drpc.Handler) error {
		return rpcCtx.Stopper.RunTaskWithErr(stream.Context(), rpcName, func(taskCtx context.Context) error {
			// Here, we align with how gRPC interceptor uses its context i.e. we pass stream.Context() to the task.
			// The original handler.HandleRPC will manage sending responses/errors through the stream.
			return handler.HandleRPC(stream, rpcName)
		})
	}
}
