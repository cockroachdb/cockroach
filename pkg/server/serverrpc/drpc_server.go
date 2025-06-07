// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverrpc

import (
	"context"
	"crypto/tls"
	"math"
	"net"

	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

// DRPCServer defines the interface for a DRPC server implementation.
type DRPCServer interface {
	// Serve starts serving DRPC requests on the given listener.
	Serve(ctx context.Context, lis net.Listener) error

	drpc.Mux // Embeds the DRPC multiplexer for service registration.

	// TLSCfg returns the TLS configuration used by the server.
	TLSCfg() *tls.Config

	// SetMode sets the current serving mode (e.g., initializing, operational, draining).
	SetMode(mode ServeMode)

	// GetMode returns the current serving mode.
	GetMode() ServeMode

	// Operational returns true if the server is in operational mode.
	Operational() bool

	// Health returns an error if the server is not healthy, nil otherwise.
	Health(ctx context.Context) error

	// Enabled returns true if the DRPC server is enabled.
	Enabled() bool
}

// TODO: Register DRPC Heartbeat service
func NewDRPCServer(_ context.Context, rpcCtx *rpc.Context) (DRPCServer, error) {
	if !rpc.ExperimentalDRPCEnabled.Get(&rpcCtx.Settings.SV) {
		return &drpcOffServer{}, nil
	}

	d := &drpcServer{}

	mux := drpcmux.New()
	d.Server = drpcserver.NewWithOptions(mux, drpcserver.Options{
		Log: func(err error) {
			log.Warningf(context.Background(), "drpc server error %v", err)
		},
		// The reader's max buffer size defaults to 4mb, and if it is exceeded (such
		// as happens with AddSSTable) the RPCs fail.
		Manager: drpcmanager.Options{Reader: drpcwire.ReaderOptions{MaximumBufferSize: math.MaxInt}},
	})
	d.Mux = mux

	tlsCfg, err := rpcCtx.GetServerTLSConfig()
	if err != nil {
		return nil, err
	}
	d.tlsCfg = tlsCfg

	// NB: any server middleware (server interceptors in gRPC parlance) would go
	// here:
	//     dmux = whateverMiddleware1(dmux)
	//     dmux = whateverMiddleware2(dmux)
	//     ...
	//
	// Each middleware must implement the Handler interface:
	//
	//   HandleRPC(stream Stream, rpc string) error
	//
	// where Stream
	// See here for an example:
	// https://github.com/bryk-io/pkg/blob/4da5fbfef47770be376e4022eab5c6c324984bf7/net/drpc/server.go#L91-L101

	d.SetMode(ModeInitializing)

	return d, nil
}

// drpcServer implements the DRPCServer interface and provides a fully functional
// DRPC server. It embeds the drpcserver.Server and drpcmux.Mux to handle DRPC
// requests and service registration.
type drpcServer struct {
	// ServeMode controls the current operational state of the server (e.g., initializing, operational, draining).
	ServeMode
	// Server is the underlying DRPC server implementation handling connections and requests.
	*drpcserver.Server
	// Mux is the DRPC multiplexer used for service registration and routing.
	*drpcmux.Mux
	// tlsCfg holds the TLS configuration for secure communication.
	tlsCfg *tls.Config
}

func (s *drpcServer) Enabled() bool       { return true }
func (s *drpcServer) TLSCfg() *tls.Config { return s.tlsCfg }

// Health checks the health of the DRPC server based on its current mode.
// Returns an error with an appropriate code if the server is not operational.
func (s *drpcServer) Health(ctx context.Context) error {
	sm := s.GetMode()
	switch sm {
	case ModeInitializing:
		return drpcerr.WithCode(errors.New("node is waiting for cluster initialization"), uint64(codes.Unavailable))
	case ModeDraining:
		return drpcerr.WithCode(errors.New("node is shutting down"), uint64(codes.Unavailable))
	case ModeOperational:
		return nil
	default:
		return srverrors.ServerError(ctx, errors.Newf("unknown mode: %v", sm))
	}
}

// drpcOffServer implements the DRPCServer interface for the case when the DRPC
// server is disabled. All methods either return default values or errors
// indicating that DRPC is not enabled. This type is used as a stub to satisfy
// the interface when DRPC functionality is turned off.
type drpcOffServer struct{}

// Serve immediately closes any accepted connection and returns ErrDRPCDisabled.
func (srv *drpcOffServer) Serve(_ context.Context, lis net.Listener) error {
	conn, err := lis.Accept()
	if err != nil {
		return err
	}
	_ = conn.Close()
	return ErrDRPCDisabled
}

// Register is a no-op for drpcOffServer and always returns nil.
func (srv *drpcOffServer) Register(interface{}, drpc.Description) error { return nil }

// TLSCfg returns nil as there is no TLS configuration for a disabled server.
func (srv *drpcOffServer) TLSCfg() *tls.Config { return nil }

// Enabled returns false, indicating the DRPC server is not enabled.
func (srv *drpcOffServer) Enabled() bool { return false }

// TODO(server): add a mode for N.A.
func (srv *drpcOffServer) GetMode() ServeMode { return ModeInitializing }

// SetMode is a no-op for drpcOffServer.
func (srv *drpcOffServer) SetMode(_ ServeMode) {}

// Operational always returns false for a disabled server.
func (srv *drpcOffServer) Operational() bool { return false }

// Health always returns ErrDRPCDisabled for a disabled server.
func (srv *drpcOffServer) Health(ctx context.Context) error { return ErrDRPCDisabled }
