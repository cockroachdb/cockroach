// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"math"
	"net"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/drpcinterceptor"
	"github.com/cockroachdb/errors"
	"storj.io/drpc"
	"storj.io/drpc/drpcclient"
	"storj.io/drpc/drpcmanager"
	"storj.io/drpc/drpcmigrate"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcpool"
	"storj.io/drpc/drpcserver"
	"storj.io/drpc/drpcwire"
)

// Default idle connection timeout for DRPC connections in the pool.
var defaultDRPCConnIdleTimeout = 5 * time.Minute

type drpcCloseNotifier struct {
	conn drpc.Conn
}

func (d *drpcCloseNotifier) CloseNotify(ctx context.Context) <-chan struct{} {
	return d.conn.Closed()
}

// TODO(server): unexport this once dial methods are added in rpccontext.
func DialDRPC(
	rpcCtx *Context,
) func(ctx context.Context, target string, class rpcbase.ConnectionClass) (drpc.Conn, error) {
	return func(ctx context.Context, target string, class rpcbase.ConnectionClass) (drpc.Conn, error) {
		transport := tcpTransport
		if rpcCtx.ContextOptions.AdvertiseAddr == target && rpcCtx.canLoopbackDial() {
			transport = loopbackTransport
		}
		drpcDialOptions, err := rpcCtx.drpcDialOptionsInternal(ctx, target, class, transport)
		if err != nil {
			return nil, err
		}

		// TODO(server): could use connection class instead of empty key here.
		pool := drpcpool.New[struct{}, drpcpool.Conn](drpcpool.Options{
			Expiration: defaultDRPCConnIdleTimeout,
		})
		pooledConn := pool.Get(ctx /* unused */, struct{}{}, func(ctx context.Context,
			_ struct{}) (drpcpool.Conn, error) {
			return drpcclient.DialContext(ctx, target, drpcDialOptions...)
		})

		// The passed in drpc connection can be either a concrete connection or
		// a pooled connection.
		clientConn, err := drpcclient.NewClientConnWithOptions(ctx, pooledConn, drpcDialOptions...)
		if err != nil {
			return nil, err
		}

		// Wrap the clientConn to ensure the entire pool is closed when this connection handle is closed.
		return &closeEntirePoolConn{
			Conn: clientConn,
			pool: pool,
		}, nil
	}
}

// drpcDialOptionsInternal is similar to grpcDialOptionsInternal but for
// drpc connections.
func (rpcCtx *Context) drpcDialOptionsInternal(
	ctx context.Context, target string, class rpcbase.ConnectionClass, transport transportType,
) ([]drpcclient.DialOption, error) {
	drpcDialOpts, err := rpcCtx.drpcDialOptsCommon(ctx, target, class)
	if err != nil {
		return nil, err
	}

	switch transport {
	case tcpTransport:
		netOpts, err := rpcCtx.drpcDialOptsNetwork(ctx, target, class)
		if err != nil {
			return nil, err
		}
		drpcDialOpts = append(drpcDialOpts, netOpts...)
	case loopbackTransport:
		localOpts, err := rpcCtx.drpcDialOptsLocal()
		if err != nil {
			return nil, err
		}
		drpcDialOpts = append(drpcDialOpts, localOpts...)
	default:
		// This panic in case the type is ever changed to include more values.
		panic(errors.AssertionFailedf("unhandled: %v", transport))
	}
	return drpcDialOpts, nil
}

// drpcDialOptsCommon is same as dialOptsCommon but for drpc connections.
func (rpcCtx *Context) drpcDialOptsCommon(
	ctx context.Context, target string, class rpcbase.ConnectionClass,
) ([]drpcclient.DialOption, error) {
	drpcDialOpts := []drpcclient.DialOption{}
	if !rpcCtx.TenantID.IsSystem() {
		key, value := newPerRPCTIDMetdata(rpcCtx.TenantID)
		drpcDialOpts = append(drpcDialOpts, drpcclient.WithPerRPCMetadata(map[string]string{key: value}))
	}

	unaryInterceptors := rpcCtx.clientUnaryInterceptorsDRPC
	if rpcCtx.Knobs.UnaryClientInterceptorDRPC != nil {
		interceptor := rpcCtx.Knobs.UnaryClientInterceptorDRPC(target, rpcbase.DefaultClass)
		if interceptor != nil {
			unaryInterceptors = append(unaryInterceptors, interceptor)
		}
	}

	drpcDialOpts = append(drpcDialOpts, drpcclient.WithChainUnaryInterceptor(unaryInterceptors...))

	streamInterceptors := rpcCtx.clientStreamInterceptorsDRPC
	if rpcCtx.Knobs.StreamClientInterceptorDRPC != nil {
		interceptor := rpcCtx.Knobs.StreamClientInterceptorDRPC(target, rpcbase.DefaultClass)
		if interceptor != nil {
			streamInterceptors = append(streamInterceptors, interceptor)
		}
	}
	drpcDialOpts = append(drpcDialOpts, drpcclient.WithChainStreamInterceptor(streamInterceptors...))

	return drpcDialOpts, nil
}

// drpcDialOptsLocal is simialar to dialOptsLocal but for drpc connections.
func (rpcCtx *Context) drpcDialOptsLocal() ([]drpcclient.DialOption, error) {
	drpcDialOpts, err := rpcCtx.drpcDialOptsNetworkCredentials()
	if err != nil {
		return nil, err
	}

	drpcDialOpts = append(drpcDialOpts, drpcclient.WithContextDialer(
		func(ctx context.Context, target string) (net.Conn, error) {
			return rpcCtx.loopbackDRPCDialFn(ctx)
		},
	))

	return drpcDialOpts, err
}

// drpcDialOptsNetworkCredentials is same as dialOptsNetworkCredentials but for drpc connections.
func (rpcCtx *Context) drpcDialOptsNetworkCredentials() ([]drpcclient.DialOption, error) {
	drpcDialOpts := []drpcclient.DialOption{}
	if !rpcCtx.ContextOptions.Insecure {
		tlsConfig, err := rpcCtx.GetClientTLSConfig()
		if err != nil {
			return nil, err
		}
		drpcDialOpts = append(drpcDialOpts, drpcclient.WithTLSConfig(tlsConfig))
	}

	return drpcDialOpts, nil
}

// drpcDialOptsNetwork is same as dialOptsNetwork but for drpc connections.
func (rpcCtx *Context) drpcDialOptsNetwork(
	ctx context.Context, target string, class rpcbase.ConnectionClass,
) ([]drpcclient.DialOption, error) {
	// TODO(server): add compression support to drpc.
	// TODO(server): add support for dial timeout.
	// TODO(server): check if onlyOnceDialer is needed for drpc.

	drpcDialOpts, err := rpcCtx.drpcDialOptsNetworkCredentials()
	if err != nil {
		return nil, err
	}

	dialerFunc := func(ctx context.Context, target string) (net.Conn, error) {
		return drpcmigrate.DialWithHeader(ctx, "tcp", target, drpcmigrate.DRPCHeader)
	}
	if rpcCtx.Knobs.InjectedLatencyOracle != nil {
		latency := rpcCtx.Knobs.InjectedLatencyOracle.GetLatency(target)
		log.VEventf(ctx, 1, "connecting with simulated latency %dms",
			latency)
		dialer := artificialLatencyDialer{
			dialerFunc: dialerFunc,
			latency:    latency,
			enabled:    rpcCtx.Knobs.InjectedLatencyEnabled,
		}
		dialerFunc = dialer.dial
	}
	drpcDialOpts = append(drpcDialOpts, drpcclient.WithContextDialer(dialerFunc))

	return drpcDialOpts, nil
}

type closeEntirePoolConn struct {
	closeOnce sync.Once
	drpc.Conn
	pool *drpcpool.Pool[struct{}, drpcpool.Conn]
}

func (c *closeEntirePoolConn) Close() (err error) {
	// TODO(server): make drpc pooledConn.Close() idempotent
	c.closeOnce.Do(func() {
		_ = c.Conn.Close()
		err = c.pool.Close()
	})
	return err
}

type DRPCConnection = Connection[drpc.Conn]

// ErrDRPCDisabled is returned from hosts that in principle could but do not
// have the DRPC server enabled.
var ErrDRPCDisabled = errors.New("DRPC is not enabled")

// DRPCServer defines the interface for a DRPC server, which can serve
// connections and provides a drpc.Mux for registering handlers.
type DRPCServer interface {
	// Serve starts serving DRPC requests on the provided listener.
	Serve(ctx context.Context, lis net.Listener) error
	drpc.Mux
}

// drpcServer implements the DRPCServer interface.
type drpcServer struct {
	*drpcserver.Server
	drpc.Mux
}

// makeStopperInterceptors returns unary and stream interceptors that run
// incoming RPCs in stopper tasks.
func makeStopperInterceptors(
	rpcCtx *Context,
) (drpcmux.UnaryServerInterceptor, drpcmux.StreamServerInterceptor) {
	unary := func(
		ctx context.Context, req interface{}, rpc string, handler drpcmux.UnaryHandler,
	) (interface{}, error) {
		var resp interface{}
		if err := rpcCtx.Stopper.RunTaskWithErr(ctx, rpc, func(ctx context.Context) error {
			var err error
			resp, err = handler(ctx, req)
			return err
		}); err != nil {
			return nil, err
		}
		return resp, nil
	}

	stream := func(
		stream drpc.Stream, rpc string, handler drpcmux.StreamHandler,
	) (interface{}, error) {
		var resp interface{}
		if err := rpcCtx.Stopper.RunTaskWithErr(stream.Context(), rpc, func(ctx context.Context) error {
			var err error
			resp, err = handler(stream)
			return err
		}); err != nil {
			return nil, err
		}
		return resp, nil
	}
	return unary, stream
}

// NewDRPCServer creates a new DRPCServer with the provided rpc context.
func NewDRPCServer(_ context.Context, rpcCtx *Context, opts ...ServerOption) (DRPCServer, error) {
	d := &drpcServer{}

	var o serverOpts
	for _, f := range opts {
		f(&o)
	}

	var unaryInterceptors []drpcmux.UnaryServerInterceptor
	var streamInterceptors []drpcmux.StreamServerInterceptor

	// These interceptors run in the order they're appended. The first
	// interceptor added becomes the outermost wrapper around the handler.

	// We start with an interceptor that ensures every RPC executes inside a
	// stopper task. Running the handler in a stopper task lets the stopper
	// keep track of in-flight RPCs and reject new ones once draining begins.
	stopUnary, stopStream := makeStopperInterceptors(rpcCtx)
	unaryInterceptors = append(unaryInterceptors, stopUnary)
	streamInterceptors = append(streamInterceptors, stopStream)

	// Recover from any uncaught panics caused by DB Console requests.
	unaryInterceptors = append(unaryInterceptors, drpcGatewayRequestRecoveryInterceptor)

	// If the metrics interceptor is set, it should be registered second so
	// that all other interceptors are included in the response time durations.
	if o.drpcUnaryRequestMetricsInterceptor != nil {
		unaryInterceptors = append(unaryInterceptors,
			drpcmux.UnaryServerInterceptor(o.drpcUnaryRequestMetricsInterceptor))
	}
	if o.drpcStreamRequestMetricsInterceptor != nil {
		streamInterceptors = append(streamInterceptors,
			drpcmux.StreamServerInterceptor(o.drpcStreamRequestMetricsInterceptor))
	}

	if !rpcCtx.ContextOptions.Insecure {
		a := kvAuth{
			sv: &rpcCtx.Settings.SV,
			tenant: tenantAuthorizer{
				tenantID:               rpcCtx.tenID,
				capabilitiesAuthorizer: rpcCtx.capabilitiesAuthorizer,
			},
			isDRPC: true,
		}

		unaryInterceptors = append(unaryInterceptors, a.AuthDRPCUnary())
		streamInterceptors = append(streamInterceptors, a.AuthDRPCStream())
	}

	if o.interceptor != nil {
		unaryInterceptors = append(unaryInterceptors, func(
			ctx context.Context, req interface{}, fullMethod string, handler drpcmux.UnaryHandler,
		) (interface{}, error) {
			if err := o.interceptor(fullMethod); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		})

		streamInterceptors = append(streamInterceptors, func(
			stream drpc.Stream, fullMethod string, handler drpcmux.StreamHandler,
		) (interface{}, error) {
			if err := o.interceptor(fullMethod); err != nil {
				return nil, err
			}
			return handler(stream)
		})
	}

	if tracer := rpcCtx.Stopper.Tracer(); tracer != nil {
		unaryInterceptors = append(unaryInterceptors, drpcinterceptor.ServerInterceptor(tracer))
		streamInterceptors = append(streamInterceptors, drpcinterceptor.StreamServerInterceptor(tracer))
	}

	mux := drpcmux.NewWithInterceptors(unaryInterceptors, streamInterceptors)

	d.Server = drpcserver.NewWithOptions(mux, drpcserver.Options{
		Log: func(err error) {
			log.Dev.Warningf(context.Background(), "drpc server error %v", err)
		},
		// The reader's max buffer size defaults to 4mb, and if it is exceeded (such
		// as happens with AddSSTable) the RPCs fail.
		Manager: drpcmanager.Options{
			Reader: drpcwire.ReaderOptions{MaximumBufferSize: math.MaxInt},
			// Enable grpc compabitility for metadata
			GRPCMetadataCompatMode: true,
		},
	})
	d.Mux = mux

	return d, nil
}

// NewDummyDRPCServer returns a DRPCServer that is disabled and always returns
// ErrDRPCDisabled.
func NewDummyDRPCServer() DRPCServer {
	return &drpcOffServer{}
}

// drpcOffServer is a disabled DRPC server implementation. It immediately closes
// accepted connections and returns ErrDRPCDisabled for all Serve calls.
// Register is a no-op.
type drpcOffServer struct{}

// Serve implements the DRPCServer interface for drpcOffServer. It closes any
// accepted connection and returns ErrDRPCDisabled.
func (srv *drpcOffServer) Serve(_ context.Context, lis net.Listener) error {
	conn, err := lis.Accept()
	if err != nil {
		return err
	}
	_ = conn.Close()
	return ErrDRPCDisabled
}

// Register implements the drpc.Mux interface for drpcOffServer. It is a no-op
// when DRPC is disabled.
func (srv *drpcOffServer) Register(interface{}, drpc.Description) error {
	return nil
}

// newDRPDCPeerOptions creates peerOptions for DRPC peers.
func newDRPCPeerOptions(
	rpcCtx *Context, k peerKey, locality roachpb.Locality,
) *peerOptions[drpc.Conn] {
	pm, _ := rpcCtx.metrics.acquire(k, locality, rpcProtocolDRPC)
	return &peerOptions[drpc.Conn]{
		locality: locality,
		peers:    &rpcCtx.drpcPeers,
		connOptions: &ConnectionOptions[drpc.Conn]{
			dial: DialDRPC(rpcCtx),
			connEquals: func(a, b drpc.Conn) bool {
				return a == b
			},
			newBatchStreamClient: func(ctx context.Context, cc drpc.Conn) (BatchStreamClient, error) {
				return kvpb.NewDRPCKVBatchClientAdapter(cc).BatchStream(ctx)
			},
			newCloseNotifier: func(_ *stop.Stopper, cc drpc.Conn) closeNotifier {
				return &drpcCloseNotifier{conn: cc}
			},
		},
		newHeartbeatClient: func(cc drpc.Conn) RPCHeartbeatClient {
			return NewDRPCHeartbeatClientAdapter(cc)
		},
		pm: pm,
	}
}
