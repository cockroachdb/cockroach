// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"hash/fnv"
	"io"
	"math"
	"net"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	circuitbreaker "github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/growstack"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/grpcinterceptor"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
)

// NewServer sets up an RPC server. Depending on the ServerOptions, the Server
// either expects incoming connections from KV nodes, or from tenant SQL
// servers.
func NewServer(ctx context.Context, rpcCtx *Context, opts ...ServerOption) (*grpc.Server, error) {
	srv, _, _, err := NewServerEx(ctx, rpcCtx, opts...)
	return srv, err
}

// ServerInterceptorInfo contains the server-side interceptors that a server
// created with NewServerEx() will run.
type ServerInterceptorInfo struct {
	// UnaryInterceptors lists the interceptors for regular (unary) RPCs.
	UnaryInterceptors []grpc.UnaryServerInterceptor
	// StreamInterceptors lists the interceptors for streaming RPCs.
	StreamInterceptors []grpc.StreamServerInterceptor
}

// ClientInterceptorInfo contains the client-side interceptors that a Context
// uses for RPC calls.
type ClientInterceptorInfo struct {
	// UnaryInterceptors lists the interceptors for regular (unary) RPCs.
	UnaryInterceptors []grpc.UnaryClientInterceptor
	// StreamInterceptors lists the interceptors for streaming RPCs.
	StreamInterceptors []grpc.StreamClientInterceptor
}

// NewServerEx is like NewServer, but also returns the interceptors that have
// been registered with gRPC for the server. These interceptors can be used
// manually when bypassing gRPC to call into the server (like the
// internalClientAdapter does).
func NewServerEx(
	ctx context.Context, rpcCtx *Context, opts ...ServerOption,
) (s *grpc.Server, d *DRPCServer, sii ServerInterceptorInfo, err error) {
	var o serverOpts
	for _, f := range opts {
		f(&o)
	}
	grpcOpts := []grpc.ServerOption{
		// The limiting factor for lowering the max message size is the fact
		// that a single large kv can be sent over the network in one message.
		// Our maximum kv size is unlimited, so we need this to be very large.
		//
		// TODO(peter,tamird): need tests before lowering.
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
		// Adjust the stream and connection window sizes. The gRPC defaults are too
		// low for high latency connections.
		grpc.InitialWindowSize(rpcCtx.initialWindowSize(ctx)),
		grpc.InitialConnWindowSize(rpcCtx.initialConnWindowSize(ctx)),
		// The default number of concurrent streams/requests on a client connection
		// is 100, while the server is unlimited. The client setting can only be
		// controlled by adjusting the server value. Set a very large value for the
		// server value so that we have no fixed limit on the number of concurrent
		// streams/requests on either the client or server.
		grpc.MaxConcurrentStreams(math.MaxInt32),
		grpc.KeepaliveParams(serverKeepalive),
		grpc.KeepaliveEnforcementPolicy(serverEnforcement),
	}
	if !rpcCtx.ContextOptions.Insecure {
		tlsConfig, err := rpcCtx.GetServerTLSConfig()
		if err != nil {
			return nil, nil, sii, err
		}
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}

	// These interceptors will be called in the order in which they appear, i.e.
	// The last element will wrap the actual handler. The first interceptor
	// guards RPC endpoints for use after Stopper.Drain() by handling the RPC
	// inside a stopper task.
	var unaryInterceptor []grpc.UnaryServerInterceptor
	var streamInterceptor []grpc.StreamServerInterceptor
	unaryInterceptor = append(unaryInterceptor, func(
		ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (interface{}, error) {
		var resp interface{}
		if err := rpcCtx.Stopper.RunTaskWithErr(ctx, info.FullMethod, func(ctx context.Context) error {
			var err error
			resp, err = handler(ctx, req)
			return err
		}); err != nil {
			return nil, err
		}
		return resp, nil
	})
	streamInterceptor = append(streamInterceptor, func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return rpcCtx.Stopper.RunTaskWithErr(ss.Context(), info.FullMethod, func(ctx context.Context) error {
			return handler(srv, ss)
		})
	})

	if !rpcCtx.ContextOptions.Insecure {
		a := kvAuth{
			sv: &rpcCtx.Settings.SV,
			tenant: tenantAuthorizer{
				tenantID:               rpcCtx.tenID,
				capabilitiesAuthorizer: rpcCtx.capabilitiesAuthorizer,
			},
		}

		unaryInterceptor = append(unaryInterceptor, a.AuthUnary())
		streamInterceptor = append(streamInterceptor, a.AuthStream())
	}

	if o.interceptor != nil {
		unaryInterceptor = append(unaryInterceptor, func(
			ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
		) (interface{}, error) {
			if err := o.interceptor(info.FullMethod); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		})

		streamInterceptor = append(streamInterceptor, func(
			srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
		) error {
			if err := o.interceptor(info.FullMethod); err != nil {
				return err
			}
			return handler(srv, stream)
		})
	}

	if tracer := rpcCtx.Stopper.Tracer(); tracer != nil {
		unaryInterceptor = append(unaryInterceptor, grpcinterceptor.ServerInterceptor(tracer))
		streamInterceptor = append(streamInterceptor, grpcinterceptor.StreamServerInterceptor(tracer))
	}

	grpcOpts = append(grpcOpts, grpc.ChainUnaryInterceptor(unaryInterceptor...))
	grpcOpts = append(grpcOpts, grpc.ChainStreamInterceptor(streamInterceptor...))

	s = grpc.NewServer(grpcOpts...)
	d, err = newDRPCServer(ctx, rpcCtx)
	if err != nil {
		return nil, nil, ServerInterceptorInfo{}, err
	}
	RegisterHeartbeatServer(s, rpcCtx.NewHeartbeatService())

	return s, d, ServerInterceptorInfo{
		UnaryInterceptors:  unaryInterceptor,
		StreamInterceptors: streamInterceptor,
	}, nil
}

// Context is a pool of *grpc.ClientConn that are periodically health-checked,
// and for which circuit breaking and metrics are provided. Callers can obtain a
// *Connection via the non-blocking GRPCDialNode method. A *Connection is akin
// to a promise, with the `Connection.Connect` method blocking on the promise. A
// single underlying *grpc.ClientConn is maintained for each triplet of (NodeID,
// TargetAddress, ConnectionClass).
//
// TODO(tbg): rename at the very least the `ctx` receiver, but possibly the whole
// thing.
// TODO(baptist): Remove the inheritance on ContextOptions directly construct
// the object with what it needs.
type Context struct {
	ContextOptions
	*SecurityContext

	RemoteClocks *RemoteClockMonitor
	MasterCtx    context.Context // cancel on stopper quiesce

	rpcCompression bool

	localInternalClient RestrictedInternalClient

	peers peerMap

	// dialbackMap is a map of currently executing dialback connections. This map
	// is typically empty or close to empty. It only holds entries that are being
	// verified for dialback due to failing a health check.
	dialbackMu struct {
		syncutil.Mutex
		m map[roachpb.NodeID]*Connection
	}

	metrics *Metrics

	// For unittesting.
	testingDialOpts []grpc.DialOption

	// For testing. See the comment on the same field in HeartbeatService.
	TestingAllowNamedRPCToAnonymousServer bool

	clientUnaryInterceptors  []grpc.UnaryClientInterceptor
	clientStreamInterceptors []grpc.StreamClientInterceptor

	// loopbackDialFn, when non-nil, is used when the target of the dial
	// is ourselves (== AdvertiseAddr).
	//
	// This special case is not merely a performance optimization: it
	// ensures that we are always able to self-dial. Reasons that could
	// block a self-dial, and have been seen in the wild, include:
	//
	// - DNS is not ready so AdvertiseAddr does not resolve.
	// - firewall rule only allows other machines to connect to our
	//   listen/external address, not ourselves.
	// - TCP port shortage on the local interface.
	//
	// The loopback listener is guaranteed to never talk to the OS'
	// TCP stack and thus always avoids any TCP-related shortcoming.
	//
	// Note that this mechanism is separate (and fully independent) from
	// the one used to provide the RestrictedInternalClient interface
	// via DialInternalClient(). RestrictedInternalClient is an
	// optimization for the "hot path" of KV batch requests, that takes
	// many shortcuts through our abstraction stack to provide direct Go
	// function calls for API functions, without the overhead of data
	// serialization/deserialization.
	//
	// At this stage, we only plan to use this optimization for the
	// small set of RPC endpoints it was designed for. The "common case"
	// remains the regular gRPC protocol using ser/deser over a link.
	// The loopbackDialFn fits under that common case by transporting
	// the gRPC protocol over an in-memory pipe.
	loopbackDialFn func(context.Context) (net.Conn, error)

	// clientCreds is used to pass additional headers to called RPCs.
	clientCreds credentials.PerRPCCredentials

	// windowSizeSettings contains the memoized window size
	// settings for this context.
	windowSizeSettings
}

// SetLoopbackDialer configures the loopback dialer function.
func (c *Context) SetLoopbackDialer(loopbackDialFn func(context.Context) (net.Conn, error)) {
	if c.ContextOptions.Knobs.NoLoopbackDialer {
		// A test has decided it is opting out of the special loopback
		// dialing mechanism. Obey it. We already have defined
		// loopbackDialFn in that case in NewContext().
		return
	}
	c.loopbackDialFn = loopbackDialFn
}

// StoreLivenessGracePeriod computes the grace period after a store restarts before which it will
// not withdraw support from other stores.
func (c *Context) StoreLivenessWithdrawalGracePeriod() time.Duration {
	// RPCHeartbeatInterval and RPCHeartbeatTimeout ensure the remote store
	// probes the RPC connection to the local store. DialTimeout ensures the
	// remote store has enough time to dial the local store, and NetworkTimeout
	// ensures the remote store's heartbeat is received by the local store.
	return c.RPCHeartbeatInterval + c.RPCHeartbeatTimeout + base.DialTimeout + base.NetworkTimeout
}

// ContextOptions are passed to NewContext to set up a new *Context.
// All pointer fields and TenantID are required.
type ContextOptions struct {
	TenantID roachpb.TenantID

	Clock                  hlc.WallClock
	ToleratedOffset        time.Duration
	FatalOnOffsetViolation bool
	Stopper                *stop.Stopper
	Settings               *cluster.Settings

	// Fields from base.Config used by a rpc.Context, either in clients
	// or servers.
	SSLCertsDir                    string
	Insecure                       bool
	ClusterName                    string
	DisableClusterNameVerification bool
	RPCHeartbeatInterval           time.Duration
	RPCHeartbeatTimeout            time.Duration
	HistogramWindowInterval        time.Duration

	// Fields from base.Config used only in RPC servers.
	LocalityAddresses []roachpb.LocalityAddress
	// We include the advertised address by reference because it is set
	// during server startup after the rpc.Context is created.
	*base.AdvertiseAddrH

	// The following fields come from base.Config and only needed by
	// SecurityContextOptions. They are not really used by the 'rpc'
	// code. They really belong elsewhere - see comments in
	// SecurityContextOptions.
	*base.SQLAdvertiseAddrH      // only for servers
	DisableTLSForHTTP       bool // only for servers

	// OnIncomingPing is called when handling a PingRequest, after
	// preliminary checks but before recording clock offset information.
	// It can inject an error or modify the response.
	OnIncomingPing func(context.Context, *PingRequest, *PingResponse) error
	// OnOutgoingPing intercepts outgoing PingRequests. It may inject an
	// error.
	OnOutgoingPing func(context.Context, *PingRequest) error
	Knobs          ContextTestingKnobs

	// NodeID is the node ID / SQL instance ID container shared
	// with the remainder of the server. If unset in the options,
	// the RPC context will instantiate its own separate container
	// (this is useful in tests).
	// Note: this ought to be renamed, see:
	// https://github.com/cockroachdb/cockroach/pull/73309
	NodeID *base.NodeIDContainer

	// StorageClusterID is the storage cluster's ID, shared with all
	// tenants on the same storage cluster. If unset in the options, the
	// RPC context will instantiate its own separate container (this is
	// useful in tests).
	StorageClusterID *base.ClusterIDContainer

	// LogicalClusterID is this server's cluster ID, different for each
	// tenant sharing the same storage cluster. If unset in the options,
	// the RPC context will use a mix of StorageClusterID and TenantID.
	LogicalClusterID *base.ClusterIDContainer

	// ClientOnly indicates that this RPC context is run by a CLI
	// utility, not a server, and thus misses server configuration, a
	// cluster version, a node ID, etc.
	ClientOnly bool

	// User is used in case ClientOnly is true.
	User username.SQLUsername

	// UseNodeAuth is only used when ClientOnly is not set.
	// When set, it indicates that this rpc.Context is running inside
	// the same process as a KV layer and thus should feel empowered
	// to use its node cert to perform outgoing RPC dials.
	UseNodeAuth bool

	// TenantRPCAuthorizer provides a handle into the tenantcapabilities
	// subsystem. It allows KV nodes to perform capability checks for incoming
	// tenant requests.
	TenantRPCAuthorizer tenantcapabilities.Authorizer

	// NeedsDialback indicates that connections created with this RPC context
	// should be verified after they are established by the recipient having a
	// backwards connection to us. This is used for KV server to KV server
	// communication. If there is already a healthy connection, then the
	// PingResponse is sent like normal, however if there is no connection then a
	// throwaway reverse TCP connection is made. This is set to true on
	// node-to-node connections and prevents one-way partitions from occurring by
	// turing them into two-way partitions.
	NeedsDialback bool

	// Locality stores the locality of this node.
	Locality roachpb.Locality
}

// DefaultContextOptions are mostly used in tests.
func DefaultContextOptions() ContextOptions {
	return ContextOptions{
		SSLCertsDir:             certnames.EmbeddedCertsDir,
		TenantID:                roachpb.SystemTenantID,
		User:                    username.NodeUserName(),
		HistogramWindowInterval: base.DefaultHistogramWindowInterval(),
		RPCHeartbeatInterval:    base.PingInterval,
		RPCHeartbeatTimeout:     base.DefaultRPCHeartbeatTimeout,
		AdvertiseAddrH:          &base.AdvertiseAddrH{},
		SQLAdvertiseAddrH:       &base.SQLAdvertiseAddrH{},
		Clock:                   &timeutil.DefaultTimeSource{},
		ToleratedOffset:         time.Nanosecond,
	}
}

// ServerContextOptionsFromBaseConfig initializes a ContextOptions struct from
// a base.Config struct.
func ServerContextOptionsFromBaseConfig(cfg *base.Config) ContextOptions {
	return ContextOptions{
		SSLCertsDir:                    cfg.SSLCertsDir,
		Insecure:                       cfg.Insecure,
		ClusterName:                    cfg.ClusterName,
		DisableClusterNameVerification: cfg.DisableClusterNameVerification,
		RPCHeartbeatInterval:           cfg.RPCHeartbeatInterval,
		RPCHeartbeatTimeout:            cfg.RPCHeartbeatTimeout,
		HistogramWindowInterval:        cfg.HistogramWindowInterval(),
		LocalityAddresses:              cfg.LocalityAddresses,
		AdvertiseAddrH:                 &cfg.AdvertiseAddrH,
		SQLAdvertiseAddrH:              &cfg.SQLAdvertiseAddrH,
		DisableTLSForHTTP:              cfg.DisableTLSForHTTP,
	}
}

func (c ContextOptions) validate() error {
	if c.TenantID == (roachpb.TenantID{}) {
		return errors.New("must specify TenantID")
	}
	if c.Clock == nil {
		return errors.New("Clock must be set")
	}
	if c.Stopper == nil {
		return errors.New("Stopper must be set")
	}
	if c.Settings == nil {
		return errors.New("Settings must be set")
	}
	if c.RPCHeartbeatInterval == 0 {
		return errors.New("RPCHeartbeatInterval must be set")
	}

	// NB: OnOutgoingPing and OnIncomingPing default to noops.
	// This is used both for testing and the cli.
	_, _ = c.OnOutgoingPing, c.OnIncomingPing

	return nil
}

// NewContext creates an rpc.Context with the supplied values.
func NewContext(ctx context.Context, opts ContextOptions) *Context {
	if err := opts.validate(); err != nil {
		panic(err)
	}

	if opts.NodeID == nil {
		// Tests rely on NewContext to generate its own ID container.
		var c base.NodeIDContainer
		opts.NodeID = &c
	}

	if opts.StorageClusterID == nil {
		// Tests rely on NewContext to generate its own ID container.
		var c base.ClusterIDContainer
		opts.StorageClusterID = &c
	}

	// In any case, inform logs when the node or cluster ID changes.
	//
	// TODO(tbg): this shouldn't be done here, but wherever these are instantiated.
	// Nothing here is specific to the `rpc.Context`.
	prevOnSetc := opts.StorageClusterID.OnSet
	opts.StorageClusterID.OnSet = func(id uuid.UUID) {
		if prevOnSetc != nil {
			prevOnSetc(id)
		}
		if log.V(2) {
			log.Infof(ctx, "ClusterID set to %s", id)
		}
	}
	prevOnSetn := opts.NodeID.OnSet
	opts.NodeID.OnSet = func(id roachpb.NodeID) {
		if prevOnSetn != nil {
			prevOnSetn(id)
		}
		if log.V(2) {
			log.Infof(ctx, "NodeID set to %s", id)
		}
	}

	if opts.LogicalClusterID == nil {
		if opts.TenantID.IsSystem() {
			// We currently expose the storage cluster ID as logical
			// cluster ID in the system tenant so that someone with
			// access to the system tenant can extract the storage cluster ID
			// via e.g. crdb_internal.cluster_id().
			//
			// TODO(knz): Remove this special case. The system tenant ought
			// to use a separate logical cluster ID too. We should use
			// separate primitives in crdb_internal, etc. to retrieve
			// the logical and storage cluster ID separately from each other.
			opts.LogicalClusterID = opts.StorageClusterID
		} else {
			// Create a logical cluster ID derived from the storage cluster
			// ID, but different for each tenant.
			// TODO(knz): Move this logic out of RPCContext.
			logicalClusterID := &base.ClusterIDContainer{}
			hasher := fnv.New64a()
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], opts.TenantID.ToUint64())
			_, _ = hasher.Write(b[:])
			hashedTenantID := hasher.Sum64()

			prevOnSet := opts.StorageClusterID.OnSet
			opts.StorageClusterID.OnSet = func(id uuid.UUID) {
				if prevOnSet != nil {
					prevOnSet(id)
				}
				hiLo := id.ToUint128()
				hiLo.Lo += hashedTenantID
				logicalClusterID.Set(ctx, uuid.FromUint128(hiLo))
			}
			opts.LogicalClusterID = logicalClusterID
		}
	}

	masterCtx, _ := opts.Stopper.WithCancelOnQuiesce(ctx)

	secCtx := NewSecurityContext(
		SecurityContextOptions{
			SSLCertsDir:       opts.SSLCertsDir,
			Insecure:          opts.Insecure,
			AdvertiseAddrH:    opts.AdvertiseAddrH,
			SQLAdvertiseAddrH: opts.SQLAdvertiseAddrH,
			DisableTLSForHTTP: opts.DisableTLSForHTTP,
		},
		security.ClusterTLSSettings(opts.Settings),
		opts.TenantID,
		opts.TenantRPCAuthorizer,
	)
	secCtx.useNodeAuth = opts.UseNodeAuth

	rpcCtx := &Context{
		ContextOptions:  opts,
		SecurityContext: secCtx,
		rpcCompression:  enableRPCCompression,
		MasterCtx:       masterCtx,
		metrics:         newMetrics(opts.Locality),
	}

	rpcCtx.dialbackMu.Lock()
	rpcCtx.dialbackMu.m = map[roachpb.NodeID]*Connection{}
	rpcCtx.dialbackMu.Unlock()

	if !opts.TenantID.IsSet() {
		panic("tenant ID not set")
	}

	if opts.ClientOnly && opts.User.Undefined() {
		panic("client username not set")
	}

	if !opts.TenantID.IsSystem() {
		rpcCtx.clientCreds = newTenantClientCreds(opts.TenantID)
	}

	if opts.Knobs.NoLoopbackDialer {
		// The test has decided it doesn't need/want a loopback dialer.
		// Ensure we still have a working dial function in that case.
		rpcCtx.loopbackDialFn = func(ctx context.Context) (net.Conn, error) {
			d := onlyOnceDialer{}
			return d.dial(ctx, opts.AdvertiseAddr)
		}
	}

	// We only monitor remote clocks in server-to-server connections.
	// CLI commands are exempted.
	if !opts.ClientOnly {
		rpcCtx.RemoteClocks = newRemoteClockMonitor(
			opts.Clock, opts.ToleratedOffset, 10*opts.RPCHeartbeatTimeout, opts.HistogramWindowInterval)
	}

	if id := opts.Knobs.StorageClusterID; id != nil {
		rpcCtx.StorageClusterID.Set(masterCtx, *id)
	}

	if tracer := rpcCtx.Stopper.Tracer(); tracer != nil {
		// We use a decorator to set the "node" tag. All other spans get the
		// node tag from context log tags.
		//
		// Unfortunately we cannot use the corresponding interceptor on the
		// server-side of gRPC to set this tag on server spans because that
		// interceptor runs too late - after a traced RPC's recording had
		// already been collected. So, on the server-side, the equivalent code
		// is in setupSpanForIncomingRPC().
		//
		tagger := func(span *tracing.Span) {
			span.SetTag("node", attribute.IntValue(int(rpcCtx.NodeID.Get())))
		}

		if rpcCtx.ClientOnly {
			// client-only RPC contexts don't have a node ID to report nor a
			// cluster version to check against.
			tagger = func(span *tracing.Span) {}
		}

		rpcCtx.clientUnaryInterceptors = append(rpcCtx.clientUnaryInterceptors,
			grpcinterceptor.ClientInterceptor(tracer, tagger))
		rpcCtx.clientStreamInterceptors = append(rpcCtx.clientStreamInterceptors,
			grpcinterceptor.StreamClientInterceptor(tracer, tagger))
	}
	// Note that we do not consult rpcCtx.Knobs.StreamClientInterceptor. That knob
	// can add another interceptor, but it can only do it dynamically, based on
	// a connection class. Only calls going over an actual gRPC connection will
	// use that interceptor.

	return rpcCtx
}

// ClusterName retrieves the configured cluster name.
func (rpcCtx *Context) ClusterName() string {
	if rpcCtx == nil {
		// This is used in tests.
		return "<MISSING RPC CONTEXT>"
	}
	return rpcCtx.ContextOptions.ClusterName
}

// Metrics returns the Context's Metrics struct.
func (rpcCtx *Context) Metrics() *Metrics {
	return rpcCtx.metrics
}

// GetLocalInternalClientForAddr returns the context's internal batch client
// for target, if it exists.
// Note: the node ID ought to be retyped, see
// https://github.com/cockroachdb/cockroach/pull/73309
func (rpcCtx *Context) GetLocalInternalClientForAddr(
	nodeID roachpb.NodeID,
) RestrictedInternalClient {
	if nodeID == rpcCtx.NodeID.Get() {
		return rpcCtx.localInternalClient
	}
	return nil
}

// internalClientAdapter is an implementation of kvpb.InternalClient that
// bypasses gRPC, calling the wrapped local server directly.
//
// Even though the calls don't go through gRPC, the internalClientAdapter runs
// the configured gRPC client-side and server-side interceptors.
type internalClientAdapter struct {
	server kvpb.InternalServer

	// clientTenantID is the tenant ID for the client (caller) side
	// of the call. (The server/callee side is
	// always the KV layer / system tenant.)
	clientTenantID roachpb.TenantID

	// separateTracer indicates that the client (caller)
	// and server (callee) sides use different tracers.
	separateTracers bool

	// batchHandler is the RPC handler for Batch(). This includes both the chain
	// of client-side and server-side gRPC interceptors, and bottoms out by
	// calling server.Batch().
	batchHandler func(ctx context.Context, ba *kvpb.BatchRequest, opts ...grpc.CallOption) (*kvpb.BatchResponse, error)

	// The streaming interceptors. These cannot be chained together at
	// construction time like the unary interceptors.
	clientStreamInterceptors clientStreamInterceptorsChain
	serverStreamInterceptors serverStreamInterceptorsChain
}

var _ RestrictedInternalClient = internalClientAdapter{}

// makeInternalClientAdapter constructs a internalClientAdapter.
//
// clientTenantID is the tenant ID of the caller side of the
// interface. This might be for a secondary tenant, which enables us
// to use the internal client adapter when running a secondary tenant
// server inside the same process as the KV layer.
//
// The caller can set separateTracers to indicate that the
// caller and callee use separate tracers, so we can't
// use a child tracing span directly.
func makeInternalClientAdapter(
	server kvpb.InternalServer,
	clientTenantID roachpb.TenantID,
	separateTracers bool,
	clientUnaryInterceptors []grpc.UnaryClientInterceptor,
	clientStreamInterceptors []grpc.StreamClientInterceptor,
	serverUnaryInterceptors []grpc.UnaryServerInterceptor,
	serverStreamInterceptors []grpc.StreamServerInterceptor,
) internalClientAdapter {
	// We're going to chain the unary interceptors together in single functions
	// that run all of them, and we're going to memo-ize the resulting functions
	// so that we don't need to generate them on the fly for every RPC call. We
	// can't do that for the streaming interceptors, unfortunately, because the
	// handler that these interceptors need to ultimately run needs to be
	// allocated specifically for every call. For the client interceptors, the
	// handler needs to capture a pipe used to communicate results, and for server
	// interceptors the handler needs to capture the request arguments.

	// batchServerHandler wraps a server.Batch() call with all the server
	// interceptors.
	batchServerHandler := chainUnaryServerInterceptors(
		&grpc.UnaryServerInfo{
			Server:     server,
			FullMethod: grpcinterceptor.BatchMethodName,
		},
		serverUnaryInterceptors,
		func(ctx context.Context, req interface{}) (interface{}, error) {
			br, err := server.Batch(ctx, req.(*kvpb.BatchRequest))
			return br, err
		},
	)
	// batchClientHandler wraps batchServer handler with all the client
	// interceptors. So we're going to get a function that calls all the client
	// interceptors, then all the server interceptors, and bottoms out with
	// calling server.Batch().
	batchClientHandler := getChainUnaryInvoker(clientUnaryInterceptors, 0, /* curr */
		func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			resp, err := batchServerHandler(ctx, req)
			if resp != nil {
				br := resp.(*kvpb.BatchResponse)
				if br != nil {
					*(reply.(*kvpb.BatchResponse)) = *br
				}
			}
			return err
		})

	return internalClientAdapter{
		server:                   server,
		clientTenantID:           clientTenantID,
		separateTracers:          separateTracers,
		clientStreamInterceptors: clientStreamInterceptors,
		serverStreamInterceptors: serverStreamInterceptors,
		batchHandler: func(ctx context.Context, ba *kvpb.BatchRequest, opts ...grpc.CallOption) (*kvpb.BatchResponse, error) {
			ba = ba.ShallowCopy()
			// Mark this as originating locally, which is useful for the decision about
			// memory allocation tracking.
			ba.AdmissionHeader.SourceLocation = kvpb.AdmissionHeader_LOCAL
			// reply serves to communicate the RPC response from the RPC handler (through
			// the server interceptors) to the client interceptors. The client
			// interceptors will have a chance to modify it, and ultimately it will be
			// returned to the caller. Unfortunately, we have to allocate here: because of
			// how the gRPC client interceptor interface works, interceptors don't get
			// a result from the next interceptor (and eventually from the server);
			// instead, the result is allocated by the client. We'll copy the
			// server-side result into reply in batchHandler().
			reply := new(kvpb.BatchResponse)

			// Create a new context from the existing one with the "local request"
			// field set. This tells the handler that this is an in-process request,
			// bypassing ctx.Peer checks. This call also overwrites any possibly
			// existing info in the context. This is important in situations where a
			// shared-process tenant calls into the local KV node, and that local RPC
			// ends up performing another RPC to the local node. The inner RPC must
			// carry the identity of the system tenant, not the one of the client of
			// the outer RPC.
			ctx = grpcutil.NewLocalRequestContext(ctx, clientTenantID)

			// Clear any leftover gRPC incoming metadata, if this call
			// is originating from a RPC handler function called as
			// a result of a tenant call. This is this case:
			//
			//    tenant -(rpc)-> tenant -(rpc)-> KV
			//                            ^ YOU ARE HERE
			//
			// at this point, the left side RPC has left some incoming
			// metadata in the context, but we need to get rid of it
			// before we let the call go through KV.
			ctx = grpcutil.ClearIncomingContext(ctx)

			// If the caller and callee use separate tracers, we make things
			// look closer to a remote call from the tracing point of view.
			if separateTracers {
				sp := tracing.SpanFromContext(ctx)
				if sp != nil && !sp.IsNoop() {
					// Fill in ba.TraceInfo. For remote RPCs (not done throught the
					// internalClientAdapter), this is done by the TracingInternalClient
					ba = ba.ShallowCopy()
					ba.TraceInfo = sp.Meta().ToProto()
				}
				// Wipe the span from context. The server will create a root span with a
				// different Tracer, based on remote parent information provided by the
				// TraceInfo above. If we didn't do this, the server would attempt to
				// create a child span with its different Tracer, which is not allowed.
				ctx = tracing.ContextWithSpan(ctx, nil)
			}

			err := batchClientHandler(ctx, grpcinterceptor.BatchMethodName, ba, reply, nil /* ClientConn */, opts...)
			return reply, err
		},
	}
}

// chainUnaryServerInterceptors takes a slice of RPC interceptors and a final RPC
// handler and returns a new handler that consists of all the interceptors
// running, in order, before finally running the original handler.
//
// Note that this allocates one function per interceptor, so the resulting
// handler should be memoized.
func chainUnaryServerInterceptors(
	info *grpc.UnaryServerInfo,
	serverInterceptors []grpc.UnaryServerInterceptor,
	handler grpc.UnaryHandler,
) grpc.UnaryHandler {
	f := handler
	for i := len(serverInterceptors) - 1; i >= 0; i-- {
		f = bindUnaryServerInterceptorToHandler(info, serverInterceptors[i], f)
	}
	return f
}

// bindUnaryServerInterceptorToHandler takes an RPC server interceptor and an
// RPC handler and returns a new handler that consists of the interceptor
// wrapping the original handler.
func bindUnaryServerInterceptorToHandler(
	info *grpc.UnaryServerInfo, interceptor grpc.UnaryServerInterceptor, handler grpc.UnaryHandler,
) grpc.UnaryHandler {
	return func(ctx context.Context, req interface{}) (resp interface{}, err error) {
		return interceptor(ctx, req, info, handler)
	}
}

type serverStreamInterceptorsChain []grpc.StreamServerInterceptor
type clientStreamInterceptorsChain []grpc.StreamClientInterceptor

// run runs the server stream interceptors and bottoms out by running handler.
//
// As opposed to the unary interceptors, we cannot memoize the chaining of
// streaming interceptors with a handler because the handler is
// request-specific: it needs to capture the request proto.
//
// This code was adapted from gRPC:
// https://github.com/grpc/grpc-go/blob/ec717cad7395d45698b57c1df1ae36b4dbaa33dd/server.go#L1396
func (c serverStreamInterceptorsChain) run(
	srv interface{},
	stream grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	if len(c) == 0 {
		return handler(srv, stream)
	}

	// state groups escaping variables into a single allocation.
	var state struct {
		i    int
		next grpc.StreamHandler
	}
	state.next = func(srv interface{}, stream grpc.ServerStream) error {
		if state.i == len(c)-1 {
			return c[state.i](srv, stream, info, handler)
		}
		state.i++
		return c[state.i-1](srv, stream, info, state.next)
	}
	return state.next(srv, stream)
}

// run runs the the client stream interceptors and bottoms out by running streamer.
//
// Unlike the unary interceptors, the chaining of these interceptors with a
// streamer cannot be memo-ized because the streamer is different on every call;
// the streamer needs to capture a pipe on which results will flow.
func (c clientStreamInterceptorsChain) run(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	if len(c) == 0 {
		return streamer(ctx, desc, cc, method, opts...)
	}

	// state groups escaping variables into a single allocation.
	var state struct {
		i    int
		next grpc.Streamer
	}
	state.next = func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if state.i == len(c)-1 {
			return c[state.i](ctx, desc, cc, method, streamer, opts...)
		}
		state.i++
		return c[state.i-1](ctx, desc, cc, method, state.next, opts...)
	}
	return state.next(ctx, desc, cc, method, opts...)
}

// getChainUnaryInvoker returns a function that, when called, invokes all the
// interceptors from curr onwards and bottoms out by invoking finalInvoker. curr
// == 0 means call all the interceptors.
//
// The returned function is generated recursively.
func getChainUnaryInvoker(
	interceptors []grpc.UnaryClientInterceptor, curr int, finalInvoker grpc.UnaryInvoker,
) grpc.UnaryInvoker {
	if curr == len(interceptors) {
		return finalInvoker
	}
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return interceptors[curr](ctx, method, req, reply, cc, getChainUnaryInvoker(interceptors, curr+1, finalInvoker), opts...)
	}
}

// Batch implements the kvpb.InternalClient interface.
func (a internalClientAdapter) Batch(
	ctx context.Context, ba *kvpb.BatchRequest, opts ...grpc.CallOption,
) (*kvpb.BatchResponse, error) {
	return a.batchHandler(ctx, ba, opts...)
}

var muxRangeFeedDesc = &grpc.StreamDesc{
	StreamName:    "MuxRangeFeed",
	ServerStreams: true,
	ClientStreams: true,
}

const muxRangefeedMethodName = "/cockroach.roachpb.Internal/MuxRangeFeed"

var muxRangefeedStreamInfo = &grpc.StreamServerInfo{
	FullMethod:     muxRangefeedMethodName,
	IsClientStream: true,
	IsServerStream: true,
}

// MuxRangeFeed implements the RestrictedInternalClient interface.
//
// This is a bi-directional streaming RPC, as opposed to RangeFeed which is
// uni-directional. This is why this implementation is a bit different.
func (a internalClientAdapter) MuxRangeFeed(
	ctx context.Context, opts ...grpc.CallOption,
) (kvpb.Internal_MuxRangeFeedClient, error) {
	// MuxRangeFeed is a bi-directional RPC, so we have to deal with two streams:
	// the client stream and the server stream. The client stream sends
	// RangeFeedRequests and receives MuxRangeFeedEvents, whereas the server
	// stream does the reverse: it sends MuxRangeFeedEvents and receives
	// RangeFeedRequests. These streams use two pipes - one for MuxRangeFeedEvents
	// and one for RangeFeedRequests. The client stream uses the writing end of
	// the RangeFeedRequest pipe and the reading end of the MuxRangeFeedEvents
	// pipe, and the server stream does the reverse.
	//
	// The flow of data through the streams, from server to client (or, read in
	// reverse, from client to server) looks like:
	//   RPC handler (i.e. Node.RangeFeed) ->
	//    -> rangeFeedServerAdapter
	//    -> grpc.ServerStream implementations provided by server-side interceptors
	//    -> rawServerStream
	//        ^
	//        | the two pipes
	//        v
	//    -> rawClientStream
	//    -> grpc.ClientStream implementations provided by client-side interceptors
	//    -> rangeFeedClientAdapter
	//    -> rawClientStream
	//    -> RPC caller

	eventWriter, eventReader := makePipe(func(dst interface{}, src interface{}) {
		*dst.(*kvpb.MuxRangeFeedEvent) = *src.(*kvpb.MuxRangeFeedEvent)
	})
	requestWriter, requestReader := makePipe(func(dst interface{}, src interface{}) {
		*dst.(*kvpb.RangeFeedRequest) = *src.(*kvpb.RangeFeedRequest)
	})
	rawClientStream := &clientStream{
		ctx:      ctx,
		receiver: eventReader,
		sender:   requestWriter,
	}

	serverCtx, serverCancel := context.WithCancel(ctx)

	if a.separateTracers {
		// Wipe the span from context. The server will create a root span with a
		// different Tracer, based on remote parent information provided by the
		// TraceInfo above. If we didn't do this, the server would attempt to
		// create a child span with its different Tracer, which is not allowed.
		serverCtx = tracing.ContextWithSpan(serverCtx, nil)
	}
	// Create a new context from the existing one with the "local request"
	// field set. This tells the handler that this is an in-process request,
	// bypassing ctx.Peer checks. This call also overwrites any possibly
	// existing info in the context. This is important in situations where a
	// shared-process tenant calls into the local KV node, and that local RPC
	// ends up performing another RPC to the local node. The inner RPC must
	// carry the identity of the system tenant, not the one of the client of
	// the outer RPC.
	serverCtx = grpcutil.NewLocalRequestContext(serverCtx, a.clientTenantID)

	// Clear any leftover gRPC incoming metadata, if this call
	// is originating from a RPC handler function called as
	// a result of a tenant call. This is this case:
	//
	//    tenant -(rpc)-> tenant -(rpc)-> KV
	//                            ^ YOU ARE HERE
	//
	// at this point, the left side RPC has left some incoming
	// metadata in the context, but we need to get rid of it
	// before we let the call go through KV.
	serverCtx = grpcutil.ClearIncomingContext(serverCtx)

	rawServerStream := &serverStream{
		ctx:      serverCtx,
		receiver: requestReader,
		sender:   eventWriter,
	}

	wg := ctxgroup.WithContext(serverCtx)
	wg.Go(func() error {
		// Handler adapts the ServerStream to the typed interface expected by the
		// RPC handler (Node.MuxRangeFeed). `stream` might be `rawServerStream` which we
		// pass to the interceptor chain below, or it might be another
		// implementation of `ServerStream` that wraps it; in practice it will be
		// tracing.grpcinterceptor.StreamServerInterceptor.
		handler := func(srv interface{}, stream grpc.ServerStream) error {
			adapter := muxRangeFeedServerAdapter{
				ServerStream: stream,
			}
			return a.server.MuxRangeFeed(adapter)
		}
		// Run the server interceptors, which will bottom out by running `handler`
		// (defined just above), which runs Node.MuxRangeFeed (our RPC handler).
		// This call is blocking.
		err := a.serverStreamInterceptors.run(a.server, rawServerStream, muxRangefeedStreamInfo, handler)
		if err == nil {
			err = io.EOF
		}
		// Notify client side that server exited.
		rawServerStream.sendError(err)
		return nil
	})

	// Run the client-side interceptors, which produce a gprc.ClientStream.
	// clientSide might end up being rfPipe, or it might end up being another
	// grpc.ClientStream implementation that wraps it.
	//
	// NOTE: For actual RPCs, going to a remote note, there's a tracing client
	// interceptor producing a tracing.grpcinterceptor.tracingClientStream
	// implementation of ClientStream. That client interceptor does not run for
	// these local requests handled by the internalClientAdapter (as opposed to
	// the tracing server interceptor, which does run).
	clientStream, err := a.clientStreamInterceptors.run(ctx, muxRangeFeedDesc, nil /* ClientConn */, muxRangefeedMethodName,
		// This function runs at the bottom of the client interceptor stack,
		// pretending to actually make an RPC call. We don't make any calls, but
		// return the pipe on which messages from the server will come.
		func(
			ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption,
		) (grpc.ClientStream, error) {
			return rawClientStream, nil
		},
		opts...)
	if err != nil {
		serverCancel()
		return nil, errors.CombineErrors(err, wg.Wait())
	}

	// Server cancelation handled by the caller for non-error
	// response.
	return muxRangeFeedClientAdapter{
		ClientStream: clientStream,
		serverCancel: serverCancel,
		serverWg:     &wg,
	}, nil
}

type muxRangeFeedClientAdapter struct {
	grpc.ClientStream
	serverCancel context.CancelFunc
	serverWg     *ctxgroup.Group
}

var _ kvpb.Internal_MuxRangeFeedClient = muxRangeFeedClientAdapter{}

func (a muxRangeFeedClientAdapter) Close() error {
	a.serverCancel()
	return a.serverWg.Wait()
}

func (a muxRangeFeedClientAdapter) Send(request *kvpb.RangeFeedRequest) error {
	// Mark this request as originating locally.
	request.AdmissionHeader.SourceLocation = kvpb.AdmissionHeader_LOCAL
	return a.SendMsg(request)
}

func (a muxRangeFeedClientAdapter) Recv() (*kvpb.MuxRangeFeedEvent, error) {
	m := new(kvpb.MuxRangeFeedEvent)
	if err := a.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

type muxRangeFeedServerAdapter struct {
	grpc.ServerStream
}

var _ kvpb.Internal_MuxRangeFeedServer = muxRangeFeedServerAdapter{}

func (a muxRangeFeedServerAdapter) Send(event *kvpb.MuxRangeFeedEvent) error {
	return a.SendMsg(event)
}

func (a muxRangeFeedServerAdapter) Recv() (*kvpb.RangeFeedRequest, error) {
	m := new(kvpb.RangeFeedRequest)
	if err := a.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// pipe represents a uni-directional pipe. Datums can be written through send(),
// and received through recv(). Errors can also be sent through sendError();
// they'll be received through recv() without ordering guarantees in
// relationship to the other datums.
type pipe struct {
	respC chan interface{}
	errC  chan error
}

// buffer size for channel used to connect local streaming rpcs.
var localStreamChannelBufferSize int64 = 128 // accessed atomically.

// TestingSetLocalStreamChannelBufferSize overrides channel buffer size
// used for streaming RPCs.
func TestingSetLocalStreamChannelBufferSize(s int64) func() {
	old := atomic.LoadInt64(&localStreamChannelBufferSize)
	atomic.StoreInt64(&localStreamChannelBufferSize, s)
	return func() { atomic.StoreInt64(&localStreamChannelBufferSize, old) }
}

// makePipe creates a pipe and return it as its two ends.
//
// assignPtr is a function that implements *dst = *src for the type of the
// datums that flow through this pipe. This is needed because the
// pipeReader.recv(m) method wants to assign to its argument in order to match
// the gRPC stream interface. However, the pipe is implemented in a generic
// (i.e. interface{}) way.
func makePipe(assignPtr func(dst interface{}, src interface{})) (pipeWriter, pipeReader) {
	p := &pipe{
		respC: make(chan interface{}, atomic.LoadInt64(&localStreamChannelBufferSize)),
		errC:  make(chan error, 1),
	}
	w := pipeWriter{pipe: p}
	r := pipeReader{
		pipe:      p,
		assignPtr: assignPtr,
	}
	return w, r
}

func (s *pipe) send(ctx context.Context, m interface{}) error {
	select {
	case s.respC <- m:
		return nil
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "send")
	}
}

func (s *pipe) sendError(err error) {
	s.errC <- err
}

func (s *pipe) recv(ctx context.Context) (interface{}, error) {
	// Prioritize respC. Both channels are buffered and the only guarantee we
	// have is that once an error is sent on errC no other events will be sent
	// on respC again.
	select {
	case e := <-s.respC:
		return e, nil
	case err := <-s.errC:
		select {
		case e := <-s.respC:
			s.errC <- err
			return e, nil
		default:
			return nil, err
		}
	case <-ctx.Done():
		return nil, errors.Wrap(ctx.Err(), "recv")
	}
}

// pipeWriter represents the writing end of a pipe.
type pipeWriter struct {
	pipe *pipe
}

func (w pipeWriter) send(ctx context.Context, m interface{}) error {
	return w.pipe.send(ctx, m)
}

func (w pipeWriter) sendError(err error) {
	w.pipe.sendError(err)
}

// pipeReader represents the reading end of a pipe.
type pipeReader struct {
	pipe      *pipe
	assignPtr func(dst interface{}, src interface{})
}

func (r pipeReader) recv(ctx context.Context, dst interface{}) error {
	m, err := r.pipe.recv(ctx)
	if err != nil {
		return err
	}
	r.assignPtr(dst, m)
	return nil
}

// clientStream is an implementation of grpc.ClientStream that send messages on
// one pipe and receives messages from another pipe.
//
// Both the sending and the receiving are optional; they can be disabled by not
// initializing the respective pipe end. A clientStream for a server-streaming
// RPC will be able to receive only; a clientStream for a client-streaming RPC
// will be able to send only. A clientStream for a bi-directional RPC will be
// able to both send and receive different types of Datums. A clientStream that
// can't send and can't receive is non-sensical.
type clientStream struct {
	// ctx is used to interrupt sends and receives. clientStream captures a ctx to
	// use throughout because the gRPC interface doesn't take a ctx.
	ctx      context.Context
	receiver pipeReader
	sender   pipeWriter
}

var _ grpc.ClientStream = clientStream{}

func (c clientStream) Header() (metadata.MD, error) {
	panic("unimplemented")
}

func (c clientStream) Trailer() metadata.MD {
	panic("unimplemented")
}

func (c clientStream) CloseSend() error {
	panic("unimplemented")
}

func (c clientStream) Context() context.Context {
	return c.ctx
}

func (c clientStream) SendMsg(m interface{}) error {
	return c.sender.send(c.ctx, m)
}

func (c clientStream) RecvMsg(m interface{}) error {
	return c.receiver.recv(c.ctx, m)
}

type serverStream struct {
	ctx      context.Context
	receiver pipeReader
	sender   pipeWriter
}

func (s serverStream) SetHeader(_ metadata.MD) error {
	panic("unimplemented")
}

func (s serverStream) SendHeader(_ metadata.MD) error {
	panic("unimplemented")
}

func (s serverStream) SetTrailer(_ metadata.MD) {
	panic("unimplemented")
}

func (s serverStream) Context() context.Context {
	return s.ctx
}

func (s serverStream) SendMsg(m interface{}) error {
	return s.sender.send(s.ctx, m)
}

func (s serverStream) RecvMsg(m interface{}) error {
	return s.receiver.recv(s.ctx, m)
}

func (s serverStream) sendError(err error) {
	s.sender.sendError(err)
}

var _ grpc.ServerStream = serverStream{}

// IsLocal returns true if the given InternalClient is local.
func IsLocal(iface RestrictedInternalClient) bool {
	_, ok := iface.(internalClientAdapter)
	return ok // internalClientAdapter is used for local connections.
}

// SetLocalInternalServer links the local server to the Context, allowing some
// RPCs to bypass gRPC.
//
// serverInterceptors lists the interceptors that will be run on RPCs done
// through this local server.
func (rpcCtx *Context) SetLocalInternalServer(
	internalServer kvpb.InternalServer,
	serverInterceptors ServerInterceptorInfo,
	clientInterceptors ClientInterceptorInfo,
) {
	clientTenantID := rpcCtx.TenantID
	separateTracers := false
	if !clientTenantID.IsSystem() {
		// This is a secondary tenant server in the same process as the KV
		// layer (shared-process multitenancy). In this case, the caller
		// and the callee use separate tracers, so we can't mix and match
		// tracing spans.
		separateTracers = true
	}
	rpcCtx.localInternalClient = makeInternalClientAdapter(
		internalServer,
		clientTenantID,
		separateTracers,
		clientInterceptors.UnaryInterceptors,
		clientInterceptors.StreamInterceptors,
		serverInterceptors.UnaryInterceptors,
		serverInterceptors.StreamInterceptors)
}

// ConnHealth returns nil if we have an open connection of the request
// class to the given node that succeeded on its most recent heartbeat.
// Note: the node ID ought to be retyped, see
// https://github.com/cockroachdb/cockroach/pull/73309
func (rpcCtx *Context) ConnHealth(
	target string, nodeID roachpb.NodeID, class ConnectionClass,
) error {
	// The local client is always considered healthy.
	if rpcCtx.GetLocalInternalClientForAddr(nodeID) != nil {
		return nil
	}
	if p, ok := rpcCtx.peers.get(peerKey{target, nodeID, class}); ok {
		return p.c.Health()
	}
	return ErrNotHeartbeated
}

type transportType bool

const (
	// loopbackTransport is used for in-memory connections that bypass
	// the TCP stack entirely, using the loopback listener.
	loopbackTransport transportType = false
	// tcpTransport is used when reaching out via TCP.
	tcpTransport transportType = true
)

// GRPCDialOptions returns the minimal `grpc.DialOption`s necessary to connect
// to a server.
func (rpcCtx *Context) GRPCDialOptions(
	ctx context.Context, target string, class ConnectionClass,
) ([]grpc.DialOption, error) {
	transport := tcpTransport
	if rpcCtx.ContextOptions.AdvertiseAddr == target && !rpcCtx.ClientOnly {
		// See the explanation on loopbackDialFn for an explanation about this.
		transport = loopbackTransport
	}
	return rpcCtx.grpcDialOptionsInternal(ctx, target, class, transport)
}

// grpcDialOptions produces dial options suitable for connecting to the given target and class.
func (rpcCtx *Context) grpcDialOptionsInternal(
	ctx context.Context, target string, class ConnectionClass, transport transportType,
) ([]grpc.DialOption, error) {
	dialOpts, err := rpcCtx.dialOptsCommon(ctx, target, class)
	if err != nil {
		return nil, err
	}

	switch transport {
	case tcpTransport:
		netOpts, err := rpcCtx.dialOptsNetwork(ctx, target, class)
		if err != nil {
			return nil, err
		}
		dialOpts = append(dialOpts, netOpts...)
	case loopbackTransport:
		localOpts, err := rpcCtx.dialOptsLocal()
		if err != nil {
			return nil, err
		}
		dialOpts = append(dialOpts, localOpts...)
	default:
		// This panic in case the type is ever changed to include more values.
		panic(errors.AssertionFailedf("unhandled: %v", transport))
	}
	return dialOpts, nil
}

// dialOptsLocal computes options used only for loopback connections.
func (rpcCtx *Context) dialOptsLocal() ([]grpc.DialOption, error) {
	// We need to include a TLS overlay even for loopback connections,
	// because currently a non-insecure server always refuses non-TLS
	// incoming connections, and inspects the TLS certs to determine the
	// identity of the client peer.
	//
	// We can elide TLS for loopback connections when we add a non-TLS
	// way to identify peers, i.e. fix these issues:
	// https://github.com/cockroachdb/cockroach/issues/54007
	// https://github.com/cockroachdb/cockroach/issues/91996
	dialOpts, err := rpcCtx.dialOptsNetworkCredentials()
	if err != nil {
		return nil, err
	}

	dialOpts = append(dialOpts, grpc.WithContextDialer(
		func(ctx context.Context, _ string) (net.Conn, error) {
			return rpcCtx.loopbackDialFn(ctx)
		}))

	return dialOpts, err
}

// GetBreakerForAddr looks up a breaker for the matching (NodeID,Class,Addr).
// If it exists, it is unique.
//
// For testing purposes only.
//
// Do not call .Report() on the returned breaker. The probe manages the lifecycle
// of the breaker.
func (rpcCtx *Context) GetBreakerForAddr(
	nodeID roachpb.NodeID, class ConnectionClass, addr net.Addr,
) (*circuitbreaker.Breaker, bool) {
	sAddr := addr.String()
	rpcCtx.peers.mu.RLock()
	defer rpcCtx.peers.mu.RUnlock()
	p, ok := rpcCtx.peers.mu.m[peerKey{
		TargetAddr: sAddr,
		NodeID:     nodeID,
		Class:      class,
	}]
	if !ok {
		return nil, false
	}
	return p.b, true
}

// GetClientTLSConfig decides which TLS client configuration (&
// certificates) to use to reach the remote node.
func (rpcCtx *Context) GetClientTLSConfig() (*tls.Config, error) {
	if rpcCtx.config.Insecure {
		return nil, nil
	}

	cm, err := rpcCtx.GetCertificateManager()
	if err != nil {
		return nil, wrapError(err)
	}

	switch {
	case rpcCtx.ClientOnly:
		// A CLI command is performing a remote RPC.
		tlsCfg, err := cm.GetClientTLSConfig(rpcCtx.User)
		return tlsCfg, wrapError(err)

	case rpcCtx.UseNodeAuth || rpcCtx.tenID.IsSystem():
		tlsCfg, err := cm.GetNodeClientTLSConfig()
		return tlsCfg, wrapError(err)

	case !rpcCtx.tenID.IsSystem():
		// A SQL server running in a standalone server doesn't have access
		// to the node certs, and thus must use the standalone tenant
		// client cert.
		tlsCfg, err := cm.GetTenantTLSConfig()
		return tlsCfg, wrapError(err)

	default:
		// We don't currently support any other way to use the rpc context.
		// go away.
		return nil, errors.AssertionFailedf("programming error: rpc context not initialized correctly")
	}
}

// dialOptsNetworkCredentials computes options that determines how the
// RPC client authenticates itself to the remote server.
func (rpcCtx *Context) dialOptsNetworkCredentials() ([]grpc.DialOption, error) {
	var dialOpts []grpc.DialOption
	if rpcCtx.ContextOptions.Insecure {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		tlsConfig, err := rpcCtx.GetClientTLSConfig()
		if err != nil {
			return nil, err
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}

	return dialOpts, nil
}

type statsTracker struct {
	m localityMetrics
}

func (t *statsTracker) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }

func (t *statsTracker) HandleRPC(_ context.Context, s stats.RPCStats) {
	if out, ok := s.(*stats.OutPayload); ok {
		t.m.ConnectionBytesSent.Inc(int64(out.WireLength))
	}
	if in, ok := s.(*stats.InPayload); ok {
		t.m.ConnectionBytesRecv.Inc(int64(in.WireLength))
	}
}

func (t *statsTracker) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }

func (t *statsTracker) HandleConn(ctx context.Context, s stats.ConnStats) {
	if _, ok := s.(*stats.ConnEnd); ok {
		t.m.ConnectionConnected.Inc(-1)
	}
	if _, ok := s.(*stats.ConnBegin); ok {
		t.m.ConnectionConnected.Inc(1)
	}
}

// dialOptsNetwork compute options used only for over-the-network RPC
// connections.
func (rpcCtx *Context) dialOptsNetwork(
	ctx context.Context, target string, class ConnectionClass,
) ([]grpc.DialOption, error) {
	dialOpts, err := rpcCtx.dialOptsNetworkCredentials()
	if err != nil {
		return nil, err
	}

	// Request request compression. Note that it's the client that
	// decides to opt into compressions; the server accepts either
	// compressed or decompressed payloads, and the specific codec used
	// is named in the request (so different clients can use different
	// compression algorithms.)
	//
	// On a related note, this configuration uses our own snappy codec.
	// We believe it works better than the gzip codec provided natively
	// by grpc, although the specific reason is now lost to history. It
	// would be possible to change/simplify this, and since it's for
	// each client to decide changing this will not require much
	// cross-version compatibility dance.
	if rpcCtx.rpcCompression {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor((snappyCompressor{}).Name())))
	}

	// GRPC uses the HTTPS_PROXY environment variable by default[1]. This is
	// surprising, and likely undesirable for CRDB because it turns the proxy
	// into an availability risk and a throughput bottleneck. We disable the use
	// of proxies by default.
	//
	// [1]: https://github.com/grpc/grpc-go/blob/c0736608/Documentation/proxy.md
	dialOpts = append(dialOpts, grpc.WithNoProxy())

	// Lower the MaxBackoff (which defaults to ~minutes) to something in the
	// ~second range. Note that we only retry once (see onlyOnceDialer) so we only
	// hit the first backoff (BaseDelay). Note also that this delay serves as a
	// sort of circuit breaker, since it will make sure that we're not trying to
	// dial a down node in a tight loop. This is not a great mechanism but it's
	// what we have right now. Higher levels have some protection (node dialer
	// circuit breakers) but not all connection attempts go through that.
	backoffConfig := backoff.DefaultConfig

	// We need to define a MaxBackoff but it should never be used due to
	// our setup with onlyOnceDialer below. So note that our choice here is
	// inconsequential assuming all works as designed.
	bo := time.Second
	if bo > base.DialTimeout {
		// This is for testing where we set a small DialTimeout. gRPC will
		// internally round up the min connection timeout to the max backoff. This
		// can be unintuitive and so we opt out of it by lowering the max backoff.
		bo = base.DialTimeout
	}
	backoffConfig.BaseDelay = bo
	backoffConfig.MaxDelay = bo
	dialOpts = append(dialOpts, grpc.WithConnectParams(grpc.ConnectParams{
		Backoff:           backoffConfig,
		MinConnectTimeout: base.DialTimeout}))

	// Ensure the TCP link remains active so that overzealous firewalls
	// don't shut it down.
	dialOpts = append(dialOpts, grpc.WithKeepaliveParams(clientKeepalive))

	// Append a testing stream interceptor, if so configured.
	//
	// Note that we cannot do this earlier (e.g. in dialOptsNetwork)
	// because at that time the target address may not be known yet.
	if rpcCtx.Knobs.StreamClientInterceptor != nil {
		testingStreamInterceptor := rpcCtx.Knobs.StreamClientInterceptor(target, class)
		if testingStreamInterceptor != nil {
			dialOpts = append(dialOpts, grpc.WithChainStreamInterceptor(testingStreamInterceptor))
		}
	}

	// Set up the dialer. Like for the stream client interceptor, we cannot
	// do this earlier because it is sensitive to the actual target address,
	// which is only definitely provided during dial.
	dialer := onlyOnceDialer{}
	dialerFunc := dialer.dial
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
	dialOpts = append(dialOpts, grpc.WithContextDialer(dialerFunc))

	// Don't retry on dial errors either, otherwise the onlyOnceDialer will get
	// into a bad state for connection errors.
	dialOpts = append(dialOpts, grpc.FailOnNonTempDialError(true))

	return dialOpts, nil
}

// dialOptsCommon computes options used for both in-memory and
// over-the-network RPC connections.
func (rpcCtx *Context) dialOptsCommon(
	ctx context.Context, target string, class ConnectionClass,
) ([]grpc.DialOption, error) {
	// The limiting factor for lowering the max message size is the fact
	// that a single large kv can be sent over the network in one message.
	// Our maximum kv size is unlimited, so we need this to be very large.
	//
	// TODO(peter,tamird): need tests before lowering.
	dialOpts := []grpc.DialOption{grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(math.MaxInt32),
		grpc.MaxCallSendMsgSize(math.MaxInt32),
	)}

	if rpcCtx.clientCreds != nil {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(rpcCtx.clientCreds))
	}

	// We throw this one in for good measure, but it only disables the retries
	// for RPCs that were already pending (which are opt in anyway, and we don't
	// opt in). It doesn't disable what gRPC calls "transparent retries" (RPC
	// was not yet put on the wire when the error occurred). So we should
	// consider this one a no-op, though it can't hurt.
	dialOpts = append(dialOpts, grpc.WithDisableRetry())

	// Configure the window sizes with optional env var overrides.
	dialOpts = append(dialOpts,
		grpc.WithInitialConnWindowSize(rpcCtx.initialConnWindowSize(ctx)),
		grpc.WithInitialWindowSize(rpcCtx.initialWindowSize(ctx)),
	)
	unaryInterceptors := rpcCtx.clientUnaryInterceptors
	unaryInterceptors = unaryInterceptors[:len(unaryInterceptors):len(unaryInterceptors)]
	if rpcCtx.Knobs.UnaryClientInterceptor != nil {
		if interceptor := rpcCtx.Knobs.UnaryClientInterceptor(
			target, class,
		); interceptor != nil {
			unaryInterceptors = append(unaryInterceptors, interceptor)
		}
	}
	if len(unaryInterceptors) > 0 {
		dialOpts = append(dialOpts, grpc.WithChainUnaryInterceptor(unaryInterceptors...))
	}
	if len(rpcCtx.clientStreamInterceptors) > 0 {
		dialOpts = append(dialOpts, grpc.WithChainStreamInterceptor(rpcCtx.clientStreamInterceptors...))
	}
	return dialOpts, nil
}

// ClientInterceptors returns the client interceptors that the Context uses on
// RPC calls. They are exposed so that RPC calls that bypass the Context (i.e.
// the ones done locally through the internalClientAdapater) can use the same
// interceptors.
func (rpcCtx *Context) ClientInterceptors() ClientInterceptorInfo {
	return ClientInterceptorInfo{
		UnaryInterceptors:  rpcCtx.clientUnaryInterceptors,
		StreamInterceptors: rpcCtx.clientStreamInterceptors,
	}
}

// growStackCodec wraps the default grpc/encoding/proto codec to detect
// BatchRequest rpcs and grow the stack prior to Unmarshaling.
type growStackCodec struct {
	encoding.Codec
}

// Unmarshal detects BatchRequests and calls growstack.Grow before calling
// through to the underlying codec.
func (c growStackCodec) Unmarshal(data []byte, v interface{}) error {
	if _, ok := v.(*kvpb.BatchRequest); ok {
		growstack.Grow()
	}
	return c.Codec.Unmarshal(data, v)
}

// Install the growStackCodec over the default proto codec in order to grow the
// stack for BatchRequest RPCs prior to unmarshaling.
func init() {
	encoding.RegisterCodec(growStackCodec{Codec: codec{}})
}

// onlyOnceDialer implements the grpc.WithDialer interface but only
// allows a single connection attempt. It does this by marking all
// errors as not temporary. In addition to this, if the first
// invocation results in an error, that error is propagated to
// the second invocation.
//
// The problem onlyOnceDialer solves is that if we let gRPC reconnect
// transparently, it may do so without us realizing that the connection
// even had an issue. So the following (and variants of it) may happen:
//
// - we are n2
// - connection is to n1 at ip1
// - n1 stops, customer wipes n1, installs an ancient binary, starts again
// - grpc transparently redials, now we have a connection to n2, a totally different binary
// - n2 sends a bunch of random RPCs to it
// - only on the next heartbeat will we re-validate versions and tear down the connection
//
// We need a reconnection attempt to come with a blocking revalidation
// of all the checks that are part of our heartbeats.
//
// We can't configure gRPC to avoid internal retries, so we give it a
// onlyOnceDialer which fails permanently on any error. In theory, we could
// allow gRPC to reconnect if we instead used a "notifying dialer" that
// immediately (temporarily) poisoned the `*peer` when a reconnection attempt is
// started by gRPC. If we wanted to go down that route, we have to make sure we
// retain the observability for connection hiccups (which is right now provided
// by the forced top-level reconnect) and also avoid complexity creep in our
// heartbeat loop (which is already relatively complex).
type onlyOnceDialer struct {
	mu struct {
		syncutil.Mutex
		err      error
		redialed bool
	}
}

type notTemporaryError struct {
	error
}

func (nte *notTemporaryError) Temporary() bool {
	return false
}

func (ood *onlyOnceDialer) dial(ctx context.Context, addr string) (net.Conn, error) {
	ood.mu.Lock()
	defer ood.mu.Unlock()

	if err := ood.mu.err; err != nil {
		if ood.mu.redialed {
			// We set up onlyOnceDialer to avoid returning any errors that could look
			// temporary to gRPC, and so we don't expect it to re-dial a connection
			// twice (the first re-dial is supposed to surface the permanent error).
			return nil, &notTemporaryError{errors.NewAssertionErrorWithWrappedErrf(err, "gRPC connection unexpectedly re-dialed")}
		}
		ood.mu.redialed = true
		return nil, err
	}

	// First dial.

	dialer := net.Dialer{
		LocalAddr: sourceAddr,
	}

	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		// Return an error and make sure it's not marked as temporary, so that
		// ideally we don't even use the dialer again. (If the caller still does
		// it's fine, but it will cause more confusing error logging, etc).
		err = &notTemporaryError{error: err}
		ood.mu.err = err
		return nil, err
	}
	ood.mu.err = grpcutil.ErrConnectionInterrupted
	return conn, nil
}

type dialerFunc func(context.Context, string) (net.Conn, error)

type artificialLatencyDialer struct {
	dialerFunc dialerFunc
	latency    time.Duration
	enabled    func() bool
}

func (ald *artificialLatencyDialer) dial(ctx context.Context, addr string) (net.Conn, error) {
	conn, err := ald.dialerFunc(ctx, addr)
	if err != nil {
		return conn, err
	}
	return &delayingConn{
		Conn:    conn,
		latency: ald.latency,
		enabled: ald.enabled,
		readBuf: new(bytes.Buffer),
	}, nil
}

type delayingListener struct {
	net.Listener
	enabled func() bool
}

// NewDelayingListener creates a net.Listener that introduces a set delay on its connections.
func NewDelayingListener(l net.Listener, enabled func() bool) net.Listener {
	return &delayingListener{Listener: l, enabled: enabled}
}

func (d *delayingListener) Accept() (net.Conn, error) {
	c, err := d.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &delayingConn{
		Conn: c,
		// Put a default latency as the server's conn. This value will get populated
		// as packets are exchanged across the delayingConnections.
		latency: time.Duration(0) * time.Millisecond,
		readBuf: new(bytes.Buffer),
		enabled: d.enabled,
	}, nil
}

// delayingConn is a wrapped net.Conn that introduces a fixed delay into all
// writes to the connection. The implementation works by specifying a timestamp
// at which the other end of the connection is allowed to read the data, and
// sending that timestamp across the network in a header packet. On the read
// side, a sleep until the timestamp is introduced after the data is read before
// the data is returned to the consumer.
//
// Note that the fixed latency here is a one-way latency, so if you want to
// simulate a round-trip latency of x milliseconds, you should use a delayingConn
// on both ends with x/2 milliseconds of latency.
type delayingConn struct {
	net.Conn
	enabled     func() bool
	latency     time.Duration
	lastSendEnd time.Time
	readBuf     *bytes.Buffer
}

func (d *delayingConn) getLatencyMS() int32 {
	if !d.isEnabled() {
		return 0
	}
	return int32(d.latency / time.Millisecond)
}

func (d *delayingConn) isEnabled() bool {
	return d.enabled == nil || d.enabled()
}

func (d *delayingConn) Write(b []byte) (n int, err error) {
	tNow := timeutil.Now()
	if d.lastSendEnd.Before(tNow) {
		d.lastSendEnd = tNow
	}
	hdr := delayingHeader{
		Magic:    magic,
		ReadTime: d.lastSendEnd.Add(d.latency).UnixNano(),
		Sz:       int32(len(b)),
		DelayMS:  d.getLatencyMS(),
	}
	if err := binary.Write(d.Conn, binary.BigEndian, hdr); err != nil {
		return n, err
	}
	x, err := d.Conn.Write(b)
	n += x
	return n, err
}

var errMagicNotFound = errors.New("didn't get expected magic bytes header")

func (d *delayingConn) Read(b []byte) (n int, err error) {
	if d.readBuf.Len() == 0 {
		var hdr delayingHeader
		if err := binary.Read(d.Conn, binary.BigEndian, &hdr); err != nil {
			return 0, err
		}
		// If we somehow don't get our expected magic, throw an error.
		if hdr.Magic != magic {
			return 0, errors.WithStack(errMagicNotFound)
		}

		// Once we receive our first packet with a DelayMS field set, we set our
		// delay to the expected delay that was sent on the write side. We only
		// want to set the latency the first time we receive a non-zero DelayMS
		// because there are cases (still not yet fully debugged, but which
		// occur when demo is run with the --insecure flag) where we set a
		// non-zero DelayMS which is then overwritten, in a subsequent call to
		// this function, with a zero value. Since the simulated latencies are
		// not dynamic, overwriting a non-zero value with a zero value is
		// never valid. Rather than perform the lengthy investigation to
		// determine why we're being called with a zero DelayMS after we've set
		// d.latency to a non-zero value, we instead key off of a zero value of
		// d.latency to indicate that d.latency has not yet been initialized.
		// Once it's initialized to a non-zero value, we won't update it again.
		if d.latency == 0 && hdr.DelayMS != 0 {
			d.latency = time.Duration(hdr.DelayMS) * time.Millisecond
		}
		if d.isEnabled() {
			defer time.Sleep(timeutil.Until(timeutil.Unix(0, hdr.ReadTime)))
		}
		if _, err := io.CopyN(d.readBuf, d.Conn, int64(hdr.Sz)); err != nil {
			return 0, err
		}
	}
	return d.readBuf.Read(b)
}

const magic = 0xfeedfeed

type delayingHeader struct {
	Magic    int64
	ReadTime int64
	Sz       int32
	DelayMS  int32
}

func (rpcCtx *Context) makeDialCtx(
	target string, remoteNodeID roachpb.NodeID, class ConnectionClass,
) context.Context {
	return rpcCtx.wrapCtx(rpcCtx.MasterCtx, target, remoteNodeID, class)
}

const RemoteNodeTag = "rnode"
const RemoteAddressTag = "raddr"
const Class = "class"
const RpcTag = "rpc"

func (rpcCtx *Context) wrapCtx(
	ctx context.Context, target string, remoteNodeID roachpb.NodeID, class ConnectionClass,
) context.Context {
	var rnodeID interface{} = remoteNodeID
	if remoteNodeID == 0 {
		rnodeID = redact.SafeString("?")
	}
	l := &logtags.Buffer{}
	l = l.Add(RemoteNodeTag, rnodeID)
	l = l.Add(RemoteAddressTag, target)
	l = l.Add(Class, class)
	l = l.Add(RpcTag, nil)
	return logtags.AddTags(ctx, l)
}

// grpcDialRaw connects to the remote node.
// The ctx passed as argument must be derived from rpcCtx.masterCtx, so
// that it respects the same cancellation policy.
func (rpcCtx *Context) grpcDialRaw(
	ctx context.Context, target string, class ConnectionClass, additionalOpts ...grpc.DialOption,
) (*grpc.ClientConn, error) {
	transport := tcpTransport
	if rpcCtx.ContextOptions.AdvertiseAddr == target && !rpcCtx.ClientOnly {
		// See the explanation on loopbackDialFn for an explanation about this.
		transport = loopbackTransport
	}
	dialOpts, err := rpcCtx.grpcDialOptionsInternal(ctx, target, class, transport)
	if err != nil {
		return nil, err
	}

	dialOpts = append(dialOpts, additionalOpts...)

	return grpc.DialContext(ctx, target, dialOpts...)
}

// GRPCUnvalidatedDial uses GRPCDialNode and disables validation of the
// node ID between client and server. This function should only be
// used with the gossip client and CLI commands which can talk to any
// node. This method implies a SystemClass.
func (rpcCtx *Context) GRPCUnvalidatedDial(target string, locality roachpb.Locality) *Connection {
	return rpcCtx.grpcDialNodeInternal(target, 0, locality, SystemClass)
}

// GRPCDialNode calls grpc.Dial with options appropriate for the
// context and class (see the comment on ConnectionClass).
//
// The remoteNodeID becomes a constraint on the expected node ID of
// the remote node; this is checked during heartbeats. The caller is
// responsible for ensuring the remote node ID is known prior to using
// this function.
func (rpcCtx *Context) GRPCDialNode(
	target string,
	remoteNodeID roachpb.NodeID,
	remoteLocality roachpb.Locality,
	class ConnectionClass,
) *Connection {
	if remoteNodeID == 0 && !rpcCtx.TestingAllowNamedRPCToAnonymousServer {
		log.Fatalf(
			rpcCtx.makeDialCtx(target, remoteNodeID, class),
			"%v", errors.AssertionFailedf("invalid node ID 0 in GRPCDialNode()"))
	}
	return rpcCtx.grpcDialNodeInternal(target, remoteNodeID, remoteLocality, class)
}

// GRPCDialPod wraps GRPCDialNode and treats the `remoteInstanceID`
// argument as a `NodeID` which it converts. This works because the
// tenant gRPC server is initialized using the `InstanceID` so it
// accepts our connection as matching the ID we're dialing.
//
// Since GRPCDialNode accepts a separate `target` and `NodeID` it
// requires no further modification to work between pods.
func (rpcCtx *Context) GRPCDialPod(
	target string,
	remoteInstanceID base.SQLInstanceID,
	remoteLocality roachpb.Locality,
	class ConnectionClass,
) *Connection {
	return rpcCtx.GRPCDialNode(target, roachpb.NodeID(remoteInstanceID), remoteLocality, class)
}

// grpcDialNodeInternal connects to the remote node and sets up the async heartbeater.
// This intentionally takes no `context.Context`; it uses one derived from rpcCtx.masterCtx.
func (rpcCtx *Context) grpcDialNodeInternal(
	target string,
	remoteNodeID roachpb.NodeID,
	remoteLocality roachpb.Locality,
	class ConnectionClass,
) *Connection {
	k := peerKey{TargetAddr: target, NodeID: remoteNodeID, Class: class}
	if p, ok := rpcCtx.peers.get(k); ok {
		// There's a cached peer, so we have a cached connection, use it.
		return p.c
	}

	// Slow path. Race to create a peer.
	conns := &rpcCtx.peers

	conns.mu.Lock()
	defer conns.mu.Unlock()

	if p, lostRace := conns.getRLocked(k); lostRace {
		return p.c
	}

	// Won race. Actually create a peer.

	if conns.mu.m == nil {
		conns.mu.m = map[peerKey]*peer{}
	}

	p := rpcCtx.newPeer(k, remoteLocality)
	// (Asynchronously) Start the probe (= heartbeat loop). The breaker is healthy
	// right now (it was just created) but the call to `.Probe` will launch the
	// probe[1] regardless.
	//
	// [1]: see (*peer).launch.
	p.b.Probe()

	// NB: we used to also insert into `peerKey{target, 0, class}` so that
	// callers that may pass a zero NodeID could coalesce onto the connection
	// with the "real" NodeID. This caused issues over the years[^1][^2] and
	// was never necessary anyway, so we don't do it anymore.
	//
	// [^1]: https://github.com/cockroachdb/cockroach/issues/37200
	// [^2]: https://github.com/cockroachdb/cockroach/pull/89539

	conns.mu.m[k] = p
	return p.snap().c
}

// NewHeartbeatService returns a HeartbeatService initialized from the Context.
func (rpcCtx *Context) NewHeartbeatService() *HeartbeatService {
	return &HeartbeatService{
		clock:                                 rpcCtx.Clock,
		remoteClockMonitor:                    rpcCtx.RemoteClocks,
		clusterName:                           rpcCtx.ClusterName(),
		disableClusterNameVerification:        rpcCtx.ContextOptions.DisableClusterNameVerification,
		clusterID:                             rpcCtx.StorageClusterID,
		nodeID:                                rpcCtx.NodeID,
		version:                               rpcCtx.Settings.Version,
		onHandlePing:                          rpcCtx.OnIncomingPing,
		testingAllowNamedRPCToAnonymousServer: rpcCtx.TestingAllowNamedRPCToAnonymousServer,
	}
}

//go:generate mockgen -destination=mocks_generated_test.go --package=. Dialbacker

type Dialbacker interface {
	GRPCUnvalidatedDial(string, roachpb.Locality) *Connection
	GRPCDialNode(string, roachpb.NodeID, roachpb.Locality, ConnectionClass) *Connection
	grpcDialRaw(
		context.Context, string, ConnectionClass, ...grpc.DialOption,
	) (*grpc.ClientConn, error)
	wrapCtx(
		ctx context.Context, target string, remoteNodeID roachpb.NodeID, class ConnectionClass,
	) context.Context
}

// VerifyDialback verifies connectivity from the recipient of a PingRequest back
// to the sender. If a system-class connection to the sender is healthy or no
// dialback is requested, the method returns success immediately.
//
// Otherwise, the outcome depends on the health of the system-class connection:
//
//   - for a blocking ping, a one-off connection to the sender is established
//     synchronously, and success is returned if and only if this dial-back
//     attempt succeeded.
//   - for a non-blocking ping, returns the health state of the system-class
//     connection, with the exception of ErrNotHeartbeated, which maps to `nil`.
func VerifyDialback(
	ctx context.Context,
	rpcCtx Dialbacker,
	request *PingRequest,
	_ *PingResponse,
	locality roachpb.Locality,
	sv *settings.Values,
) error {
	if request.NeedsDialback == PingRequest_NONE || !enableRPCCircuitBreakers.Get(sv) {
		return nil
	}

	baseAddr := util.UnresolvedAddr{NetworkField: "tcp", AddressField: request.OriginAddr}
	target := locality.LookupAddress(request.LocalityAddress, &baseAddr).AddressField

	// As a fast-path, determine if we have a healthy system-class connection to
	// the node that sent us the ping. We use the System class because that is
	// what is important from a liveness perspective - if we are unable to
	// maintain a healthy connection on the System class, other connections
	// are bound to fail, too.
	//
	// Note that the health check will also trigger a non-blocking dial of the
	// connection if it hasn't been dialed before, and that this connection is
	// stateful even across disconnects, i.e. if the connection fails, the health
	// check will return an error until the connection is re-established.
	var connHealthErr error
	if request.OriginNodeID == 0 {
		// The incoming connection was initiated using rpcCtx.GRPCUnvalidatedDial,
		// so we don't know the origin's NodeID and use gRPCUnvalidatedDial to
		// inform the fast path as well.
		connHealthErr = rpcCtx.GRPCUnvalidatedDial(target, roachpb.Locality{}).Health() // NB: dials SystemClass
	} else {
		connHealthErr = rpcCtx.GRPCDialNode(target, request.OriginNodeID, roachpb.Locality{}, SystemClass).Health()
	}

	// We have a successful connection so report success. Any ongoing attempts no
	// longer need to be tracked.
	if connHealthErr == nil {
		return nil
	}

	if request.NeedsDialback == PingRequest_NON_BLOCKING {
		if errors.Is(connHealthErr, ErrNotHeartbeated) {
			return nil
		}
		return connHealthErr
	} else {
		log.VEventf(ctx, 2, "unable to verify health on existing conn, trying dialback conn to %s, n%d mode %v, %v",
			target, request.OriginNodeID, request.NeedsDialback, connHealthErr)

		// Since we don't have a successful reverse connection, try and dial back
		// manually. We don't use the regular dialer pool to avoid a circular dependency:
		// Dialing through the pool starts with a BLOCKING connection, which the remote
		// side would try to dial back, which would call into VerifyDialback for this
		// connection again, etc, for an infinite loop of blocking connections.
		// A throwaway connection keeps it simple.
		ctx := rpcCtx.wrapCtx(ctx, target, request.OriginNodeID, SystemClass)
		ctx = logtags.AddTag(ctx, "dialback", nil)
		conn, err := rpcCtx.grpcDialRaw(ctx, target, SystemClass, grpc.WithBlock())
		if conn != nil { // NB: the nil check simplifies mocking in TestVerifyDialback
			_ = conn.Close() // nolint:grpcconnclose
		}
		if err != nil {
			log.Infof(ctx, "blocking dialback connection failed to %s, n%d, %v", target, request.OriginNodeID, err)
			return err
		}
		log.VEventf(ctx, 2, "blocking dialback connection to n%d succeeded", request.OriginNodeID)
		return nil
	}
}
