// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

// ClientConnConfig contains the configuration for a ClientConn.
type ClientConnConfig struct {
	// Tracer is an optional tracer.
	// May be nil.
	Tracer *tracing.Tracer

	// Settings are the cluster settings to use.
	Settings *cluster.Settings

	// ServerAddr is the server address to connect to.
	ServerAddr string

	// Insecure specifies whether to use an insecure connection.
	Insecure bool

	// SSLCertsDir is the directory containing the SSL certificates to use.
	SSLCertsDir string

	// User is the username to use.
	User username.SQLUsername

	// ClusterName is the name of the cluster to connect to.
	// This is used to verify the server's identity.
	ClusterName string

	// DisableClusterNameVerification can be set to avoid checking
	// the cluser name.
	DisableClusterNameVerification bool

	// TestingKnobs are optional testing knobs.
	TestingKnobs ContextTestingKnobs

	// RPCHeartbeatInterval is the interval for sending a heartbeat.
	RPCHeartbeatInterval time.Duration
	// RPCHeartbeatTimeout is the timeout for a heartbeat.
	RPCHeartbeatTimeout time.Duration

	// HistogramWindowInterval is the duration of the sliding window used to
	// measure clock offsets.
	HistogramWindowInterval time.Duration

	// Clock is the HLC clock to use. This can be nil to disable
	// maxoffset verification.
	Clock *hlc.Clock
}

// MakeClientConnConfigFromBaseConfig creates a ClientConnConfig from a
// base.Config.
func MakeClientConnConfigFromBaseConfig(
	cfg base.Config,
	user username.SQLUsername,
	tracer *tracing.Tracer,
	settings *cluster.Settings,
	clock *hlc.Clock,
	knobs ContextTestingKnobs,
) ClientConnConfig {
	return ClientConnConfig{
		Tracer:                         tracer,
		Settings:                       settings,
		User:                           user,
		TestingKnobs:                   knobs,
		Clock:                          clock,
		ServerAddr:                     cfg.AdvertiseAddr,
		Insecure:                       cfg.Insecure,
		SSLCertsDir:                    cfg.SSLCertsDir,
		ClusterName:                    cfg.ClusterName,
		DisableClusterNameVerification: cfg.DisableClusterNameVerification,
		RPCHeartbeatInterval:           cfg.RPCHeartbeatInterval,
		RPCHeartbeatTimeout:            cfg.RPCHeartbeatTimeout,
		HistogramWindowInterval:        cfg.HistogramWindowInterval(),
	}
}

// NewClientContext creates a new client context.
func NewClientContext(ctx context.Context, cfg ClientConnConfig) (*Context, *stop.Stopper) {
	tracer := cfg.Tracer
	if tracer == nil {
		tracer = tracing.NewTracer()
	}
	stopper := stop.NewStopper(stop.WithTracer(tracer))
	opts := ContextOptions{
		// Setting TenantID this way here is intentional. It means
		// that the TLS cert selection will not attempt to use
		// a tenant server cert to connect to the remote server,
		// and instead pick up a regular client cert.
		TenantID: roachpb.SystemTenantID,

		Stopper:  stopper,
		Settings: cfg.Settings,

		ClientOnly:                     true,
		Insecure:                       cfg.Insecure,
		SSLCertsDir:                    cfg.SSLCertsDir,
		ClusterName:                    cfg.ClusterName,
		DisableClusterNameVerification: cfg.DisableClusterNameVerification,
		RPCHeartbeatInterval:           cfg.RPCHeartbeatInterval,
		RPCHeartbeatTimeout:            cfg.RPCHeartbeatTimeout,
		HistogramWindowInterval:        cfg.HistogramWindowInterval,
		User:                           cfg.User,
		Knobs:                          cfg.TestingKnobs,
		AdvertiseAddrH:                 &base.AdvertiseAddrH{},
		SQLAdvertiseAddrH:              &base.SQLAdvertiseAddrH{},
	}

	if cfg.Clock == nil {
		opts.Clock = &timeutil.DefaultTimeSource{}
		opts.ToleratedOffset = time.Nanosecond
	} else {
		opts.Clock = cfg.Clock.WallClock()
		opts.ToleratedOffset = cfg.Clock.MaxOffset()
	}

	// Client connections never use loopback dialing.
	opts.Knobs.NoLoopbackDialer = true

	return NewContext(ctx, opts), stopper
}

// NewClientConn creates a new client connection.
// The caller is responsible for calling the returned function
// to release associated resources.
func NewClientConn(
	ctx context.Context, cfg ClientConnConfig,
) (conn *grpc.ClientConn, cleanup func(), err error) {
	if ctx.Done() == nil {
		return nil, nil, errors.New("context must be cancellable")
	}
	rpcContext, stopper := NewClientContext(ctx, cfg)
	closer := func() {
		// We use context.Background() here and not ctx because we
		// want to ensure that the closers always run to completion
		// even if the context used to create the client conn is
		// canceled.
		stopper.Stop(context.Background())
	}
	defer func() {
		if err != nil {
			closer()
		}
	}()

	addr, err := addr.AddrWithDefaultLocalhost(cfg.ServerAddr)
	if err != nil {
		return nil, nil, err
	}
	// We use GRPCUnvalidatedDial() here because it does not matter
	// to which node we're talking to.
	conn, err = rpcContext.GRPCUnvalidatedDial(addr, roachpb.Locality{}).Connect(ctx)
	if err != nil {
		return nil, nil, err
	}
	stopper.AddCloser(stop.CloserFn(func() {
		_ = conn.Close() // nolint:grpcconnclose
	}))

	return conn, closer, nil
}
