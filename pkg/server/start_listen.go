// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"sync"

	"github.com/cockroachdb/cmux"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

type RPCListenerFactory func(
	ctx context.Context,
	addr, advertiseAddr *string,
	connName string,
	acceptProxyProtocolHeaders bool,
) (net.Listener, error)

// startListenRPCAndSQL starts the RPC and SQL listeners. It returns:
//   - The listener for pgwire connections coming over the network. This will be used
//     to start the SQL server when initialization has completed.
//   - The listener for internal sql connections running over our pipes interface.
//   - A dialer function that can be used to open a connection to the RPC loopback interface.
//   - A function that starts the RPC server, when the cluster is known to have
//     bootstrapped or when waiting for init().
//
// This does not start *accepting* connections just yet.
func startListenRPCAndSQL(
	ctx, workersCtx context.Context,
	cfg BaseConfig,
	stopper *stop.Stopper,
	grpc *grpcServer,
	rpcListenerFactory RPCListenerFactory,
	enableSQLListener bool,
	acceptProxyProtocolHeaders bool,
) (
	sqlListener net.Listener,
	pgLoopbackListener *netutil.LoopbackListener,
	rpcLoopbackDial func(context.Context) (net.Conn, error),
	startRPCServer func(ctx context.Context),
	err error,
) {
	rpcChanName := "rpc/sql"
	if cfg.SplitListenSQL || !enableSQLListener {
		rpcChanName = "rpc"
	}
	var ln net.Listener
	if k := cfg.TestingKnobs.Server; k != nil {
		knobs := k.(*TestingKnobs)
		ln = knobs.RPCListener
	}
	if ln == nil {
		var err error
		ln, err = rpcListenerFactory(ctx, &cfg.Addr, &cfg.AdvertiseAddr, rpcChanName, acceptProxyProtocolHeaders)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		log.Eventf(ctx, "listening on port %s", cfg.Addr)
	}

	var pgL net.Listener
	if cfg.SplitListenSQL && enableSQLListener {
		if cfg.SQLAddrListener == nil {
			pgL, err = ListenAndUpdateAddrs(ctx, &cfg.SQLAddr, &cfg.SQLAdvertiseAddr, "sql", acceptProxyProtocolHeaders)
		} else {
			pgL = cfg.SQLAddrListener
		}
		if err != nil {
			return nil, nil, nil, nil, err
		}
		// The SQL listener shutdown worker, which closes everything under
		// the SQL port when the stopper indicates we are shutting down.
		waitQuiesce := func(ctx context.Context) {
			<-stopper.ShouldQuiesce()
			// NB: we can't do this as a Closer because (*Server).ServeWith is
			// running in a worker and usually sits on accept() which unblocks
			// only when the listener closes. In other words, the listener needs
			// to close when quiescing starts to allow that worker to shut down.
			if err := pgL.Close(); err != nil {
				log.Ops.Fatalf(ctx, "%v", err)
			}
		}
		if err := stopper.RunAsyncTask(workersCtx, "wait-quiesce", waitQuiesce); err != nil {
			waitQuiesce(workersCtx)
			return nil, nil, nil, nil, err
		}
		log.Eventf(ctx, "listening on sql port %s", cfg.SQLAddr)
	}

	// serveOnMux is used to ensure that the mux gets listened on eventually,
	// either via the returned startRPCServer() or upon stopping.
	var serveOnMux sync.Once

	m := cmux.New(ln)
	// cmux auto-retries Accept() by default. Tell it
	// to stop doing work if we see a request to shut down.
	m.HandleError(func(err error) bool {
		select {
		case <-stopper.ShouldQuiesce():
			log.Infof(workersCtx, "server shutting down: instructing cmux to stop accepting")
			return false
		default:
			return true
		}
	})

	if !cfg.SplitListenSQL && enableSQLListener {
		// If the pg port is split, it will be opened above. Otherwise,
		// we make it hang off the RPC listener via cmux here.
		pgL = m.Match(func(r io.Reader) bool {
			return pgwire.Match(r)
		})
		// Also if the pg port is not split, the actual listen address for
		// SQL become equal to that of RPC.
		cfg.SQLAddr = cfg.Addr
		// Then we update the advertised addr with the right port, if
		// the port had been auto-allocated.
		if err := UpdateAddrs(ctx, &cfg.SQLAddr, &cfg.SQLAdvertiseAddr, ln.Addr()); err != nil {
			return nil, nil, nil, nil, errors.Wrapf(err, "internal error")
		}
	}

	// Host drpc only if it's _possible_ to turn it on (this requires a test build
	// or env var). If the setting _is_ on, then it was overridden in testing and
	// we want to host the server too.
	hostDRPC := rpc.ExperimentalDRPCEnabled.Validate(nil /* not used */, true) == nil ||
		rpc.ExperimentalDRPCEnabled.Get(&cfg.Settings.SV)

	// If we're not hosting drpc, make a listener that never accepts anything.
	// We will start the dRPC server all the same; it barely consumes any
	// resources.
	var drpcL net.Listener = &noopListener{make(chan struct{})}
	if hostDRPC {
		// Throw away the header before passing the conn to the drpc server. This
		// would not be required explicitly if we used `drpcmigrate.ListenMux` but
		// cmux keeps the prefix.
		drpcL = &dropDRPCHeaderListener{wrapped: m.Match(drpcMatcher)}
	}

	grpcL := m.Match(cmux.Any())
	if serverTestKnobs, ok := cfg.TestingKnobs.Server.(*TestingKnobs); ok {
		if serverTestKnobs.ContextTestingKnobs.InjectedLatencyOracle != nil {
			grpcL = rpc.NewDelayingListener(grpcL, serverTestKnobs.ContextTestingKnobs.InjectedLatencyEnabled)
			drpcL = rpc.NewDelayingListener(drpcL, serverTestKnobs.ContextTestingKnobs.InjectedLatencyEnabled)
		}
	}

	rpcLoopbackL := netutil.NewLoopbackListener(ctx, stopper)
	sqlLoopbackL := netutil.NewLoopbackListener(ctx, stopper)
	drpcCtx, drpcCancel := context.WithCancel(workersCtx)

	// The remainder shutdown worker.
	waitForQuiesce := func(context.Context) {
		<-stopper.ShouldQuiesce()
		drpcCancel()
		// TODO(bdarnell): Do we need to also close the other listeners?
		netutil.FatalIfUnexpected(grpcL.Close())
		netutil.FatalIfUnexpected(drpcL.Close())
		netutil.FatalIfUnexpected(rpcLoopbackL.Close())
		netutil.FatalIfUnexpected(sqlLoopbackL.Close())
		netutil.FatalIfUnexpected(ln.Close())
	}

	stopGRPC := func() {
		grpc.Stop()
		serveOnMux.Do(func() {
			// The cmux matches don't shut down properly unless serve is called on the
			// cmux at some point. Use serveOnMux to ensure it's called during shutdown
			// if we wouldn't otherwise reach the point where we start serving on it.
			netutil.FatalIfUnexpected(m.Serve())
		})
	}
	stopper.AddCloser(stop.CloserFn(stopGRPC))

	if err := stopper.RunAsyncTask(
		workersCtx, "grpc-drpc-quiesce", waitForQuiesce,
	); err != nil {
		waitForQuiesce(ctx)
		stopGRPC()
		drpcCancel()
		return nil, nil, nil, nil, err
	}
	stopper.AddCloser(stop.CloserFn(stopGRPC))

	// startRPCServer starts the RPC server. We do not do this
	// immediately because we want the cluster to be ready (or ready to
	// initialize) before we accept RPC requests. The caller
	// (Server.Start) will call this at the right moment.
	startRPCServer = func(ctx context.Context) {
		// Serve the gRPC endpoint.
		_ = stopper.RunAsyncTask(workersCtx, "serve-grpc", func(context.Context) {
			netutil.FatalIfUnexpected(grpc.Serve(grpcL))
		})
		_ = stopper.RunAsyncTask(drpcCtx, "serve-drpc", func(ctx context.Context) {
			if cfg := grpc.drpc.TLSCfg; cfg != nil {
				drpcTLSL := tls.NewListener(drpcL, cfg)
				netutil.FatalIfUnexpected(grpc.drpc.Srv.Serve(ctx, drpcTLSL))
			} else {
				netutil.FatalIfUnexpected(grpc.drpc.Srv.Serve(ctx, drpcL))
			}
		})
		_ = stopper.RunAsyncTask(workersCtx, "serve-loopback-grpc", func(context.Context) {
			netutil.FatalIfUnexpected(grpc.Serve(rpcLoopbackL))
		})

		_ = stopper.RunAsyncTask(ctx, "serve-mux", func(context.Context) {
			serveOnMux.Do(func() {
				netutil.FatalIfUnexpected(m.Serve())
			})
		})
	}

	return pgL, sqlLoopbackL, rpcLoopbackL.Connect, startRPCServer, nil
}
