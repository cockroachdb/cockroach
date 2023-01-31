// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
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

// startListenRPCAndSQL starts the RPC and SQL listeners.
// It returns the SQL listener, which can be used
// to start the SQL server when initialization has completed.
// It also returns a function that starts the RPC server,
// when the cluster is known to have bootstrapped or
// when waiting for init().
// This does not start *accepting* connections just yet.
func startListenRPCAndSQL(
	ctx, workersCtx context.Context,
	cfg BaseConfig,
	stopper *stop.Stopper,
	grpc *grpcServer,
	enableSQLListener bool,
) (
	sqlListener net.Listener,
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
		ln, err = ListenAndUpdateAddrs(ctx, &cfg.Addr, &cfg.AdvertiseAddr, rpcChanName)
		if err != nil {
			return nil, nil, nil, err
		}
		log.Eventf(ctx, "listening on port %s", cfg.Addr)
	}

	var pgL net.Listener
	if cfg.SplitListenSQL && enableSQLListener {
		pgL, err = ListenAndUpdateAddrs(ctx, &cfg.SQLAddr, &cfg.SQLAdvertiseAddr, "sql")
		if err != nil {
			return nil, nil, nil, err
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
			return nil, nil, nil, err
		}
		log.Eventf(ctx, "listening on sql port %s", cfg.SQLAddr)
	}

	// serveOnMux is used to ensure that the mux gets listened on eventually,
	// either via the returned startRPCServer() or upon stopping.
	var serveOnMux sync.Once

	m := cmux.New(ln)

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
			return nil, nil, nil, errors.Wrapf(err, "internal error")
		}
	}

	anyL := m.Match(cmux.Any())
	if serverTestKnobs, ok := cfg.TestingKnobs.Server.(*TestingKnobs); ok {
		if serverTestKnobs.ContextTestingKnobs.InjectedLatencyOracle != nil {
			anyL = rpc.NewDelayingListener(anyL, serverTestKnobs.ContextTestingKnobs.InjectedLatencyEnabled)
		}
	}

	loopbackL := netutil.NewLoopbackListener(ctx, stopper)

	// The remainder shutdown worker.
	waitForQuiesce := func(context.Context) {
		<-stopper.ShouldQuiesce()
		// TODO(bdarnell): Do we need to also close the other listeners?
		netutil.FatalIfUnexpected(anyL.Close())
		netutil.FatalIfUnexpected(loopbackL.Close())
		netutil.FatalIfUnexpected(ln.Close())
	}

	// cmux auto-retries Accept() by default. Tell it
	// to stop doing work if we see a request to shut down.
	m.HandleError(func(err error) bool {
		select {
		case <-stopper.ShouldQuiesce():
			log.Infof(ctx, "server shutting down: instructing cmux to stop accepting")
			return false
		default:
			return true
		}
	})
	stopper.AddCloser(stop.CloserFn(func() {
		grpc.Stop()
		serveOnMux.Do(func() {
			// The cmux matches don't shut down properly unless serve is called on the
			// cmux at some point. Use serveOnMux to ensure it's called during shutdown
			// if we wouldn't otherwise reach the point where we start serving on it.
			netutil.FatalIfUnexpected(m.Serve())
		})
	}))
	if err := stopper.RunAsyncTask(
		workersCtx, "grpc-quiesce", waitForQuiesce,
	); err != nil {
		return nil, nil, nil, err
	}

	// startRPCServer starts the RPC server. We do not do this
	// immediately because we want the cluster to be ready (or ready to
	// initialize) before we accept RPC requests. The caller
	// (Server.Start) will call this at the right moment.
	startRPCServer = func(ctx context.Context) {
		// Serve the gRPC endpoint.
		_ = stopper.RunAsyncTask(workersCtx, "serve-grpc", func(context.Context) {
			netutil.FatalIfUnexpected(grpc.Serve(anyL))
		})
		_ = stopper.RunAsyncTask(workersCtx, "serve-loopback-grpc", func(context.Context) {
			netutil.FatalIfUnexpected(grpc.Serve(loopbackL))
		})

		_ = stopper.RunAsyncTask(ctx, "serve-mux", func(context.Context) {
			serveOnMux.Do(func() {
				netutil.FatalIfUnexpected(m.Serve())
			})
		})
	}

	return pgL, loopbackL.Connect, startRPCServer, nil
}
