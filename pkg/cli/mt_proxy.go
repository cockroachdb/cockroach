// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cli

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/errors"

	"github.com/cockroachdb/cockroach/pkg/util/stop"

	"golang.org/x/sys/unix"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/spf13/cobra"
)

var proxyOpts sqlproxyccl.ProxyOptions

var mtStartSQLProxyCmd = &cobra.Command{
	Use:   "start-proxy <basepath>",
	Short: "start-proxy host:port",
	Long: `Starts a SQL proxy.

This proxy accepts incoming connections and relays them to a backend server
determined by the arguments used.
`,
	RunE: MaybeDecorateGRPCError(runStartSQLProxy),
	Args: cobra.NoArgs,
}

func init() {
	f := mtStartSQLProxyCmd.Flags()
	f.StringVar(&proxyOpts.Denylist, "denylist-file", "",
		"Denylist file to limit access to IP addresses and tenant ids.")
	f.StringVar(&proxyOpts.ListenAddr, "listen-addr", "127.0.0.1:46257",
		"Listen address for incoming connections.")
	f.StringVar(&proxyOpts.ListenCert, "listen-cert", "",
		"File containing PEM-encoded x509 certificate for listen address.")
	f.StringVar(&proxyOpts.ListenKey, "listen-key", "",
		"File containing PEM-encoded x509 key for listen address.")
	f.StringVar(&proxyOpts.MetricsAddress, "listen-metrics", "0.0.0.0:8080",
		"Listen address for incoming connections.")
	f.StringVar(&proxyOpts.RoutingRule, "routing-rule", "",
		"Routing rule for incoming connections. Use '{{clusterName}}' for substitution.")
	f.StringVar(&proxyOpts.DirectoryAddr, "directory", "",
		"Directory address for resolving from backend id to IP.")
	f.BoolVar(&proxyOpts.SkipVerify, "skip-verify", false,
		"If true, skip identity verification of backend. For testing only.")
	f.BoolVar(&proxyOpts.Insecure, "insecure", false,
		"If true, use insecure connection to the backend.")
	f.DurationVar(&proxyOpts.RatelimitBaseDelay, "ratelimit-base-delay", 50*time.Millisecond,
		"Initial backoff after a failed login attempt. Set to 0 to disable rate limiting.")
	f.DurationVar(&proxyOpts.ValidateAccessInterval, "validate-access-interval", 30*time.Second,
		"Time interval between validation that current connections are still valid.")
	f.DurationVar(&proxyOpts.PollConfigInterval, "poll-config-interval", 30*time.Second,
		"Polling interval changes in config file.")
	f.DurationVar(&proxyOpts.IdleTimeout, "idle-timeout", 0,
		"Close connections idle for this duration.")
}

func runStartSQLProxy(cmd *cobra.Command, args []string) (returnErr error) {
	// Initialize logging, stopper and context that can be canceled
	ctx, stopper, err := initLogging(cmd)
	if err != nil {
		return err
	}
	defer stopper.Stop(ctx)

	log.Infof(ctx, "New proxy with opts: %+v", proxyOpts)

	proxyLn, err := net.Listen("tcp", proxyOpts.ListenAddr)
	if err != nil {
		return err
	}
	defer func() { _ = proxyLn.Close() }()

	metricsLn, err := net.Listen("tcp", proxyOpts.MetricsAddress)
	if err != nil {
		return err
	}
	defer func() { _ = metricsLn.Close() }()

	handler, err := sqlproxyccl.NewProxyHandler(ctx, proxyOpts)
	if err != nil {
		return err
	}

	server := sqlproxyccl.NewServer(handler.Handle)

	errChan := make(chan error, 1)

	if err := stopper.RunAsyncTask(ctx, "serve-http", func(ctx context.Context) {
		log.Infof(ctx, "HTTP metrics server listening at %s", metricsLn.Addr())
		if err := server.ServeHTTP(ctx, metricsLn); err != nil {
			errChan <- err
		}
	}); err != nil {
		return err
	}

	if err := stopper.RunAsyncTask(ctx, "serve-proxy", func(ctx context.Context) {
		log.Infof(ctx, "proxy server listening at %s", proxyLn.Addr())
		if err := server.Serve(ctx, proxyLn); err != nil {
			errChan <- err
		}
	}); err != nil {
		return err
	}

	return waitForSignals(ctx, stopper, errChan)
}

func initLogging(cmd *cobra.Command) (ctx context.Context, stopper *stop.Stopper, err error) {
	// Remove the default store, which avoids using it to set up logging.
	// Instead, we'll default to logging to stderr unless --log-dir is
	// specified. This makes sense since the standalone SQL server is
	// at the time of writing stateless and may not be provisioned with
	// suitable storage.
	serverCfg.Stores.Specs = nil
	serverCfg.ClusterName = ""

	ctx = context.Background()
	stopper, err = setupAndInitializeLoggingAndProfiling(ctx, cmd, false /* isServerCmd */)
	if err != nil {
		return
	}
	ctx, _ = stopper.WithCancelOnQuiesce(ctx)
	return
}

func waitForSignals(ctx context.Context, stopper *stop.Stopper, errChan chan error) (returnErr error) {
	// Need to alias the signals if this has to run on non-unix OSes too.
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, unix.SIGINT, unix.SIGTERM)

	// Dump the stacks when QUIT is received.
	quitSignalCh := make(chan os.Signal, 1)
	signal.Notify(quitSignalCh, unix.SIGQUIT)
	go func() {
		for {
			<-quitSignalCh
			log.DumpStacks(context.Background())
		}
	}()

	select {
	case err := <-errChan:
		log.StartAlwaysFlush()
		return err
	case <-stopper.ShouldQuiesce():
		// Stop has been requested through the stopper's Stop
		<-stopper.IsStopped()
		// StartAlwaysFlush both flushes and ensures that subsequent log
		// writes are flushed too.
		log.StartAlwaysFlush()
	case sig := <-signalCh: // INT or TERM
		log.StartAlwaysFlush() // In case the caller follows up with KILL
		log.Ops.Infof(ctx, "received signal '%s'", sig)
		if sig == os.Interrupt {
			returnErr = errors.New("interrupted")
		}
		go func() {
			log.Infof(ctx, "server stopping")
			stopper.Stop(ctx)
		}()
	case <-log.FatalChan():
		stopper.Stop(ctx)
		select {} // Block and wait for logging go routine to shut down the process
	}

	for {
		select {
		case sig := <-signalCh:
			switch sig {
			case os.Interrupt: // SIGTERM after SIGTERM
				log.Ops.Infof(ctx, "received additional signal '%s'; continuing graceful shutdown", sig)
				continue
			}

			log.Ops.Shoutf(ctx, severity.ERROR,
				"received signal '%s' during shutdown, initiating hard shutdown", log.Safe(sig))
			panic("terminate")
		case <-stopper.IsStopped():
			const msgDone = "server shutdown completed"
			log.Ops.Infof(ctx, msgDone)
			fmt.Fprintln(os.Stdout, msgDone)
		}
		break
	}

	return returnErr
}
