// Copyright 2022 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"flag"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/httpproxy"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/ingest"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/obsutil"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/process"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/produce"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/queue"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/router"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/transform"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/validate"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	logspb "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/collector/logs/v1"
	_ "github.com/cockroachdb/cockroach/pkg/ui/distoss" // web UI init hooks
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
)

// drainSignals are the signals that will cause the server to drain and exit.
//
// The signals will initiate a graceful shutdown. If received a second time,
// SIGINT will be reraised without a signal handler and the default action
// terminate the process abruptly.
//
// Receiving SIGTERM a second time does not do a brutal shutdown, as SIGTERM is
// named termSignal below.
var drainSignals = []os.Signal{unix.SIGINT, unix.SIGTERM}

// termSignal is the signal that causes an idempotent graceful
// shutdown (i.e. second occurrence does not incur hard shutdown).
var termSignal os.Signal = unix.SIGTERM

// maxMemoryBytes is the max memory bytes to be used by memory queue.
// TODO(maryliag): make performance testing to decide on the final value.
var maxMemoryBytes int = 500 * 1024 * 1024 // 500Mb

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "obsservice",
	Short: "An observability service for CockroachDB",
	Long: `The Observability Service ingests monitoring and observability data 
from one or more CockroachDB clusters.`,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		cfg := httpproxy.ReverseHTTPProxyConfig{
			HTTPAddr:      httpAddr,
			TargetURL:     targetURL,
			CACertPath:    caCertPath,
			UICertPath:    uiCertPath,
			UICertKeyPath: uiCertKeyPath,
		}

		// TODO(abarganier): migrate DB migrations over to target storage for aggregated outputs
		//connCfg, err := pgxpool.ParseConfig(sinkPGURL)
		//if err != nil {
		//	return errors.Wrapf(err, "invalid --sink-pgurl (%s)", sinkPGURL)
		//}
		//if connCfg.ConnConfig.Database == "" {
		//	fmt.Printf("No database explicitly provided in --sink-pgurl. Using %q.\n", defaultSinkDBName)
		//	connCfg.ConnConfig.Database = defaultSinkDBName
		//}
		//if err := migrations.RunDBMigrations(ctx, connCfg.ConnConfig); err != nil {
		//	return errors.Wrap(err, "failed to run DB migrations")
		//}

		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, drainSignals...)

		stopper := stop.NewStopper()

		stmtInsightsPipeline, stmtInsightsProcessor, err := makeStatementInsightsPipeline()
		if err != nil {
			return errors.Wrapf(err, "failed to create Statement Insights Pipeline")
		}
		// Run the event ingestion in the background.
		eventRouter := router.NewEventRouter(map[obspb.EventType]obslib.EventConsumer{
			obspb.EventlogEvent:               &obsutil.StdOutConsumer{},
			obspb.StatementInsightsStatsEvent: stmtInsightsPipeline,
		})
		err = stmtInsightsProcessor.Start(ctx, stopper)
		if err != nil {
			return errors.Wrapf(err, "failed to start Statement Insights Processor")
		}
		ingester := ingest.MakeEventIngester(ctx, eventRouter, nil)

		// Instantiate the net listener & gRPC server.
		listener, err := net.Listen("tcp", otlpAddr)
		if err != nil {
			return errors.Wrapf(err, "failed to listen for incoming HTTP connections on address %s", otlpAddr)
		}
		grpcServer := grpc.NewServer()
		logspb.RegisterLogsServiceServer(grpcServer, ingester)
		if err := stopper.RunAsyncTask(ctx, "server-quiesce", func(ctx context.Context) {
			<-stopper.ShouldQuiesce()
			grpcServer.GracefulStop()
		}); err != nil {
			return err
		}
		if err := stopper.RunAsyncTask(ctx, "event-ingester-server", func(ctx context.Context) {
			if err := grpcServer.Serve(listener); err != nil {
				log.Fatalf(ctx, "gRPC server returned an unexpected error: %+v", err)
			}
		}); err != nil {
			return err
		}
		log.Infof(ctx, "Listening for OTLP connections on %s.\n", otlpAddr)

		// Run the reverse HTTP proxy in the background.
		httpproxy.NewReverseHTTPProxy(ctx, cfg).Start(ctx, stopper)

		// Block until the process is signaled to terminate.
		sig := <-signalCh
		log.Infof(ctx, "received signal %s. Shutting down.", sig)
		go func() {
			stopper.Stop(ctx)
		}()

		// Print the shutdown progress every 5 seconds.
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					log.Infof(ctx, "%d running tasks", stopper.NumTasks())
				case <-stopper.IsStopped():
					return
				}
			}
		}()

		// Wait until the shutdown is complete or we receive another signal.
		select {
		case <-stopper.IsStopped():
			log.Infof(ctx, "shutdown complete")
		case sig = <-signalCh:
			switch sig {
			case termSignal:
				log.Infof(ctx, "received SIGTERM while shutting down. Continuing shutdown.")
			default:
				// Crash.
				handleSignalDuringShutdown(sig)
			}
		}
		return nil
	},
}

// Flags.
var (
	otlpAddr                  string
	httpAddr                  string
	targetURL                 string
	caCertPath                string
	uiCertPath, uiCertKeyPath string
	sinkPGURL                 string
)

func main() {

	// Add all the flags registered with the standard "flag" package. Useful for
	// --vmodule, for example.
	RootCmd.PersistentFlags().AddGoFlagSet(flag.CommandLine)

	RootCmd.PersistentFlags().StringVar(
		&otlpAddr,
		"otlp-addr",
		"localhost:4317",
		"The address on which to listen for exported events using OTLP gRPC. If the port is missing, 4317 is used.")
	RootCmd.PersistentFlags().StringVar(
		&httpAddr,
		"http-addr",
		"localhost:8081",
		"The address on which to listen for HTTP requests.")
	RootCmd.PersistentFlags().StringVar(
		&targetURL,
		"crdb-http-url",
		"http://localhost:8080",
		"The base URL to which HTTP requests are proxied.")
	RootCmd.PersistentFlags().StringVar(
		&caCertPath,
		"ca-cert",
		"",
		"Path to the certificate authority certificate file. If specified,"+
			" HTTP requests are only proxied to CRDB nodes that present certificates signed by this CA."+
			" If not specified, the system's CA list is used.")
	RootCmd.PersistentFlags().StringVar(
		&uiCertPath,
		"ui-cert",
		"",
		"Path to the certificate used used by the Observability Service.")
	RootCmd.PersistentFlags().StringVar(
		&uiCertKeyPath,
		"ui-cert-key",
		"",
		"Path to the private key used by the Observability Service. "+
			"This is the key corresponding to the --ui-cert certificate.")

	// Flags about connecting to the sink cluster.
	RootCmd.PersistentFlags().StringVar(
		&sinkPGURL,
		"sink-pgurl",
		"postgresql://root@localhost:26257?sslmode=disable",
		"PGURL for the sink cluster. If the url does not include a database name, "+
			"then \"obsservice\" will be used.")

	if err := RootCmd.Execute(); err != nil {
		exit.WithCode(exit.UnspecifiedError())
	}
}

func handleSignalDuringShutdown(sig os.Signal) {
	// On Unix, a signal that was not handled gracefully by the application
	// should be reraised so it is visible in the exit code.

	// Reset signal to its original disposition.
	signal.Reset(sig)

	// Reraise the signal. os.Signal is always sysutil.Signal.
	if err := unix.Kill(unix.Getpid(), sig.(sysutil.Signal)); err != nil {
		// Sending a valid signal to ourselves should never fail.
		//
		// Unfortunately it appears (#34354) that some users
		// run CockroachDB in containers that only support
		// a subset of all syscalls. If this ever happens, we
		// still need to quit immediately.
		log.Fatalf(context.Background(), "unable to forward signal %v: %v", sig, err)
	}

	// Block while we wait for the signal to be delivered.
	select {}
}

func makeStatementInsightsPipeline() (
	obslib.EventConsumer,
	*process.MemQueueProcessor[*obspb.StatementInsightsStatistics],
	error,
) {
	memQueue := queue.NewMemoryQueue[*obspb.StatementInsightsStatistics](
		maxMemoryBytes, func(statistics *obspb.StatementInsightsStatistics) int {
			return statistics.Size()
		}, "StmtInsightsStatisticsMemQueue")
	memQueueProducer := produce.NewMemQueueProducer[*obspb.StatementInsightsStatistics](memQueue)
	insightsTransformer := &transform.StmtInsightTransformer{}
	insightsValidator := &validate.StmtInsightValidator{}

	producerGroup, err := produce.NewProducerGroup[*obspb.StatementInsightsStatistics](
		"StmtInsightsStatisticsProducerGroup",
		obslib.Observability,
		insightsTransformer,
		[]validate.Validator[*obspb.StatementInsightsStatistics]{insightsValidator},
		[]produce.EventProducer[*obspb.StatementInsightsStatistics]{memQueueProducer})

	if err != nil {
		return nil, nil, err
	}

	// TODO: replace process.InsightsStdoutProcessor for a real Insights processor
	// and delete the file process/stdout.go
	processor, err := process.NewMemQueueProcessor[*obspb.StatementInsightsStatistics](memQueue, &process.InsightsStdoutProcessor{})
	if err != nil {
		return nil, nil, err
	}

	return producerGroup, processor, nil
}
