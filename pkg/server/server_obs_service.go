// Copyright 2023 The Cockroach Authors.
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
	"net"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/ingest"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/migrations"
	logspb "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/collector/logs/v1"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/jackc/pgx/v4/pgxpool"
	"google.golang.org/grpc"
)

// startEmbeddedObsService creates the schema for the Observability Service (if
// it doesn't exist already), starts the internal RPC service for event
// ingestion and hooks up the event exporter to talk to the local service.
func (s *Server) startEmbeddedObsService(ctx context.Context) error {
	// Create the Obs Service schema.
	loopbackConfig, err := pgxpool.ParseConfig("")
	if err != nil {
		return err
	}
	loopbackConfig.ConnConfig.User = "root"
	loopbackConfig.ConnConfig.Database = "crdb_observability"
	loopbackConfig.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return s.loopbackPgL.Connect(ctx)
	}
	log.Infof(ctx, "running migrations for embedded Observability Service")
	if err := migrations.RunDBMigrations(ctx, loopbackConfig.ConnConfig); err != nil {
		return err
	}

	// Create the internal ingester RPC server.
	embeddedObsSvc, err := ingest.MakeEventIngester(ctx, loopbackConfig)
	if err != nil {
		return err
	}
	// We'll use an RPC server serving on a "loopback" interface implemented with
	// in-memory pipes.
	grpcServer := grpc.NewServer()
	logspb.RegisterLogsServiceServer(grpcServer, &embeddedObsSvc)
	rpcLoopbackL := netutil.NewLoopbackListener(ctx, s.stopper)
	if err := s.stopper.RunAsyncTask(
		ctx, "obssvc-loopback-quiesce", func(ctx context.Context) {
			<-s.stopper.ShouldQuiesce()
			grpcServer.Stop()
			embeddedObsSvc.Close()
		},
	); err != nil {
		embeddedObsSvc.Close()
		return err
	}
	if err := s.stopper.RunAsyncTask(
		ctx, "obssvc-listener", func(ctx context.Context) {
			netutil.FatalIfUnexpected(grpcServer.Serve(rpcLoopbackL))
		}); err != nil {
		return err
	}

	// Now that the ingester is listening for RPCs, we can hook up the exporter to
	// it and start the exporter. Note that in the non-embedded case, Start() has
	// already been called.
	s.eventsExporter.SetDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		conn, err := rpcLoopbackL.Connect(ctx)
		return conn, err
	})
	log.Infof(ctx, "starting event exporting talking to local event ingester")
	if err := s.eventsExporter.Start(ctx, s.stopper); err != nil {
		return err
	}
	return nil
}
