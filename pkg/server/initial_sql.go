// Copyright 2020 The Cockroach Authors.
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
	"fmt"
	"net"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/ingest"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/migrations"
	logspb "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/collector/logs/v1"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4/pgxpool"
	"google.golang.org/grpc"
)

// RunInitialSQL concerns itself with running "initial SQL" code when
// a cluster is started for the first time.
//
// The "startSingleNode" argument is true for `start-single-node`,
// and `cockroach demo` with 2 nodes or fewer.
// If adminUser is non-empty, an admin user with that name is
// created upon initialization. Its password is then also returned.
func (s *Server) RunInitialSQL(
	ctx context.Context, startSingleNode bool, adminUser, adminPassword string,
) error {
	if s.cfg.ObsServiceAddr == base.ObsServiceEmbedFlagValue {
		if err := s.startEmbeddedObsService(ctx); err != nil {
			return err
		}
	}

	newCluster := s.InitialStart() && s.NodeID() == kvstorage.FirstNodeID
	if !newCluster {
		// The initial SQL code only runs the first time the cluster is initialized.
		return nil
	}

	if startSingleNode {
		// For start-single-node, set the default replication factor to
		// 1 so as to avoid warning messages and unnecessary rebalance
		// churn.
		if err := s.disableReplication(ctx); err != nil {
			log.Ops.Errorf(ctx, "could not disable replication: %v", err)
			return err
		}
		log.Ops.Infof(ctx, "Replication was disabled for this cluster.\n"+
			"When/if adding nodes in the future, update zone configurations to increase the replication factor.")
	}

	if adminUser != "" && !s.Insecure() {
		if err := s.createAdminUser(ctx, adminUser, adminPassword); err != nil {
			return err
		}
	}

	return nil
}

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

// RunInitialSQL implements cli.serverStartupInterface.
func (s *SQLServerWrapper) RunInitialSQL(context.Context, bool, string, string) error {
	return nil
}

// createAdminUser creates an admin user with the given name.
func (s *Server) createAdminUser(ctx context.Context, adminUser, adminPassword string) error {
	ie := s.sqlServer.internalExecutor
	_, err := ie.Exec(
		ctx, "admin-user", nil,
		fmt.Sprintf("CREATE USER %s WITH PASSWORD $1", adminUser),
		adminPassword,
	)
	if err != nil {
		return err
	}
	// TODO(knz): Demote the admin user to an operator privilege with fewer options.
	_, err = ie.Exec(ctx, "admin-user", nil, fmt.Sprintf("GRANT admin TO %s", tree.Name(adminUser)))
	return err
}

// disableReplication changes the replication factor on
// all defined zones to become 1. This is used by start-single-node
// and demo to define single-node clusters, so as to avoid
// churn in the log files.
//
// The change is effected using the internal SQL interface of the
// given server object.
func (s *Server) disableReplication(ctx context.Context) (retErr error) {
	ie := s.sqlServer.internalExecutor

	it, err := ie.QueryIterator(ctx, "get-zones", nil,
		"SELECT target FROM crdb_internal.zones")
	if err != nil {
		return err
	}
	// We have to make sure to close the iterator since we might return
	// from the for loop early (before Next() returns false).
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		zone := string(*it.Cur()[0].(*tree.DString))
		if _, err := ie.Exec(ctx, "set-zone", nil,
			fmt.Sprintf("ALTER %s CONFIGURE ZONE USING num_replicas = 1", zone)); err != nil {
			return err
		}
	}
	return err
}
