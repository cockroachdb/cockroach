// Copyright 2021 The Cockroach Authors.
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
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// StartTenant starts a stand-alone SQL server against a KV backend.
func StartTenant(
	ctx context.Context,
	stopper *stop.Stopper,
	kvClusterName string, // NB: gone after https://github.com/cockroachdb/cockroach/issues/42519
	baseCfg BaseConfig,
	sqlCfg SQLConfig,
) (sqlServer *SQLServer, pgAddr string, httpAddr string, _ error) {
	args, err := makeSQLServerArgs(stopper, kvClusterName, baseCfg, sqlCfg)
	if err != nil {
		return nil, "", "", err
	}
	s, err := newSQLServer(ctx, args)
	if err != nil {
		return nil, "", "", err
	}

	s.execCfg.SQLStatusServer = newTenantStatusServer(
		baseCfg.AmbientCtx, &adminPrivilegeChecker{ie: args.circularInternalExecutor},
		args.sessionRegistry, args.contentionRegistry, args.flowScheduler, baseCfg.Settings, s,
	)

	// TODO(asubiotto): remove this. Right now it is needed to initialize the
	// SpanResolver.
	s.execCfg.DistSQLPlanner.SetNodeInfo(roachpb.NodeDescriptor{NodeID: 0})

	connManager := netutil.MakeServer(
		args.stopper,
		// The SQL server only uses connManager.ServeWith. The both below
		// are unused.
		nil, // tlsConfig
		nil, // handler
	)
	knobs := baseCfg.TestingKnobs.TenantTestingKnobs
	if tenantKnobs, ok := knobs.(*sql.TenantTestingKnobs); ok && tenantKnobs.IdleExitCountdownDuration != 0 {
		SetupIdleMonitor(ctx, args.stopper, baseCfg.IdleExitAfter, connManager, tenantKnobs.IdleExitCountdownDuration)
	} else {
		SetupIdleMonitor(ctx, args.stopper, baseCfg.IdleExitAfter, connManager)
	}

	pgL, err := ListenAndUpdateAddrs(ctx, &args.Config.SQLAddr, &args.Config.SQLAdvertiseAddr, "sql")
	if err != nil {
		return nil, "", "", err
	}

	{
		waitQuiesce := func(ctx context.Context) {
			<-args.stopper.ShouldQuiesce()
			// NB: we can't do this as a Closer because (*Server).ServeWith is
			// running in a worker and usually sits on accept(pgL) which unblocks
			// only when pgL closes. In other words, pgL needs to close when
			// quiescing starts to allow that worker to shut down.
			_ = pgL.Close()
		}
		if err := args.stopper.RunAsyncTask(ctx, "wait-quiesce-pgl", waitQuiesce); err != nil {
			waitQuiesce(ctx)
			return nil, "", "", err
		}
	}

	httpL, err := ListenAndUpdateAddrs(ctx, &args.Config.HTTPAddr, &args.Config.HTTPAdvertiseAddr, "http")
	if err != nil {
		return nil, "", "", err
	}

	{
		waitQuiesce := func(ctx context.Context) {
			<-args.stopper.ShouldQuiesce()
			_ = httpL.Close()
		}
		if err := args.stopper.RunAsyncTask(ctx, "wait-quiesce-http", waitQuiesce); err != nil {
			waitQuiesce(ctx)
			return nil, "", "", err
		}
	}

	pgLAddr := pgL.Addr().String()
	httpLAddr := httpL.Addr().String()
	args.recorder.AddNode(
		args.registry,
		roachpb.NodeDescriptor{},
		timeutil.Now().UnixNano(),
		pgLAddr,   // advertised addr
		httpLAddr, // http addr
		pgLAddr,   // sql addr
	)

	if err := args.stopper.RunAsyncTask(ctx, "serve-http", func(ctx context.Context) {
		mux := http.NewServeMux()
		debugServer := debug.NewServer(args.Settings, s.pgServer.HBADebugFn())
		mux.Handle("/", debugServer)
		mux.HandleFunc("/health", func(w http.ResponseWriter, req *http.Request) {
			// Return Bad Request if called with arguments.
			if err := req.ParseForm(); err != nil || len(req.Form) != 0 {
				http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
				return
			}
		})
		f := varsHandler{metricSource: args.recorder, st: args.Settings}.handleVars
		mux.Handle(statusVars, http.HandlerFunc(f))
		_ = http.Serve(httpL, mux)
	}); err != nil {
		return nil, "", "", err
	}

	const (
		socketFile = "" // no unix socket
	)
	orphanedLeasesTimeThresholdNanos := args.clock.Now().WallTime

	// TODO(tbg): the log dir is not configurable at this point
	// since it is integrated too tightly with the `./cockroach start` command.
	if err := startSampleEnvironment(ctx, sampleEnvironmentCfg{
		st:                   args.Settings,
		stopper:              args.stopper,
		minSampleInterval:    base.DefaultMetricsSampleInterval,
		goroutineDumpDirName: args.GoroutineDumpDirName,
		heapProfileDirName:   args.HeapProfileDirName,
		runtime:              args.runtime,
	}); err != nil {
		return nil, "", "", err
	}

	s.execCfg.DistSQLPlanner.SetNodeInfo(roachpb.NodeDescriptor{NodeID: roachpb.NodeID(args.nodeIDContainer.SQLInstanceID())})

	if err := s.preStart(ctx,
		args.stopper,
		args.TestingKnobs,
		connManager,
		pgL,
		socketFile,
		orphanedLeasesTimeThresholdNanos,
	); err != nil {
		return nil, "", "", err
	}

	// Register the server's identifiers so that log events are
	// decorated with the server's identity. This helps when gathering
	// log events from multiple servers into the same log collector.
	//
	// We do this only here, as the identifiers may not be known before this point.
	clusterID := args.rpcContext.ClusterID.Get().String()
	log.SetNodeIDs(clusterID, 0 /* nodeID is not known for a SQL-only server. */)
	log.SetTenantIDs(args.TenantID.String(), int32(s.SQLInstanceID()))

	if err := s.startServeSQL(ctx,
		args.stopper,
		s.connManager,
		s.pgL,
		socketFile); err != nil {
		return nil, "", "", err
	}

	return s, pgLAddr, httpLAddr, nil
}
