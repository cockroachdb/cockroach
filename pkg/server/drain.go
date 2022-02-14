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
	"io"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	queryWait = settings.RegisterDurationSetting(
		settings.TenantWritable,
		"server.shutdown.query_wait",
		"the server will wait for at least this amount of time for active queries to finish "+
			"(note that the --drain-wait parameter for cockroach node drain may need adjustment "+
			"after changing this setting)",
		10*time.Second,
		settings.NonNegativeDurationWithMaximum(10*time.Hour),
	).WithPublic()

	drainWait = settings.RegisterDurationSetting(
		settings.TenantWritable,
		"server.shutdown.drain_wait",
		"the amount of time a server waits in an unready state before proceeding with the rest "+
			"of the shutdown process "+
			"(note that the --drain-wait parameter for cockroach node drain may need adjustment "+
			"after changing this setting)",
		0*time.Second,
		settings.NonNegativeDurationWithMaximum(10*time.Hour),
	).WithPublic()

	connectionWait = settings.RegisterDurationSetting(
		settings.TenantReadOnly,
		"server.shutdown.connection_wait",
		"the maximum amount of time a server waits all SQL connections to be "+
			"closed before proceeding with the rest of the shutdown process. "+
			"When all SQL connections are closed before times out, the server early "+
			"exits this phase and proceed to the rest of the shutdown process. "+
			"It must be set with a non-negative value. The maximum value allowed is "+
			"10 hours. "+
			"(note that the --drain-wait parameter for cockroach node drain may need "+
			"adjustment after changing this setting)",
		0*time.Second,
		settings.NonNegativeDurationWithMaximum(10*time.Hour),
	).WithPublic()
)

// Drain puts the node into the specified drain mode(s) and optionally
// instructs the process to terminate.
// This method is part of the serverpb.AdminClient interface.
func (s *adminServer) Drain(req *serverpb.DrainRequest, stream serverpb.Admin_DrainServer) error {
	ctx := stream.Context()
	ctx = s.server.AnnotateCtx(ctx)

	// Which node is this request for?
	nodeID, local, err := s.server.status.parseNodeID(req.NodeId)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, err.Error())
	}
	if !local {
		// This request is for another node. Forward it.
		// In contrast to many RPC calls we implement around
		// the server package, the Drain RPC is a *streaming*
		// RPC. This means that it may have more than one
		// response. We must forward all of them.

		// Connect to the target node.
		client, err := s.dialNode(ctx, nodeID)
		if err != nil {
			return serverError(ctx, err)
		}
		return delegateDrain(ctx, req, client, stream)
	}

	return s.server.drain.handleDrain(ctx, req, stream)
}

type drainServer struct {
	stopper      *stop.Stopper
	grpc         *grpcServer
	sqlServer    *SQLServer
	drainSleepFn func(time.Duration)

	kvServer struct {
		nodeLiveness *liveness.NodeLiveness
		node         *Node
	}
}

// newDrainServer constructs a drainServer suitable for any kind of server.
func newDrainServer(
	cfg BaseConfig, stopper *stop.Stopper, grpc *grpcServer, sqlServer *SQLServer,
) *drainServer {
	var drainSleepFn = time.Sleep
	if cfg.TestingKnobs.Server != nil {
		if cfg.TestingKnobs.Server.(*TestingKnobs).DrainSleepFn != nil {
			drainSleepFn = cfg.TestingKnobs.Server.(*TestingKnobs).DrainSleepFn
		}
	}
	return &drainServer{
		stopper:      stopper,
		grpc:         grpc,
		sqlServer:    sqlServer,
		drainSleepFn: drainSleepFn,
	}
}

// setNode configures the drainServer to also support KV node shutdown.
func (s *drainServer) setNode(node *Node, nodeLiveness *liveness.NodeLiveness) {
	s.kvServer.node = node
	s.kvServer.nodeLiveness = nodeLiveness
}

func (s *drainServer) handleDrain(
	ctx context.Context, req *serverpb.DrainRequest, stream serverpb.Admin_DrainServer,
) error {
	log.Ops.Infof(ctx, "drain request received with doDrain = %v, shutdown = %v", req.DoDrain, req.Shutdown)

	res := serverpb.DrainResponse{}
	if req.DoDrain {
		remaining, info, err := s.runDrain(ctx, req.Verbose)
		if err != nil {
			log.Ops.Errorf(ctx, "drain failed: %v", err)
			return err
		}
		res.DrainRemainingIndicator = remaining
		res.DrainRemainingDescription = info.StripMarkers()
	}
	if s.isDraining() {
		res.IsDraining = true
	}

	if err := stream.Send(&res); err != nil {
		return err
	}

	return s.maybeShutdownAfterDrain(ctx, req)
}

func (s *drainServer) maybeShutdownAfterDrain(
	ctx context.Context, req *serverpb.DrainRequest,
) error {
	if !req.Shutdown {
		if req.DoDrain {
			// The condition "if doDrain" is because we don't need an info
			// message for just a probe.
			log.Ops.Infof(ctx, "drain request completed without server shutdown")
		}
		return nil
	}

	go func() {
		// TODO(tbg): why don't we stop the stopper first? Stopping the stopper
		// first seems more reasonable since grpc.Stop closes the listener right
		// away (and who knows whether gRPC-goroutines are tied up in some
		// stopper task somewhere).
		s.grpc.Stop()
		s.stopper.Stop(ctx)
	}()

	select {
	case <-s.stopper.IsStopped():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Second):
		// This is a hack to work around the problem in
		// https://github.com/cockroachdb/cockroach/issues/37425#issuecomment-494336131
		//
		// There appear to be deadlock scenarios in which we don't manage to
		// fully stop the grpc server (which implies closing the listener, i.e.
		// seeming dead to the outside world) or don't manage to shut down the
		// stopper (the evidence in #37425 is inconclusive which one it is).
		//
		// Other problems in this area are known, such as
		// https://github.com/cockroachdb/cockroach/pull/31692
		//
		// The signal-based shutdown path uses a similar time-based escape hatch.
		// Until we spend (potentially lots of time to) understand and fix this
		// issue, this will serve us well.
		log.Fatal(ctx, "timeout after drain")
		return errors.New("unreachable")
	}
}

// delegateDrain forwards a drain request to another node.
// 'client' is where the request should be forwarded to.
// 'stream' is where the request came from, and where the response should go.
func delegateDrain(
	ctx context.Context,
	req *serverpb.DrainRequest,
	client serverpb.AdminClient,
	stream serverpb.Admin_DrainServer,
) error {
	// Retrieve the stream interface to the target node.
	drainClient, err := client.Drain(ctx, req)
	if err != nil {
		return err
	}
	// Forward all the responses from the remote server,
	// to our client.
	for {
		// Receive one response message from the target node.
		resp, err := drainClient.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			if grpcutil.IsClosedConnection(err) {
				// If the drain request contained Shutdown==true, it's
				// possible for the RPC connection to the target node to be
				// shut down before a DrainResponse and EOF is
				// received. This is not truly an error.
				break
			}

			return err
		}
		// Forward the response from the target node to our remote
		// client.
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	return nil
}

// runDrain idempotently activates the draining mode.
// Note: new code should not be taught to use this method
// directly. Use the Drain() RPC instead with a suitably crafted
// DrainRequest.
//
// On failure, the system may be in a partially drained
// state; the client should either continue calling Drain() or shut
// down the server.
//
// The reporter function, if non-nil, is called for each
// packet of load shed away from the server during the drain.
func (s *drainServer) runDrain(
	ctx context.Context, verbose bool,
) (remaining uint64, info redact.RedactableString, err error) {
	reports := make(map[redact.SafeString]int)
	var mu syncutil.Mutex
	reporter := func(howMany int, what redact.SafeString) {
		if howMany > 0 {
			mu.Lock()
			reports[what] += howMany
			mu.Unlock()
		}
	}
	defer func() {
		// Detail the counts based on the collected reports.
		var descBuf strings.Builder
		comma := redact.SafeString("")
		for what, howMany := range reports {
			remaining += uint64(howMany)
			redact.Fprintf(&descBuf, "%s%s: %d", comma, what, howMany)
			comma = ", "
		}
		info = redact.RedactableString(descBuf.String())
		log.Ops.Infof(ctx, "drain remaining: %d", remaining)
		if info != "" {
			log.Ops.Infof(ctx, "drain details: %s", info)
		}
	}()

	if err = s.drainInner(ctx, reporter, verbose); err != nil {
		return 0, "", err
	}

	return
}

func (s *drainServer) drainInner(
	ctx context.Context, reporter func(int, redact.SafeString), verbose bool,
) (err error) {
	// First drain all clients and SQL leases.
	if err = s.drainClients(ctx, reporter); err != nil {
		return err
	}
	// Finally, mark the node as draining in liveness and drain the
	// range leases.
	return s.drainNode(ctx, reporter, verbose)
}

// isDraining returns true if either clients are being drained
// or one of the stores on the node is not accepting replicas.
func (s *drainServer) isDraining() bool {
	return s.sqlServer.pgServer.IsDraining() || (s.kvServer.node != nil && s.kvServer.node.IsDraining())
}

// drainClients starts draining the SQL layer.
func (s *drainServer) drainClients(
	ctx context.Context, reporter func(int, redact.SafeString),
) error {
	shouldDelayDraining := !s.isDraining()
	// Mark the server as draining in a way that probes to
	// /health?ready=1 will notice.
	s.grpc.setMode(modeDraining)
	s.sqlServer.isReady.Set(false)

	// Log the number of connections periodically.
	if err := s.logActiveConns(ctx); err != nil {
		log.Ops.Info(ctx, fmt.Sprintf("error showing alive SQL connections: %v", err))
	}

	// Wait for drainUnreadyWait. This will fail load balancer checks and
	// delay draining so that client traffic can move off this node.
	// Note delay only happens on first call to drain.
	if shouldDelayDraining {
		drainWaitDuration := drainWait.Get(&s.sqlServer.execCfg.Settings.SV)
		drainWaitLogMessage := fmt.Sprintf(
			"node enters the unready status, health checkpoint starts to report "+
				"the shutting-down status. The server will proceed to the next draining "+
				"phase after %s (set by cluster setting \"server.shutdown.drain_wait\")",
			drainWaitDuration,
		)
		log.Ops.Info(ctx, drainWaitLogMessage)
		s.drainSleepFn(drainWaitDuration)
	}

	// Disable incoming SQL clients up to the queryWait timeout.
	queryMaxWait := queryWait.Get(&s.sqlServer.execCfg.Settings.SV)
	connectionMaxWait := connectionWait.Get(&s.sqlServer.execCfg.Settings.SV)

	//if err := s.sqlServer.pgServer.Drain(ctx, queryMaxWait, reporter); err != nil {
	//	return err
	//}
	if err := s.sqlServer.pgServer.WaitSqlDraining(
		ctx,
		connectionMaxWait,
		queryMaxWait,
		reporter,
	); err != nil {
		return errors.Wrapf(err, "error when waiting for pgwire server to complete draining")
	}

	// Stop ongoing SQL execution up to the queryWait timeout.
	s.sqlServer.distSQLServer.Drain(ctx, queryMaxWait, reporter)

	// Drain the SQL leases. This must be done after the pgServer has
	// given sessions a chance to finish ongoing work.
	s.sqlServer.leaseMgr.SetDraining(ctx, true /* drain */, reporter)

	// Done. This executes the defers set above to drain SQL leases.
	return nil
}

// drainNode initiates the draining mode for the node, which
// starts draining range leases.
func (s *drainServer) drainNode(
	ctx context.Context, reporter func(int, redact.SafeString), verbose bool,
) (err error) {
	if s.kvServer.node == nil {
		// No KV subsystem. Nothing to do.
		return nil
	}
	if err = s.kvServer.nodeLiveness.SetDraining(ctx, true /* drain */, reporter); err != nil {
		return err
	}
	return s.kvServer.node.SetDraining(true /* drain */, reporter, verbose)
}

// logActiveConns logs the number of active SQL connections every 3 seconds.
func (s *drainServer) logActiveConns(ctx context.Context) error {
	return s.stopper.RunAsyncTask(ctx, "log-active-conns", func(ctx context.Context) {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.sqlServer.pgServer.LogActiveConns(ctx)
			case <-s.stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			}
		}
	})
}
