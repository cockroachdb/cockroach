// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"io"
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverctl"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// QueryShutdownTimeout is the max amount of time waiting for
	// queries to stop execution.
	QueryShutdownTimeout = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"server.shutdown.query_wait",
		"the timeout for waiting for active transactions to finish during a drain "+
			"(note that the --drain-wait parameter for cockroach node drain may need adjustment "+
			"after changing this setting)",
		10*time.Second,
		settings.NonNegativeDurationWithMaximum(10*time.Hour),
		settings.WithName("server.shutdown.transactions.timeout"),
		settings.WithPublic)

	// DrainWait is the initial wait time before a drain effectively starts.
	DrainWait = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"server.shutdown.drain_wait",
		"the amount of time a server waits in an unready state before proceeding with a drain "+
			"(note that the --drain-wait parameter for cockroach node drain may need adjustment "+
			"after changing this setting. --drain-wait is to specify the duration of the "+
			"whole draining process, while server.shutdown.initial_wait is to set the "+
			"wait time for health probes to notice that the node is not ready.)",
		0*time.Second,
		settings.NonNegativeDurationWithMaximum(10*time.Hour),
		settings.WithName("server.shutdown.initial_wait"),
		settings.WithPublic)

	// ConnectionShutdownTimeout is the max amount of time waiting for
	// clients to disconnect.
	ConnectionShutdownTimeout = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"server.shutdown.connection_wait",
		"the maximum amount of time a server waits for all SQL connections to "+
			"be closed before proceeding with a drain. "+
			"(note that the --drain-wait parameter for cockroach node drain may need adjustment "+
			"after changing this setting)",
		0*time.Second,
		settings.NonNegativeDurationWithMaximum(10*time.Hour),
		settings.WithName("server.shutdown.connections.timeout"),
		settings.WithPublic)

	// JobShutdownTimeout is the max amount of time waiting for jobs to
	// stop executing.
	JobShutdownTimeout = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"server.shutdown.jobs_wait",
		"the maximum amount of time a server waits for all currently executing jobs "+
			"to notice drain request and to perform orderly shutdown",
		10*time.Second,
		settings.NonNegativeDurationWithMaximum(10*time.Minute),
		settings.WithName("server.shutdown.jobs.timeout"))
)

// Drain puts the node into the specified drain mode(s) and optionally
// instructs the process to terminate.
// This method is part of the serverpb.AdminClient interface.
func (s *adminServer) Drain(req *serverpb.DrainRequest, stream serverpb.Admin_DrainServer) error {
	ctx := stream.Context()
	ctx = s.AnnotateCtx(ctx)

	// Which node is this request for?
	nodeID, local, err := s.serverIterator.parseServerID(req.NodeId)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if !local {
		// This request is for another node. Forward it.
		// In contrast to many RPC calls we implement around
		// the server package, the Drain RPC is a *streaming*
		// RPC. This means that it may have more than one
		// response. We must forward all of them.

		// Connect to the target node.
		client, err := s.dialNode(ctx, roachpb.NodeID(nodeID))
		if err != nil {
			return srverrors.ServerError(ctx, err)
		}
		return delegateDrain(ctx, req, client, stream)
	}

	return s.drainServer.handleDrain(ctx, req, stream)
}

type drainServer struct {
	stopper *stop.Stopper
	// stopTrigger is used to request that the server is shut down.
	stopTrigger  *stopTrigger
	grpc         *grpcServer
	sqlServer    *SQLServer
	drainSleepFn func(time.Duration)
	serverCtl    *serverController

	kvServer struct {
		nodeLiveness *liveness.NodeLiveness
		node         *Node
	}
}

// newDrainServer constructs a drainServer suitable for any kind of server.
func newDrainServer(
	cfg BaseConfig,
	stopper *stop.Stopper,
	stopTrigger *stopTrigger,
	grpc *grpcServer,
	sqlServer *SQLServer,
) *drainServer {
	var drainSleepFn = time.Sleep
	if cfg.TestingKnobs.Server != nil {
		if cfg.TestingKnobs.Server.(*TestingKnobs).DrainSleepFn != nil {
			drainSleepFn = cfg.TestingKnobs.Server.(*TestingKnobs).DrainSleepFn
		}
	}
	return &drainServer{
		stopper:      stopper,
		stopTrigger:  stopTrigger,
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
		s.stopTrigger.signalStop(ctx, serverctl.MakeShutdownRequest(serverctl.ShutdownReasonDrainRPC, nil /* err */))
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
			if req.Shutdown && grpcutil.IsClosedConnection(err) {
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
// Note: this represents ONE round of draining. This code is iterated on
// indefinitely until all range leases have been drained.
// This iteration can be found here: pkg/cli/start.go, pkg/cli/quit.go.
//
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
	if s.sqlServer.sqlLivenessSessionID != "" {
		// Set draining only if SQL instance was initialized
		if err := s.sqlServer.sqlInstanceStorage.SetInstanceDraining(
			ctx, s.sqlServer.sqlLivenessSessionID, s.sqlServer.SQLInstanceID()); err != nil {
			return err
		}
	}

	if s.serverCtl != nil {
		// We are on a KV node, with a server controller.
		//
		// First tell the controller to stop starting new servers.
		s.serverCtl.SetDraining()

		// Then shut down tenant servers orchestrated from
		// this node.
		stillRunning := s.serverCtl.drain(ctx)
		reporter(stillRunning, "tenant servers")
		// If we still have tenant servers, we can't make progress on
		// draining SQL clients (on the system tenant) and the KV node,
		// because that would block the graceful drain of the tenant
		// server(s).
		if stillRunning > 0 {
			return nil
		}
		log.Infof(ctx, "all tenant servers stopped")
	}

	// Drain the SQL layer.
	// Drains all SQL connections, distributed SQL execution flows, and SQL table leases.
	if err = s.drainClients(ctx, reporter); err != nil {
		return err
	}
	log.Infof(ctx, "done draining clients")

	// Mark the node as draining in liveness and drain all range leases.
	return s.drainNode(ctx, reporter, verbose)
}

// isDraining returns true if either SQL client connections are being drained
// or if one of the stores on the node is not accepting replicas.
func (s *drainServer) isDraining() bool {
	return s.sqlServer.pgServer.IsDraining() || (s.kvServer.node != nil && s.kvServer.node.IsDraining())
}

// drainClients starts draining the SQL layer.
func (s *drainServer) drainClients(
	ctx context.Context, reporter func(int, redact.SafeString),
) error {
	// Setup a cancelable context so that the logOpenConns goroutine exits when
	// this function returns.
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	shouldDelayDraining := !s.isDraining()

	// Set the gRPC mode of the node to "draining" and mark the node as "not ready".
	// Probes to /health?ready=1 will now notice the change in the node's readiness.
	s.grpc.setMode(modeDraining)
	s.sqlServer.isReady.Store(false)

	// Log the number of connections periodically.
	if err := s.logOpenConns(ctx); err != nil {
		log.Ops.Warningf(ctx, "error showing alive SQL connections: %v", err)
	}

	// Wait the duration of DrainWait.
	// This will fail load balancer checks and delay draining so that client
	// traffic can move off this node.
	// Note delay only happens on first call to drain.
	if shouldDelayDraining {
		log.Ops.Info(ctx, "waiting for health probes to notice that the node "+
			"is not ready for new sql connections")
		s.drainSleepFn(DrainWait.Get(&s.sqlServer.execCfg.Settings.SV))
	}

	// Wait for users to close the existing SQL connections.
	// During this phase, the server is rejecting new SQL connections.
	// The server exits this phase either once all SQL connections are closed,
	// or the connectionMaxWait timeout elapses, whichever happens earlier.
	if err := s.sqlServer.pgServer.WaitForSQLConnsToClose(ctx, ConnectionShutdownTimeout.Get(&s.sqlServer.execCfg.Settings.SV), s.stopper); err != nil {
		return err
	}

	// Inform the job system that the node is draining.
	//
	// We cannot do this before SQL clients disconnect, because
	// otherwise there is a risk that one of the remaining SQL sessions
	// issues a BACKUP or some other job-based statement before it
	// disconnects, and encounters a job error as a result -- that the
	// registry is now unavailable due to the drain.
	{
		_ = timeutil.RunWithTimeout(ctx, "drain-job-registry",
			JobShutdownTimeout.Get(&s.sqlServer.execCfg.Settings.SV),
			func(ctx context.Context) error {
				s.sqlServer.jobRegistry.DrainRequested(ctx)
				return nil
			})
	}

	// Inform the auto-stats tasks that the node is draining.
	s.sqlServer.statsRefresher.SetDraining()

	// Drain any remaining SQL connections.
	// The QueryShutdownTimeout duration is a timeout for waiting for SQL queries to finish.
	// If the timeout is reached, any remaining connections
	// will be closed.
	queryMaxWait := QueryShutdownTimeout.Get(&s.sqlServer.execCfg.Settings.SV)
	if err := s.sqlServer.pgServer.Drain(ctx, queryMaxWait, reporter, s.stopper); err != nil {
		return err
	}

	// Drain all distributed SQL execution flows.
	// The QueryShutdownTimeout duration is used to wait on currently running flows to finish.
	s.sqlServer.distSQLServer.Drain(ctx, queryMaxWait, reporter)

	// Flush in-memory SQL stats into the statement stats system table.
	statsProvider := s.sqlServer.pgServer.SQLServer.GetSQLStatsProvider()
	// If the SQL server is disabled there is nothing to drain here.
	if !s.sqlServer.cfg.DisableSQLServer {
		statsProvider.MaybeFlush(ctx, s.stopper)
	}
	statsProvider.Stop(ctx)

	// Inform the async tasks for table stats that the node is draining
	// and wait for task shutdown.
	s.sqlServer.statsRefresher.WaitForAutoStatsShutdown(ctx)

	// Inform the job system that the node is draining and wait for task
	// shutdown.
	s.sqlServer.jobRegistry.WaitForRegistryShutdown(ctx)

	// Drain all SQL table leases. This must be done after the pgServer has
	// given sessions a chance to finish ongoing work and after the background
	// tasks that may issue SQL statements have shut down.
	s.sqlServer.leaseMgr.SetDraining(ctx, true /* drain */, reporter)

	session, err := s.sqlServer.sqlLivenessProvider.Release(ctx)
	if err != nil {
		return err
	}
	// If we started a sql session on this node.
	if session != "" {
		instanceID := s.sqlServer.sqlIDContainer.SQLInstanceID()
		err = s.sqlServer.sqlInstanceStorage.ReleaseInstance(ctx, session, instanceID)
		if err != nil {
			return err
		}
	}

	// Mark the node as fully drained.
	s.sqlServer.gracefulDrainComplete.Store(true)
	// Mark this phase in the logs to clarify the context of any subsequent
	// errors/warnings, if any.
	log.Infof(ctx, "SQL server drained successfully; SQL queries cannot execute any more")
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

	// Set the node's liveness status to "draining".
	if err = s.kvServer.nodeLiveness.SetDraining(ctx, true /* drain */, reporter); err != nil {
		return err
	}
	// Mark the stores of the node as "draining" and drain all range leases.
	return s.kvServer.node.SetDraining(true /* drain */, reporter, verbose)
}

// logOpenConns logs the number of open SQL connections every 3 seconds.
func (s *drainServer) logOpenConns(ctx context.Context) error {
	return s.stopper.RunAsyncTask(ctx, "log-open-conns", func(ctx context.Context) {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				openConns := s.sqlServer.pgServer.GetConnCancelMapLen()
				log.Ops.Infof(ctx, "number of open connections: %d\n", openConns)
				if openConns == 0 {
					return
				}
			case <-s.stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			}
		}
	})
}

// CallDrainServerSide is a reference implementation for a server-side
// function that wishes to shut down a server gracefully via the Drain
// interface. The Drain interface is responsible for notifying clients
// and shutting down systems in a particular order that prevents
// client app disruptions. We generally prefer graceful drains to the
// disorderly shutdown caused by either a process crash or a direct
// call to the stopper's Stop() method.
//
// By default, this code will wait forever for a graceful drain to
// complete. The caller can override this behavior by passing a context
// with a deadline.
//
// For an example client-side implementation (drain client over RPC),
// see the code in pkg/cli/node.go, doDrain().
func CallDrainServerSide(ctx context.Context, drainFn ServerSideDrainFn) {
	var (
		prevRemaining = uint64(math.MaxUint64)
		verbose       = false
	)

	ctx = logtags.AddTag(ctx, "call-graceful-drain", nil)
	for {
		// Let the caller interrupt the process via context cancellation
		// if so desired.
		select {
		case <-ctx.Done():
			log.Ops.Errorf(ctx, "drain interrupted by caller: %v", ctx.Err())
			return
		default:
		}

		remaining, _, err := drainFn(ctx, verbose)
		if err != nil {
			log.Ops.Errorf(ctx, "graceful drain failed: %v", err)
			return
		}
		if remaining == 0 {
			// No more work to do.
			log.Ops.Infof(ctx, "graceful drain complete")
			return
		}

		// If range lease transfer stalls or the number of
		// remaining leases somehow increases, verbosity is set
		// to help with troubleshooting.
		if remaining >= prevRemaining {
			verbose = true
		}

		// Avoid a busy wait with high CPU usage if the server replies
		// with an incomplete drain too quickly.
		time.Sleep(200 * time.Millisecond)

		// Remember the remaining work to set the verbose flag in the next
		// iteration.
		prevRemaining = remaining
	}
}

// ServerSideDrainFn is the interface of the server-side handler for the Drain logic.
type ServerSideDrainFn func(ctx context.Context, verbose bool) (uint64, redact.RedactableString, error)
