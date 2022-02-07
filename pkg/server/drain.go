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
	"io"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
		"the timeout for waiting for active queries to finish during a drain "+
			"(note that the --drain-wait parameter for cockroach node drain may need adjustment "+
			"after changing this setting)",
		10*time.Second,
	).WithPublic()

	drainWait = settings.RegisterDurationSetting(
		settings.TenantWritable,
		"server.shutdown.drain_wait",
		"the minimum amount of time a server waits in an unready state before proceeding with a drain"+
			"(note that the --drain-wait parameter for cockroach node drain may need adjustment "+
			"after changing this setting)",
		0*time.Second,
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
			return err
		}
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

	doDrain := req.DoDrain

	log.Ops.Infof(ctx, "drain request received with doDrain = %v, shutdown = %v", doDrain, req.Shutdown)

	res := serverpb.DrainResponse{}
	if doDrain {
		remaining, info, err := s.server.Drain(ctx, req.Verbose)
		if err != nil {
			log.Ops.Errorf(ctx, "drain failed: %v", err)
			return err
		}
		res.DrainRemainingIndicator = remaining
		res.DrainRemainingDescription = info.StripMarkers()
	}
	if s.server.isDraining() {
		res.IsDraining = true
	}

	if err := stream.Send(&res); err != nil {
		return err
	}

	if !req.Shutdown {
		if doDrain {
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
		s.server.grpc.Stop()
		s.server.stopper.Stop(ctx)
	}()

	select {
	case <-s.server.stopper.IsStopped():
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

// Drain idempotently activates the draining mode.
// Note: this represents ONE round of draining. This code is iterated on
// indefinitely until all range leases have been drained.
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
//
// TODO(knz): This method is currently exported for use by the
// shutdown code in cli/start.go; however, this is a mis-design. The
// start code should use the Drain() RPC like quit does.
func (s *Server) Drain(
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

	if err = s.doDrain(ctx, reporter, verbose); err != nil {
		return 0, "", err
	}

	return
}

func (s *Server) doDrain(
	ctx context.Context, reporter func(int, redact.SafeString), verbose bool,
) (err error) {
	// Drain the SQL layer.
	// Drains all SQL connections, distributed SQL execution flows, and SQL table leases.
	if err = s.drainClients(ctx, reporter); err != nil {
		return err
	}
	// Drain all range leases.
	return s.drainNode(ctx, reporter, verbose)
}

// isDraining returns true if either SQL client connections are being drained
// or if one of the stores on the node is not accepting replicas.
func (s *Server) isDraining() bool {
	return s.sqlServer.pgServer.IsDraining() || s.node.IsDraining()
}

// drainClients starts draining the SQL layer.
func (s *Server) drainClients(ctx context.Context, reporter func(int, redact.SafeString)) error {
	shouldDelayDraining := !s.isDraining()

	// Set the gRPC mode of the node to "draining" and mark the node as "not ready".
	// Probes to /health?ready=1 will now notice the change in the node's readiness.
	s.grpc.setMode(modeDraining)
	s.sqlServer.isReady.Set(false)

	// Wait the duration of drainWait.
	// This will fail load balancer checks and delay draining so that client
	// traffic can move off this node.
	// Note delay only happens on first call to drain.
	if shouldDelayDraining {
		s.drainSleepFn(drainWait.Get(&s.st.SV))
	}

	// Drain all SQL connections.
	// The queryWait duration is used to wait on clients to self-disconnect.
	// After the duration, the connections are closed.
	queryMaxWait := queryWait.Get(&s.st.SV)
	if err := s.sqlServer.pgServer.Drain(ctx, queryMaxWait, reporter); err != nil {
		return err
	}

	// Drain all distributed SQL execution flows.
	// The queryWait duration is used to wait on currently running flows to finish.
	s.sqlServer.distSQLServer.Drain(ctx, queryMaxWait, reporter)

	// Drain all SQL table leases. This must be done after the pgServer has
	// given sessions a chance to finish ongoing work.
	s.sqlServer.leaseMgr.SetDraining(ctx, true /* drain */, reporter)

	// Done. This executes the defers set above to drain SQL leases.
	return nil
}

// drainNode initiates the draining mode for the node, which
// starts draining range leases.
func (s *Server) drainNode(
	ctx context.Context, reporter func(int, redact.SafeString), verbose bool,
) (err error) {
	// Set the node's liveness status to "draining".
	if err = s.nodeLiveness.SetDraining(ctx, true /* drain */, reporter); err != nil {
		return err
	}
	// Mark the stores of the node as "draining" and drain all range leases.
	return s.node.SetDraining(true /* drain */, reporter, verbose)
}
