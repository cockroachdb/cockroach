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
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	queryWait = settings.RegisterDurationSetting(
		"server.shutdown.query_wait",
		"the server will wait for at least this amount of time for active queries to finish",
		10*time.Second,
	).WithPublic()

	drainWait = settings.RegisterDurationSetting(
		"server.shutdown.drain_wait",
		"the amount of time a server waits in an unready state before proceeding with the rest "+
			"of the shutdown process",
		0*time.Second,
	).WithPublic()
)

// Drain puts the node into the specified drain mode(s) and optionally
// instructs the process to terminate.
// This method is part of the serverpb.AdminClient interface.
func (s *adminServer) Drain(req *serverpb.DrainRequest, stream serverpb.Admin_DrainServer) error {
	ctx := stream.Context()
	ctx = s.server.AnnotateCtx(ctx)

	if len(req.Pre201Marker) > 0 {
		return status.Errorf(codes.InvalidArgument,
			"Drain request coming from unsupported client version older than 20.1.")
	}

	doDrain := req.DoDrain

	log.Ops.Infof(ctx, "drain request received with doDrain = %v, shutdown = %v", doDrain, req.Shutdown)

	res := serverpb.DrainResponse{}
	if doDrain {
		remaining, info, err := s.server.Drain(ctx)
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
// Note: new code should not be taught to use this method
// directly. Use the Drain() RPC instead with a suitably crafted
// DrainRequest.
//
// On failure, the system may be in a partially drained state; the
// client should either continue calling Drain() until the remaining
// count comes down to zero, or shut down the server.
//
// TODO(knz): This method is currently exported for use by the
// shutdown code in cli/start.go; however, this is a mis-design. The
// start code should use the Drain() RPC like quit does.
func (s *Server) Drain(
	ctx context.Context,
) (remaining uint64, info redact.RedactableString, err error) {
	progress := newDrainProgress()

	// Regardless of the return path, report on the progress in logs before
	// the function exits.
	defer func() {
		remaining, info = progress.getProgress()
		log.Ops.Infof(ctx, "drain progress: %d", remaining)
		if info != "" {
			log.Ops.Infof(ctx, "drain details: %s", info)
		}
	}()

	// First drain all clients and SQL leases.
	log.Infof(ctx, "draining clients")
	if err := s.drainClients(ctx, progress.report); err != nil {
		return remaining, info, err
	}

	// Mark the node liveness record as draining. This starts telling
	// range caches on other nodes that this node is going away.
	log.Infof(ctx, "draining liveness")
	if err := s.nodeLiveness.SetDraining(ctx, true /* drain */, progress.report); err != nil {
		return remaining, info, err
	}

	// GetLiveNodeCount includes nodes which are member of the
	// cluster and for which their liveness
	// record has not expired. This includes the current (draining) node,
	// because marking the liveness record as draining does not prevent
	// it from heartbeating.
	liveNodes := s.nodeLiveness.GetLiveNodeCount()

	if progress.hasProgress() {
		// Wait until some confidence exists that the other nodes have
		// acknowledged the draining state: this waits until the expiry
		// of of the liveness record, which is at least as late as when
		// other nodes are forced to refresh their range leases.
		//
		// The problem that this wait solves is the following. When other
		// nodes already have replicas for the same ranges as this node,
		// these other nodes may attempt to transfer leases to this node
		// for routine rebalancing as long as they see this node as live
		// (via its liveness). This wait ensures they are forced to reload
		// the liveness record and see the node is draining, and stop using
		// it as target for rebalancing.
		//
		// We wait here only the first time Drain() is called, when the
		// liveness record has been toggled from non-draining to
		// draining.
		//
		// The reason why hasProgress() is synonymous with "Drain() is
		// being called for the first time" here is because only during
		// the first iteration is there work performed in drainClients()
		// and nodeLiveness.SetDraining() above. At the second and later
		// iterations, these first two steps do no work.

		toWait, err := s.nodeLiveness.TimeUntilLivenessExpiry(ctx)
		if err != nil {
			return remaining, info, err
		}

		if toWait > 0 {
			log.Infof(ctx, "waiting %s for the liveness record to expire", toWait)
			time.Sleep(toWait)
		} else {
			log.VInfof(ctx, 1, "no liveness record on this node, no expiry to wait on")
		}

		if liveNodes > 1 {
			// If we believe there are other nodes, we also wait 5 seconds
			// past the expiration to give ample time for these nodes to
			// re-load their copy of this node's liveness record, prior to
			// us transferring leases below.
			//
			// This wait is not necessary for correctness; it is merely an
			// optimization: it reduces the probability that another node
			// hasn't seen the expiration yet and tries to transfer a
			// lease back to this draining node during the lease drain
			// below.
			//
			// We also only use the optimization if we have some
			// confidence that there are other ready nodes in the cluster;
			// for a single-node cluster, this wait is clearly a waste of
			// time and would be a source of annoyance to the user.
			const extraLivenessDrainStatePropagationDelay = 2 * time.Second

			log.Infof(ctx, "waiting %s to let draining state propagate throughout cluster", extraLivenessDrainStatePropagationDelay)
			time.Sleep(extraLivenessDrainStatePropagationDelay)
		}
	}

	// Transfer the range leases away.
	// This may take a while; that's OK.
	//
	// Note that we are careful to only start shedding range leases away
	// after we have some confidence that the other nodes have picked up
	// the draining bit (by waiting for the expiration delay,
	// above). This is because otherwise, this node would be a candidate
	// for transferring the leases back immediately, concurrently
	// negating the progress made by this SetDraining() call.
	log.Infof(ctx, "transferring leases")
	if err := s.node.SetDraining(true /* drain */, progress.report); err != nil {
		return remaining, info, err
	}

	if !progress.hasProgress() && liveNodes > 1 {
		s.TestingLeaseTransferDoneAfterDrain.Set(true)
		// If there is no more work to do, the process will then proceed to
		// shut down.
		//
		// Just before doing so however, if we believe there are other
		// nodes, then wait a little bit more.
		//
		// The problem that this second wait helps solve is when other
		// nodes do not have replicas for the ranges whose leases were
		// just transferred away, but may have oudated information about
		// them in their range desc/lease caches. These other nodes may be
		// serving application traffic, and we want to give them a chance
		// to encounter a NotLeaseHolderError and refresh their cache
		// after the last lease has been transferred.
		//
		// Like above, this is an optimization: if this was not
		// occurring, the other nodes would simply time out on a request
		// and start inquiring other replicas to discover the new
		// leaseholder. We also avoid the optimization if the ready node
		// count is just 1, to prevent UX annoyances.
		const extraIdleStateForReceivingStrayMisroutedLeaseholderRequestsDelay = 5 * time.Second
		log.Infof(ctx,
			"waiting %s so that final requests to this node from rest of cluster can be redirected",
			extraIdleStateForReceivingStrayMisroutedLeaseholderRequestsDelay)
		time.Sleep(extraIdleStateForReceivingStrayMisroutedLeaseholderRequestsDelay)
	}

	return remaining, info, nil
}

// isDraining returns true if either clients are being drained
// or one of the stores on the node is not accepting replicas.
func (s *Server) isDraining() bool {
	return s.sqlServer.pgServer.IsDraining() || s.node.IsDraining()
}

// drainClients starts draining the SQL layer.
func (s *Server) drainClients(ctx context.Context, reporter func(int, redact.SafeString)) error {
	// Mark the server as draining in a way that probes to
	// /health?ready=1 will notice.
	s.grpc.setMode(modeDraining)
	s.sqlServer.acceptingClients.Set(false)
	// Wait for drainUnreadyWait. This will fail load balancer checks and
	// delay draining so that client traffic can move off this node.
	time.Sleep(drainWait.Get(&s.st.SV))

	// Disable incoming SQL clients up to the queryWait timeout.
	drainMaxWait := queryWait.Get(&s.st.SV)
	if err := s.sqlServer.pgServer.Drain(drainMaxWait, reporter); err != nil {
		return err
	}
	// Stop ongoing SQL execution up to the queryWait timeout.
	s.sqlServer.distSQLServer.Drain(ctx, drainMaxWait, reporter)

	// Drain the SQL leases. This must be done after the pgServer has
	// given sessions a chance to finish ongoing work.
	s.sqlServer.leaseMgr.SetDraining(true /* drain */, reporter)

	return nil
}

// drainProgress stores the result of calls to the reporter function
// passed to the various draining function.
//
// This is made safe for concurrent use.
type drainProgress struct {
	// We use a mutex to ensure that the reporter function can be called
	// concurrently and safely from multiple goroutines.
	syncutil.Mutex
	reports map[redact.SafeString]int
}

func newDrainProgress() *drainProgress {
	return &drainProgress{
		reports: make(map[redact.SafeString]int),
	}
}

// report some drain work to the drainProgress tracker.
// This is safe for use by multiple goroutines concurrently.
func (p *drainProgress) report(howMany int, what redact.SafeString) {
	if howMany > 0 {
		p.Lock()
		defer p.Unlock()
		p.reports[what] += howMany
	}
}

// hasProgress returns true iff some progress was reported via
// the report() method already.
func (p *drainProgress) hasProgress() bool {
	p.Lock()
	defer p.Unlock()
	return len(p.reports) > 0
}

// getProgress retrieves a description and a count of the work
// performed so far.
// The caller guarantees that no concurrent calls to the report()
// method are occurring.
func (p *drainProgress) getProgress() (remaining uint64, details redact.RedactableString) {
	var descBuf redact.StringBuilder
	comma := redact.SafeString("")
	for what, howMany := range p.reports {
		remaining += uint64(howMany)
		descBuf.Printf("%s%s: %d", comma, what, howMany)
		comma = ", "
	}
	return remaining, descBuf.RedactableString()
}
