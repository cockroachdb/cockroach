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
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// DeprecatedDrainParameter the special value that must be
	// passed in DrainRequest.DeprecatedProbeIndicator to signal the
	// drain request is not a probe.
	// This variable is also used in the v20.1 "quit" client
	// to provide a valid input to the request sent to
	// v19.1 nodes.
	//
	// TODO(knz): Remove this in v20.2 and whenever the "quit" command
	// is not meant to work with 19.x servers any more, whichever comes
	// later.
	DeprecatedDrainParameter = []int32{0, 1}

	queryWait = settings.RegisterPublicDurationSetting(
		"server.shutdown.query_wait",
		"the server will wait for at least this amount of time for active queries to finish",
		10*time.Second,
	)

	drainWait = settings.RegisterPublicDurationSetting(
		"server.shutdown.drain_wait",
		"the amount of time a server waits in an unready state before proceeding with the rest "+
			"of the shutdown process",
		0*time.Second,
	)
)

// Drain puts the node into the specified drain mode(s) and optionally
// instructs the process to terminate.
// This method is part of the serverpb.AdminClient interface.
func (s *adminServer) Drain(req *serverpb.DrainRequest, stream serverpb.Admin_DrainServer) error {
	ctx := stream.Context()
	ctx = s.server.AnnotateCtx(ctx)

	doDrain := req.DoDrain
	if len(req.DeprecatedProbeIndicator) > 0 {
		// Pre-20.1 behavior.
		// TODO(knz): Remove this condition in 20.2.
		doDrain = true
		if !reflect.DeepEqual(req.DeprecatedProbeIndicator, DeprecatedDrainParameter) {
			return status.Errorf(codes.InvalidArgument, "Invalid drain request parameter.")
		}
	}

	log.Infof(ctx, "drain request received with doDrain = %v, shutdown = %v", doDrain, req.Shutdown)

	res := serverpb.DrainResponse{}
	if doDrain {
		remaining, info, err := s.server.Drain(ctx)
		if err != nil {
			log.Errorf(ctx, "drain failed: %v", err)
			return err
		}
		res.DrainRemainingIndicator = remaining
		res.DrainRemainingDescription = info
	}
	if s.server.isDraining() {
		res.DeprecatedDrainStatus = DeprecatedDrainParameter
		res.IsDraining = true
	}

	if err := stream.Send(&res); err != nil {
		return err
	}

	if !req.Shutdown {
		if doDrain {
			// The condition "if doDrain" is because we don't need an info
			// message for just a probe.
			log.Infof(ctx, "drain request completed without server shutdown")
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
		os.Exit(1)
		return errors.New("unreachable")
	}
}

// Drain idempotently activates the draining mode.
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
func (s *Server) Drain(ctx context.Context) (remaining uint64, info string, err error) {
	reports := make(map[string]int)
	var mu syncutil.Mutex
	reporter := func(howMany int, what string) {
		if howMany > 0 {
			mu.Lock()
			reports[what] += howMany
			mu.Unlock()
		}
	}
	defer func() {
		// Detail the counts based on the collected reports.
		var descBuf strings.Builder
		comma := ""
		for what, howMany := range reports {
			remaining += uint64(howMany)
			fmt.Fprintf(&descBuf, "%s%s: %d", comma, what, howMany)
			comma = ", "
		}
		info = descBuf.String()
		log.Infof(ctx, "drain remaining: %d", remaining)
		if info != "" {
			log.Infof(ctx, "drain details: %s", info)
		}
	}()

	if err := s.doDrain(ctx, reporter); err != nil {
		return 0, "", err
	}

	return
}

func (s *Server) doDrain(ctx context.Context, reporter func(int, string)) error {
	// First drain all clients and SQL leases.
	if err := s.drainClients(ctx, reporter); err != nil {
		return err
	}
	// Finally, mark the node as draining in liveness and drain the
	// range leases.
	return s.drainNode(ctx, reporter)
}

// isDraining returns true if either clients are being drained
// or one of the stores on the node is not accepting replicas.
func (s *Server) isDraining() bool {
	return s.sqlServer.pgServer.IsDraining() || s.node.IsDraining()
}

// drainClients starts draining the SQL layer.
func (s *Server) drainClients(ctx context.Context, reporter func(int, string)) error {
	// Mark the server as draining in a way that probes to
	// /health?ready=1 will notice.
	s.grpc.setMode(modeDraining)
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

	// Done. This executes the defers set above to drain SQL leases.
	return nil
}

// drainNode initiates the draining mode for the node, which
// starts draining range leases.
func (s *Server) drainNode(ctx context.Context, reporter func(int, string)) error {
	s.nodeLiveness.SetDraining(ctx, true /* drain */, reporter)
	return s.node.SetDraining(true /* drain */, reporter)
}
