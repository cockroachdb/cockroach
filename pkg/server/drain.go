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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var (
	// GracefulDrainModes is the standard succession of drain modes entered
	// for a graceful shutdown.
	GracefulDrainModes = []serverpb.DrainMode{serverpb.DrainMode_CLIENT, serverpb.DrainMode_LEASES}

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
	// `cockroach quit` sends a probe to check that the server is able
	// to process a Drain request, prior to sending an actual drain
	// request.
	isProbe := len(req.On) == 0 && len(req.Off) == 0 && !req.Shutdown

	ctx := stream.Context()

	on := make([]serverpb.DrainMode, len(req.On))
	off := make([]serverpb.DrainMode, len(req.Off))

	if !isProbe {
		var buf strings.Builder
		comma := ""
		for i := range req.On {
			on[i] = serverpb.DrainMode(req.On[i])
			fmt.Fprintf(&buf, "%sshutdown: %s", comma, on[i].String())
			comma = ", "
		}
		for i := range req.Off {
			off[i] = serverpb.DrainMode(req.Off[i])
			fmt.Fprintf(&buf, "%sresume: %s", comma, off[i].String())
			comma = ", "
		}
		// Report the event to the log file for troubleshootability.
		log.Infof(ctx, "drain request received (%s), process shutdown: %v", buf.String(), req.Shutdown)
	} else {
		log.Infof(ctx, "received request for drain status")
	}

	nowOn, err := s.server.Undrain(ctx, off)
	if err != nil {
		return err
	}
	if len(on) > 0 {
		nowOn, err = s.server.Drain(ctx, on)
		if err != nil {
			return err
		}
	}

	// Report the current status to the client.
	res := serverpb.DrainResponse{
		On: make([]int32, len(nowOn)),
	}
	for i := range nowOn {
		res.On[i] = int32(nowOn[i])
	}
	if err := stream.Send(&res); err != nil {
		return err
	}

	if !req.Shutdown {
		if !isProbe {
			// We don't need an info message for just a probe.
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

// Drain idempotently activates the given DrainModes on the Server in the order
// in which they are supplied.
// For example, Drain is typically called with [CLIENT,LEADERSHIP] before
// terminating the process for graceful shutdown.
// On success, returns all active drain modes after carrying out the request.
// On failure, the system may be in a partially drained state and should be
// recovered by calling Undrain() with the same (or a larger) slice of modes.
func (s *Server) Drain(ctx context.Context, on []serverpb.DrainMode) ([]serverpb.DrainMode, error) {
	return s.doDrain(ctx, on, true /* setTo */)
}

// Undrain idempotently deactivates the given DrainModes on the Server in the
// order in which they are supplied.
// On success, returns any remaining active drain modes.
func (s *Server) Undrain(
	ctx context.Context, off []serverpb.DrainMode,
) ([]serverpb.DrainMode, error) {
	return s.doDrain(ctx, off, false /* setTo */)
}

func (s *Server) doDrain(
	ctx context.Context, modes []serverpb.DrainMode, setTo bool,
) ([]serverpb.DrainMode, error) {
	for _, mode := range modes {
		switch mode {
		case serverpb.DrainMode_CLIENT:
			if err := s.drainClients(ctx, setTo); err != nil {
				return nil, err
			}

		case serverpb.DrainMode_LEASES:
			if err := s.drainRangeLeases(ctx, setTo); err != nil {
				return nil, err
			}
		default:
			return nil, errors.Errorf("unknown drain mode: %s", mode)
		}
	}
	var nowOn []serverpb.DrainMode
	if s.pgServer.IsDraining() {
		nowOn = append(nowOn, serverpb.DrainMode_CLIENT)
	}
	if s.node.IsDraining() {
		nowOn = append(nowOn, serverpb.DrainMode_LEASES)
	}
	return nowOn, nil
}

// drainClients starts draining the SQL layer if setTo is true, and
// stops draining SQL if setTo is false.
func (s *Server) drainClients(ctx context.Context, setTo bool) error {
	if setTo {
		s.grpc.setMode(modeDraining)
		// Wait for drainUnreadyWait. This will fail load balancer checks and
		// delay draining so that client traffic can move off this node.
		time.Sleep(drainWait.Get(&s.st.SV))
	} else {
		// We're disableing the drain. Make the server
		// operational again at the end.
		defer func() { s.grpc.setMode(modeOperational) }()
	}
	// Since enabling the SQL table lease manager's draining mode will
	// prevent the acquisition of new leases, the switch must be made
	// after the pgServer has given sessions a chance to finish ongoing
	// work.
	defer s.leaseMgr.SetDraining(setTo)

	if !setTo {
		// Re-enable client connections.
		s.distSQLServer.Undrain(ctx)
		s.pgServer.Undrain()
		return nil
	}

	// Disable incoming SQL clients up to the queryWait timeout.
	drainMaxWait := queryWait.Get(&s.st.SV)
	if err := s.pgServer.Drain(drainMaxWait); err != nil {
		return err
	}
	// Stop ongoing SQL execution up to the queryWait timeout.
	s.distSQLServer.Drain(ctx, drainMaxWait)

	// Done. This executes the defers set above to (un)drain SQL leases.
	return nil
}

// drainRangeLeases starts draining range leases if setTo is true, and stops draining leases if setTo is false.
func (s *Server) drainRangeLeases(ctx context.Context, setTo bool) error {
	s.nodeLiveness.SetDraining(ctx, setTo)
	return s.node.SetDraining(setTo)
}
