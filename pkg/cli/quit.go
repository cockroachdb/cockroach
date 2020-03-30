// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// quitCmd command shuts down the node server.
var quitCmd = &cobra.Command{
	Use:   "quit",
	Short: "drain and shut down a node\n",
	Long: `
Shut down the server. The first stage is drain, where the server
stops accepting client connections, then stops extant
connections, and finally pushes range leases onto other nodes,
subject to various timeout parameters configurable via
cluster settings. After the first stage completes,
the server process is shut down.

See also 'cockroach node drain' to drain a server
without stopping the server process.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runQuit),
}

// runQuit accesses the quit shutdown path.
func runQuit(cmd *cobra.Command, args []string) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// At the end, we'll report "ok" if there was no error.
	defer func() {
		if err == nil {
			fmt.Println("ok")
		}
	}()

	// Establish a RPC connection.
	c, finish, err := getAdminClient(ctx, serverCfg)
	if err != nil {
		return err
	}
	defer finish()

	// If --decommission was passed, perform the decommission as first
	// step. (Note that this flag is deprecated. It will be removed.)
	if quitCtx.serverDecommission {
		var myself []roachpb.NodeID // will remain empty, which means target yourself
		if err := runDecommissionNodeImpl(ctx, c, nodeDecommissionWaitAll, myself); err != nil {
			log.Warningf(ctx, "%v", err)
			if server.IsWaitingForInit(err) {
				err = errors.New("node cannot be decommissioned before it has been initialized")
			}
			return err
		}
	}

	return drainAndShutdown(ctx, c)
}

// drainAndShutdown attempts to drain the server and then shut it
// down. When given an empty onModes slice, it's a hard shutdown.
func drainAndShutdown(ctx context.Context, c serverpb.AdminClient) (err error) {
	hardError, err := doDrain(ctx, c)
	if hardError {
		return err
	}

	if err != nil {
		log.Warningf(ctx, "drain did not complete successfully; hard shutdown may cause disruption")
	}
	// We have already performed the drain above. We use a nil array
	// of drain modes to indicate no further drain needs to be attempted
	// and go straight to shutdown. We try two times just in case there
	// is a transient error.
	hardErr, err := doShutdown(ctx, c)
	if err != nil && !hardErr {
		log.Warningf(ctx, "hard shutdown attempt failed, retrying: %v", err)
		_, err = doShutdown(ctx, c)
	}
	return errors.Wrap(err, "hard shutdown failed")
}

// doDrain calls a graceful drain.
//
// If the function returns hardError true, then the caller should not
// proceed with an alternate strategy (it's likely the server has gone
// away).
func doDrain(ctx context.Context, c serverpb.AdminClient) (hardError bool, err error) {
	// The next step is to drain. The timeout is configurable
	// via --drain-wait.
	if quitCtx.drainWait == 0 {
		return doDrainNoTimeout(ctx, c)
	}

	err = contextutil.RunWithTimeout(ctx, "drain", quitCtx.drainWait, func(ctx context.Context) (err error) {
		hardError, err = doDrainNoTimeout(ctx, c)
		return err
	})
	if _, ok := err.(*contextutil.TimeoutError); ok {
		log.Infof(ctx, "drain timed out: %v", err)
		err = errors.New("drain timeout")
	}

	return hardError, err
}

func doDrainNoTimeout(ctx context.Context, c serverpb.AdminClient) (hardError bool, err error) {
	defer func() {
		if server.IsWaitingForInit(err) {
			log.Infof(ctx, "%v", err)
			err = errors.New("node cannot be drained before it has been initialized")
		}
	}()

	// Send a drain request with the drain bit set and the shutdown bit
	// unset.
	stream, err := c.Drain(ctx, &serverpb.DrainRequest{
		DeprecatedProbeIndicator: server.DeprecatedDrainParameter,
		DoDrain:                  true,
	})
	if err != nil {
		return true, errors.Wrap(err, "error sending drain request")
	}
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			// Done.
			break
		}
		if err != nil {
			// Unexpected error.
			log.Infof(ctx, "graceful shutdown failed: %v", err)
			return false, err
		}
		// Iterate until end of stream, which indicates the drain is
		// complete.
	}
	return false, nil
}

// doShutdown attempts to trigger a server shutdown *without*
// draining. Use doDrain() prior to perform a drain, or
// drainAndShutdown() to combine both.
func doShutdown(ctx context.Context, c serverpb.AdminClient) (hardError bool, err error) {
	defer func() {
		if server.IsWaitingForInit(err) {
			log.Infof(ctx, "encountered error: %v", err)
			err = errors.New("node cannot be shut down before it has been initialized")
			err = errors.WithHint(err, "You can still stop the process using a service manager or a signal.")
			hardError = true
		}
		if grpcutil.IsClosedConnection(err) {
			// This most likely means that we shut down successfully. Note
			// that sometimes the connection can be shut down even before a
			// DrainResponse gets sent back to us, so we don't require a
			// response on the stream (see #14184).
			err = nil
		}
	}()

	// We use a shorter timeout because a shutdown request has nothing
	// else to do than shut down the node immediately.
	err = contextutil.RunWithTimeout(ctx, "hard shutdown", 10*time.Second, func(ctx context.Context) error {
		// Send a drain request with the drain bit unset (no drain).
		// and the shutdown bit set.
		stream, err := c.Drain(ctx, &serverpb.DrainRequest{Shutdown: true})
		if err != nil {
			return errors.Wrap(err, "error sending shutdown request")
		}
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
		}
	})
	if _, ok := err.(*contextutil.TimeoutError); !ok {
		hardError = true
	}
	return hardError, err
}

// getAdminClient returns an AdminClient and a closure that must be invoked
// to free associated resources.
func getAdminClient(ctx context.Context, cfg server.Config) (serverpb.AdminClient, func(), error) {
	conn, _, finish, err := getClientGRPCConn(ctx, cfg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Failed to connect to the node")
	}
	return serverpb.NewAdminClient(conn), finish, nil
}
