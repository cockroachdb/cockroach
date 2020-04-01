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
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// getAdminClient returns an AdminClient and a closure that must be invoked
// to free associated resources.
func getAdminClient(ctx context.Context, cfg server.Config) (serverpb.AdminClient, func(), error) {
	conn, _, finish, err := getClientGRPCConn(ctx, cfg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Failed to connect to the node")
	}
	return serverpb.NewAdminClient(conn), finish, nil
}

// quitCmd command shuts down the node server.
var quitCmd = &cobra.Command{
	Use:   "quit",
	Short: "drain and shutdown node\n",
	Long: `
Shutdown the server. The first stage is drain, where any new requests
will be ignored by the server. When all extant requests have been
completed, the server exits.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runQuit),
}

// checkNodeRunning performs a no-op RPC and returns an error if it failed to
// connect to the server.
func checkNodeRunning(ctx context.Context, c serverpb.AdminClient) error {
	// Send a no-op Drain request.
	stream, err := c.Drain(ctx, &serverpb.DrainRequest{
		On:       nil,
		Shutdown: false,
	})
	if err != nil {
		return errors.Wrap(err, "Failed to connect to the node: error sending drain request")
	}
	// Ignore errors from the stream. We've managed to connect to the node above,
	// and that's all that this function is interested in.
	for {
		if _, err := stream.Recv(); err != nil {
			if server.IsWaitingForInit(err) {
				return err
			}
			if err != io.EOF {
				log.Warningf(ctx, "unexpected error from no-op Drain request: %s", err)
			}
			break
		}
	}
	return nil
}

// doShutdown attempts to trigger a server shutdown. When given an empty
// onModes slice, it's a hard shutdown.
//
// errTryHardShutdown is returned if the caller should do a hard-shutdown.
func doShutdown(ctx context.Context, c serverpb.AdminClient, onModes []int32) error {
	// We want to distinguish between the case in which we can't even connect to
	// the server (in which case we don't want our caller to try to come back with
	// a hard retry) and the case in which an attempt to shut down fails (times
	// out, or perhaps drops the connection while waiting). To that end, we first
	// run a noop DrainRequest. If that fails, we give up.
	if err := checkNodeRunning(ctx, c); err != nil {
		if server.IsWaitingForInit(err) {
			return fmt.Errorf("node cannot be shut down before it has been initialized")
		}
		if grpcutil.IsClosedConnection(err) {
			return nil
		}
		return err
	}
	// Send a drain request and continue reading until the connection drops (which
	// then counts as a success, for the connection dropping is likely the result
	// of the Stopper having reached the final stages of shutdown).
	stream, err := c.Drain(ctx, &serverpb.DrainRequest{
		On:       onModes,
		Shutdown: true,
	})
	if err != nil {
		//  This most likely means that we shut down successfully. Note that
		//  sometimes the connection can be shut down even before a DrainResponse gets
		//  sent back to us, so we don't require a response on the stream (see
		//  #14184).
		if grpcutil.IsClosedConnection(err) {
			return nil
		}
		return errors.Wrap(err, "Error sending drain request")
	}
	for {
		if _, err := stream.Recv(); err != nil {
			if grpcutil.IsClosedConnection(err) {
				return nil
			}
			// Unexpected error; the caller should try again (and harder).
			return errTryHardShutdown{err}
		}
	}
}

type errTryHardShutdown struct{ error }

// runQuit accesses the quit shutdown path.
func runQuit(cmd *cobra.Command, args []string) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer func() {
		if err == nil {
			fmt.Println("ok")
		}
	}()
	onModes := make([]int32, len(server.GracefulDrainModes))
	for i, m := range server.GracefulDrainModes {
		onModes[i] = int32(m)
	}

	c, finish, err := getAdminClient(ctx, serverCfg)
	if err != nil {
		return err
	}
	defer finish()

	if quitCtx.serverDecommission {
		var myself []roachpb.NodeID // will remain empty, which means target yourself
		if err := runDecommissionNodeImpl(ctx, c, nodeDecommissionWaitAll, myself); err != nil {
			return err
		}
	}
	errChan := make(chan error, 1)
	go func() {
		errChan <- doShutdown(ctx, c, onModes)
	}()
	select {
	case err := <-errChan:
		if err != nil {
			if _, ok := err.(errTryHardShutdown); ok {
				log.Warningf(ctx, "graceful shutdown failed: %s; proceeding with hard shutdown\n", err)
				break
			}
			return err
		}
		return nil
	case <-time.After(time.Minute):
		log.Warningf(ctx, "timed out; proceeding with hard shutdown")
	}
	// Not passing drain modes tells the server to not bother and go
	// straight to shutdown. We try two times just in case there is a transient error.
	err = doShutdown(ctx, c, nil)
	if err != nil {
		log.Warningf(ctx, "hard shutdown attempt failed, retrying: %v", err)
		err = doShutdown(ctx, c, nil)
	}
	return errors.Wrap(doShutdown(ctx, c, nil), "hard shutdown failed")
}
