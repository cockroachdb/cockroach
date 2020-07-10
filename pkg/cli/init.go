// Copyright 2017 The Cockroach Authors.
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
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "initialize a cluster",
	Long: `
Perform one-time-only initialization of a CockroachDB cluster.

After starting one or more nodes with --join flags, run the init
command on one node (passing the same --host and certificate flags
you would use for the sql command).
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runInit),
}

func runInit(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Wait for the node to be ready for initialization.
	conn, finish, err := waitForClientReadinessAndGetClientGRPCConn(ctx)
	if err != nil {
		return err
	}
	defer finish()

	// Actually perform cluster initialization.
	c := serverpb.NewInitClient(conn)
	if _, err = c.Bootstrap(ctx, &serverpb.BootstrapRequest{}); err != nil {
		return err
	}

	fmt.Fprintln(os.Stdout, "Cluster successfully initialized")
	return nil
}

// waitForClientReadinessAndGetClientGRPCConn waits for the node to
// be ready for initialization. This check ensures that the `init`
// command is less likely to fail because it was issued too
// early. In general, retrying the `init` command is dangerous [0],
// so we make a best effort at minimizing chances for users to
// arrive in an uncomfortable situation.
//
// [0]: https://github.com/cockroachdb/cockroach/pull/19753#issuecomment-341561452
func waitForClientReadinessAndGetClientGRPCConn(
	ctx context.Context,
) (conn *grpc.ClientConn, finish func(), err error) {
	defer func() {
		// If we're returning with an error, tear down the gRPC connection
		// that's been established, if any.
		if finish != nil && err != nil {
			finish()
		}
	}()

	retryOpts := retry.Options{InitialBackoff: time.Second, MaxBackoff: time.Second}
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		if err = contextutil.RunWithTimeout(ctx, "init-open-conn", 5*time.Second,
			func(ctx context.Context) error {
				// (Attempt to) establish the gRPC connection. If that fails,
				// it may be that the server hasn't started to listen yet, in
				// which case we'll retry.
				conn, _, finish, err = getClientGRPCConn(ctx, serverCfg)
				if err != nil {
					return err
				}

				// Access the /health endpoint. Until/unless this succeeds, the
				// node is not yet fully initialized and ready to accept
				// Bootstrap requests.
				ac := serverpb.NewAdminClient(conn)
				_, err := ac.Health(ctx, &serverpb.HealthRequest{})
				return err
			}); err != nil {
			err = errors.Wrapf(err, "node not ready to perform cluster initialization")
			fmt.Fprintln(stderr, "warning:", err, "(retrying)")

			// We're going to retry; first cancel the connection that's
			// been established, if any.
			if finish != nil {
				finish()
				finish = nil
			}
			// Then retry.
			continue
		}

		// No error - connection was established and health endpoint is
		// ready.
		return conn, finish, err
	}
	err = errors.New("maximum number of retries exceeded")
	return
}
