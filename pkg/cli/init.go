// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
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
	RunE: clierrorplus.MaybeDecorateError(runInit),
}

var initCmdOptions = struct {
	virtualized      bool
	virtualizedEmpty bool
}{}

func runInit(cmd *cobra.Command, args []string) error {
	if initCmdOptions.virtualized && initCmdOptions.virtualizedEmpty {
		return errors.Newf("only one of --virtualized and --virtualized-empty can be used")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Wait for the node to be ready for initialization.
	if err := dialAndCheckHealth(ctx); err != nil {
		return err
	}
	conn, finish, err := getClientGRPCConn(ctx, serverCfg)
	if err != nil {
		return err
	}
	defer finish()

	typ := serverpb.InitType_NONE
	if initCmdOptions.virtualized {
		typ = serverpb.InitType_VIRTUALIZED
	} else if initCmdOptions.virtualizedEmpty {
		typ = serverpb.InitType_VIRTUALIZED_EMPTY
	}

	// Actually perform cluster initialization.
	c := serverpb.NewInitClient(conn)
	if _, err = c.Bootstrap(ctx, &serverpb.BootstrapRequest{
		InitType: typ,
	}); err != nil {
		return err
	}

	fmt.Fprintln(os.Stdout, "Cluster successfully initialized")
	return nil
}

// dialAndCheckHealth waits for the node to be ready for initialization. This
// check ensures that the `init` command is less likely to fail because it was
// issued too early. In general, retrying the `init` command is dangerous [0],
// so we make a best effort at minimizing chances for users to arrive in an
// uncomfortable situation.
//
// [0]: https://github.com/cockroachdb/cockroach/pull/19753#issuecomment-341561452
func dialAndCheckHealth(ctx context.Context) error {
	retryOpts := retry.Options{InitialBackoff: time.Second, MaxBackoff: time.Second}

	tryConnect := func(ctx context.Context) error {
		// (Attempt to) establish the gRPC connection. If that fails,
		// it may be that the server hasn't started to listen yet, in
		// which case we'll retry.
		conn, finish, err := getClientGRPCConn(ctx, serverCfg)
		if err != nil {
			return err
		}
		defer finish()

		// Access the /health endpoint. Until/unless this succeeds, the
		// node is not yet fully initialized and ready to accept
		// Bootstrap requests.
		ac := serverpb.NewAdminClient(conn)
		_, err = ac.Health(ctx, &serverpb.HealthRequest{})
		return err
	}

	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		if err := timeutil.RunWithTimeout(
			ctx, "init-open-conn", 5*time.Second, tryConnect,
		); err != nil {
			err = errors.Wrapf(err, "node not ready to perform cluster initialization")
			fmt.Fprintln(stderr, "warning:", err, "(retrying)")
			continue
		}

		// We managed to connect and run a sanity check. Note however that `conn` in
		// `tryConnect` was established using a context that is now canceled, so we
		// let the caller do its own dial.
		return nil
	}
	return errors.New("maximum number of retries exceeded")
}
