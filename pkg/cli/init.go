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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
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
you would use for the sql command). The target of the init command
must appear in the --join flags of other nodes.

A node started without the --join flag initializes itself as a
single-node cluster, so the init command is not used in that case.
`,
	Args: cobra.NoArgs,
	RunE: maybeShoutError(MaybeDecorateGRPCError(runInit)),
}

func runInit(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, _, finish, err := getClientGRPCConn(ctx, serverCfg)
	if err != nil {
		return err
	}
	defer finish()

	c := serverpb.NewInitClient(conn)

	if _, err = c.Bootstrap(ctx, &serverpb.BootstrapRequest{}); err != nil {
		if strings.Contains(err.Error(), server.ErrClusterInitialized.Error()) {
			// We really want to use errors.Is() here but this would require
			// error serialization support in gRPC.
			// This is not yet performed in CockroachDB even though
			// the error library now has infrastructure to do so, see:
			// https://github.com/cockroachdb/errors/pull/14
			return errors.WithHint(err,
				"Please ensure all your start commands are using --join.")
		}
		return err
	}

	fmt.Fprintln(os.Stdout, "Cluster successfully initialized")
	return nil
}
