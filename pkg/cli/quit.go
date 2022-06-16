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

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// quitCmd command shuts down the node server.
// TODO(irfansharif): Delete this subcommand once v20.2 is cut.
var quitCmd = &cobra.Command{
	Use:   "quit",
	Short: "drain and shut down a node\n",
	Long: `
Shut down the server. The first stage is drain, where the server stops accepting
client connections, then stops extant connections, and finally pushes range
leases onto other nodes, subject to various timeout parameters configurable via
cluster settings. After the first stage completes, the server process is shut
down.

If an argument is specified, the command affects the node
whose ID is given. If --self is specified, the command
affects the node that the command is connected to (via --host).
`,
	Args: cobra.MaximumNArgs(1),
	RunE: clierrorplus.MaybeDecorateError(runQuit),
	Deprecated: `see 'cockroach node drain' instead to drain a 
server without terminating the server process (which can in turn be done using 
an orchestration layer or a process manager, or by sending a termination signal
directly).`,
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

	if !drainCtx.nodeDrainSelf && len(args) == 0 {
		fmt.Fprintf(stderr, "warning: draining a node without node ID or passing --self explicitly is deprecated.\n")
		drainCtx.nodeDrainSelf = true
	}
	if drainCtx.nodeDrainSelf && len(args) > 0 {
		return errors.Newf("cannot use --%s with an explicit node ID", cliflags.NodeDrainSelf.Name)
	}

	targetNode := "local"
	if len(args) > 0 {
		targetNode = args[0]
	}

	// Establish a RPC connection.
	c, finish, err := getAdminClient(ctx, serverCfg)
	if err != nil {
		return err
	}
	defer finish()

	return drainAndShutdown(ctx, c, targetNode)
}
