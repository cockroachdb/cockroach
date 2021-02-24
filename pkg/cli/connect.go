// Copyright 2021 The Cockroach Authors.
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
	"net"

	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/spf13/cobra"
)

// connectCmd triggers a TLS initialization handshake and writes
// certificates in the specified certs-dir for use with start.
var connectCmd = &cobra.Command{
	Use:   "connect --certs-dir=<path to cockroach certs dir> --init-token=<shared secret> --join=<host 1>,<host 2>,...,<host N>",
	Short: "build TLS certificates for use with the start command",
	Long: `
Connects to other nodes and negotiates an initialization bundle for use with
secure inter-node connections.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runConnect),
}

// runConnect connects to other nodes and negotiates an initialization bundle
// for use with secure inter-node connections.
func runConnect(cmd *cobra.Command, args []string) error {
	peers := []string(serverCfg.JoinList)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", startCtx.serverListenAddr, serverListenPort))
	if err != nil {
		return err
	}
	defer func() {
		_ = listener.Close()
	}()

	return server.InitHandshake(ctx, baseCfg, baseCfg.InitToken, len(peers), peers, baseCfg.SSLCertsDir, listener)
}
