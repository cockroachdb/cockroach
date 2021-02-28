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
	"os"

	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/spf13/cobra"
)

// connectCmd triggers a TLS initialization handshake and writes
// certificates in the specified certs-dir for use with start.
var connectCmd = &cobra.Command{
	Use:   "connect --certs-dir=<path to cockroach certs dir> --init-token=<shared secret> --join=<host 1>,<host 2>,...,<host N>",
	Short: "auto-build TLS certificates for use with the start command",
	Long: `
Connects to other nodes and negotiates an initialization bundle for use with
secure inter-node connections.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runConnect),
}

// runConnect connects to other nodes and negotiates an initialization bundle
// for use with secure inter-node connections.
func runConnect(cmd *cobra.Command, args []string) (retErr error) {
	peers := []string(serverCfg.JoinList)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Ensure that the default hostnames / ports are filled in for the
	// various address fields in baseCfg.
	if err := baseCfg.ValidateAddrs(ctx); err != nil {
		return err
	}

	// We are creating listeners so that if the host part of the listen
	// address means "all interfaces", the Listen call will resolve this
	// into a concrete network address. We need all separate listeners
	// because the certs will want to use the advertised addresses.
	rpcLn, err := server.ListenAndUpdateAddrs(ctx, &baseCfg.Addr, &baseCfg.AdvertiseAddr, "rpc")
	if err != nil {
		return err
	}
	defer func() {
		_ = rpcLn.Close()
	}()
	httpLn, err := server.ListenAndUpdateAddrs(ctx, &baseCfg.HTTPAddr, &baseCfg.HTTPAdvertiseAddr, "http")
	if err != nil {
		return err
	}
	if baseCfg.SplitListenSQL {
		sqlLn, err := server.ListenAndUpdateAddrs(ctx, &baseCfg.SQLAddr, &baseCfg.SQLAdvertiseAddr, "sql")
		if err != nil {
			return err
		}
		_ = sqlLn.Close()
	}
	// Note: we want the http listener to remain open while we open the
	// SQL listener above to detect port conflict in the configuration
	// properly.
	_ = httpLn.Close()

	defer func() {
		if retErr == nil {
			fmt.Println("server certificate generation complete.\n" +
				"Files were generated in: " + os.ExpandEnv(baseCfg.SSLCertsDir) + "\n\n" +
				"Do not forget to generate a client certificate for the 'root' user!\n" +
				"This must be done manually, preferably from a different unix user account\n" +
				"than the one running the server. Eample command:\n\n" +
				"   " + os.Args[0] + " cert create-client root --ca-key=<path-to-client-ca-key>\n")
		}
	}()

	return server.InitHandshake(ctx, baseCfg, startCtx.initToken, startCtx.numExpectedNodes, peers, baseCfg.SSLCertsDir, rpcLn)
}
