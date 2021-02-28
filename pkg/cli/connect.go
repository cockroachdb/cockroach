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

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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
	if err := validateConnectFlags(cmd, true /* requireExplicitFlags */); err != nil {
		return err
	}

	peers := []string(serverCfg.JoinList)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Infof(ctx, "validating the command line network arguments")

	// Ensure that the default hostnames / ports are filled in for the
	// various address fields in baseCfg.
	if err := baseCfg.ValidateAddrs(ctx); err != nil {
		return err
	}

	log.Ops.Infof(ctx, "starting the initial network listeners")

	// We are creating listeners so that if the host part of the listen
	// address means "all interfaces", the Listen call will resolve this
	// into a concrete network address. We need all separate listeners
	// because the certs will want to use the advertised addresses.
	rpcLn, err := server.ListenAndUpdateAddrs(ctx, &baseCfg.Addr, &baseCfg.AdvertiseAddr, "rpc")
	if err != nil {
		return err
	}
	log.Ops.Infof(ctx, "started rpc listener at: %s", rpcLn.Addr())
	defer func() {
		_ = rpcLn.Close()
	}()
	httpLn, err := server.ListenAndUpdateAddrs(ctx, &baseCfg.HTTPAddr, &baseCfg.HTTPAdvertiseAddr, "http")
	if err != nil {
		return err
	}
	log.Ops.Infof(ctx, "started http listener at: %s", httpLn.Addr())
	if baseCfg.SplitListenSQL {
		sqlLn, err := server.ListenAndUpdateAddrs(ctx, &baseCfg.SQLAddr, &baseCfg.SQLAdvertiseAddr, "sql")
		if err != nil {
			return err
		}
		log.Ops.Infof(ctx, "started sql listener at: %s", sqlLn.Addr())
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

func validateConnectFlags(cmd *cobra.Command, requireExplicitFlags bool) error {
	if requireExplicitFlags {
		f := flagSetForCmd(cmd)
		if !(f.Lookup(cliflags.SingleNode.Name).Changed ||
			(f.Lookup(cliflags.NumExpectedInitialNodes.Name).Changed && f.Lookup(cliflags.InitToken.Name).Changed)) {
			return errors.Newf("either --%s must be passed, or both --%s and --%s",
				cliflags.SingleNode.Name, cliflags.NumExpectedInitialNodes.Name, cliflags.InitToken.Name)
		}
		if f.Lookup(cliflags.SingleNode.Name).Changed &&
			(f.Lookup(cliflags.NumExpectedInitialNodes.Name).Changed || f.Lookup(cliflags.InitToken.Name).Changed) {
			return errors.Newf("--%s cannot be specified together with --%s or --%s",
				cliflags.SingleNode.Name, cliflags.NumExpectedInitialNodes.Name, cliflags.InitToken.Name)
		}
	}

	if startCtx.genCertsForSingleNode {
		startCtx.numExpectedNodes = 1
		startCtx.initToken = "start-single-node"
		return nil
	}

	if startCtx.numExpectedNodes < 1 {
		return errors.Newf("flag --%s must be set to a value greater than or equal to 1",
			cliflags.NumExpectedInitialNodes.Name)
	}
	if startCtx.initToken == "" {
		return errors.Newf("flag --%s must be set to a non-empty string",
			cliflags.InitToken.Name)
	}
	return nil
}
