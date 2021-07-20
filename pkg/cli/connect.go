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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/spf13/cobra"
)

// Sub-commands for connect command.
var connectCmds = []*cobra.Command{
	connectInitCmd,
	connectJoinCmd,
}

var connectCmd = &cobra.Command{
	Use:   "connect [command]",
	Short: "Create certificates for securely connecting with clusters\n",
	Long: `
Bootstrap security certificates for connecting to new or existing clusters.`,
	RunE: usageAndErr,
}

func init() {
	connectCmd.AddCommand(connectCmds...)
}

// connectInitCmd triggers a TLS initialization handshake and writes
// certificates in the specified certs-dir for use with start.
var connectInitCmd = &cobra.Command{
	Use:   "init --certs-dir=<path to cockroach certs dir> --init-token=<shared secret> --join=<host 1>,<host 2>,...,<host N>",
	Short: "auto-build TLS certificates for use with the start command",
	Long: `
Connects to other nodes and negotiates an initialization bundle for use with
secure inter-node connections.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runConnectInit),
}

// runConnectInit connects to other nodes and negotiates an initialization bundle
// for use with secure inter-node connections.
func runConnectInit(cmd *cobra.Command, args []string) (retErr error) {
	if err := validateConnectInitFlags(cmd, true /* requireExplicitFlags */); err != nil {
		return err
	}

	// If the node cert already exists, skip all the complexity of setting up
	// servers, etc.
	cl := security.MakeCertsLocator(baseCfg.SSLCertsDir)
	if exists, err := cl.HasNodeCert(); err != nil {
		return err
	} else if exists {
		return errors.Newf("node certificate already exists in %s", baseCfg.SSLCertsDir)
	}

	// Ensure that log files are populated when the process terminates.
	defer log.Flush()

	peers := []string(serverCfg.JoinList)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx = logtags.AddTag(ctx, "connect", nil)

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
			fmt.Println("server certificate generation complete.\n\n" +
				"cert files generated in: " + os.ExpandEnv(baseCfg.SSLCertsDir) + "\n\n" +
				"Do not forget to generate a client certificate for the 'root' user!\n" +
				"This must be done manually, preferably from a different unix user account\n" +
				"than the one running the server. Example command:\n\n" +
				"   " + os.Args[0] + " cert create-client root --ca-key=<path-to-client-ca-key>\n")
		}
	}()

	reporter := func(format string, args ...interface{}) {
		fmt.Printf(format+"\n", args...)
	}

	return server.InitHandshake(ctx, reporter, baseCfg, startCtx.initToken, startCtx.numExpectedNodes, peers, baseCfg.SSLCertsDir, rpcLn)
}

func validateConnectInitFlags(cmd *cobra.Command, requireExplicitFlags bool) error {
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
