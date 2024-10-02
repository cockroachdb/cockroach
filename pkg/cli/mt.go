// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/spf13/cobra"
)

func init() {
	cockroachCmd.AddCommand(MTCmd)
	MTCmd.AddCommand(mtStartSQLCmd)

	mtCertsCmd.AddCommand(
		mtCreateTenantCACertCmd,
		mtCreateTenantCertCmd,
		mtCreateTenantSigningCertCmd,
	)

	MTCmd.AddCommand(mtCertsCmd)
}

// MTCmd is the base command for functionality related to multi-tenancy.
var MTCmd = &cobra.Command{
	Use:   "mt [command]",
	Short: "commands related to multi-tenancy",
	Long: `
Commands related to multi-tenancy.

This functionality is **experimental** and for internal use only.
`,
	RunE:   UsageAndErr,
	Hidden: true,
}

var mtCertsCmd = &cobra.Command{
	Use:   "cert [command]",
	Short: "certificate creation for multi-tenancy",
	Long: `
Commands that create certificates for multi-tenancy.
These are useful mostly for testing. In production deployments the certificates
will not be collected in one place and not all of them will be issued using this
command.

This functionality is **experimental** and for internal use only.
`,
	RunE: UsageAndErr,
}

// ClearStoresAndSetupLoggingForMTCommands will clear the cluster name, the
// store specs, and then sets up default logging.
func ClearStoresAndSetupLoggingForMTCommands(
	cmd *cobra.Command, ctx context.Context,
) (*stop.Stopper, error) {
	serverCfg.ClusterName = ""
	serverCfg.Stores.Specs = nil

	stopper, err := setupAndInitializeLoggingAndProfiling(ctx, cmd, false /* isServerCmd */)
	if err != nil {
		return nil, err
	}
	return stopper, nil
}
