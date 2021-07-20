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

import "github.com/spf13/cobra"

func init() {
	cockroachCmd.AddCommand(mtCmd)
	mtCmd.AddCommand(mtStartSQLCmd)
	mtCmd.AddCommand(mtStartSQLProxyCmd)
	mtCmd.AddCommand(mtTestDirectorySvr)

	mtCertsCmd.AddCommand(
		mtCreateTenantClientCACertCmd,
		mtCreateTenantClientCertCmd,
	)

	mtCmd.AddCommand(mtCertsCmd)
}

// mtCmd is the base command for functionality related to multi-tenancy.
var mtCmd = &cobra.Command{
	Use:   "mt [command]",
	Short: "commands related to multi-tenancy",
	Long: `
Commands related to multi-tenancy.

This functionality is **experimental** and for internal use only.
`,
	RunE:   usageAndErr,
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
	RunE: usageAndErr,
}
