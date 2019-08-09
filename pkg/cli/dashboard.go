// Copyright 2019 The Cockroach Authors.
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
	"github.com/cockroachdb/errors"
	"github.com/skratchdot/open-golang/open"
	"github.com/spf13/cobra"
)

var dashboardCmd = &cobra.Command{
	Use:   "dashboard",
	Short: "open a local browser pointing to the remote web UI",
	Long: `
Connect to a running cluster via one of its nodes, retrieve the URL of
the web UI as advertised by the node, and launch a web browser
on the local machine pointing to this URL automatically.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(func(cmd *cobra.Command, _ []string) error {
		return runDashboard(cmd)
	}),
}

func runDashboard(cmd *cobra.Command) error {
	conn, err := getPasswordAndMakeSQLClient("cockroach dashboard")
	if err != nil {
		return err
	}
	defer conn.Close()

	vals, err := conn.QueryRow(
		`SELECT value FROM crdb_internal.node_runtime_info WHERE component='UI' AND field = 'URL'`, nil,
	)
	if err != nil {
		return err
	}
	if len(vals) == 0 {
		return errors.AssertionFailedf("unable to obtain the web UI URL from the server")
	}

	uiStr := vals[0].(string)
	return errors.WithHintf(
		open.Run(uiStr),
		"You can open the web UI manually in a web browser:\n\n\t%s\n", uiStr)
}
