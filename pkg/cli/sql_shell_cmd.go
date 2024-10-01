// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cli/clienturl"
	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// sqlShellCmd opens a sql shell.
var sqlShellCmd = &cobra.Command{
	Use:   "sql [options]",
	Short: "open a sql shell",
	Long: `
Open a sql shell running against a cockroach database.
`,
	Args: cobra.NoArgs,
	RunE: clierrorplus.MaybeDecorateError(runTerm),
}

func runTerm(cmd *cobra.Command, args []string) (resErr error) {
	closeFn, err := sqlCtx.Open(os.Stdin)
	if err != nil {
		return err
	}
	defer closeFn()

	if cliCtx.IsInteractive {
		// The user only gets to see the welcome message on interactive sessions.
		// Refer to README.md to understand the general design guidelines for
		// help texts.
		const welcomeMessage = `#
# Welcome to the CockroachDB SQL shell.
# All statements must be terminated by a semicolon.
# To exit, type: \q.
#
`
		fmt.Print(welcomeMessage)
	}

	ctx := context.Background()
	conn, err := makeSQLClient(ctx, catconstants.InternalSQLAppName, useDefaultDb)
	if err != nil {
		return err
	}
	defer func() { resErr = errors.CombineErrors(resErr, conn.Close()) }()

	sqlCtx.ShellCtx.CertsDir = baseCfg.SSLCertsDir
	sqlCtx.ShellCtx.ParseURL = clienturl.MakeURLParserFn(cmd, cliCtx.clientOpts)
	return sqlCtx.Run(ctx, conn)
}
