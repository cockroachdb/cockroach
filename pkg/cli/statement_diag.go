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
	"bytes"
	"context"
	"fmt"
	"strconv"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var stmtDiagCmd = &cobra.Command{
	Use:   "statement-diag [command] [options]",
	Short: "commands for managing statement diagnostics bundles",
	Long: `This set of commands can be used to manage and download statement diagnostic
bundles, and to cancel outstanding diagnostics activation requests. Statement
diagnostics can be activated from the UI or using EXPLAIN ANALYZE (DEBUG).`,
	RunE: UsageAndErr,
}

var stmtDiagListCmd = &cobra.Command{
	Use:   "list",
	Short: "list available bundles and outstanding activation requests",
	Long: `List statement diagnostics that are available for download and outstanding
diagnostics activation requests.`,
	Args: cobra.NoArgs,
	RunE: clierrorplus.MaybeDecorateError(runStmtDiagList),
}

func runStmtDiagList(cmd *cobra.Command, args []string) (resErr error) {
	const timeFmt = "2006-01-02 15:04:05 MST"

	conn, err := makeSQLClient("cockroach statement-diag", useSystemDb)
	if err != nil {
		return err
	}
	defer func() { resErr = errors.CombineErrors(resErr, conn.Close()) }()

	ctx := context.Background()

	// -- List bundles --
	bundles, err := clisqlclient.StmtDiagListBundles(ctx, conn)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	if len(bundles) == 0 {
		fmt.Printf("No statement diagnostics bundles available.\n")
	} else {
		fmt.Printf("Statement diagnostics bundles:\n")
		w := tabwriter.NewWriter(&buf, 4, 0, 2, ' ', 0)
		fmt.Fprint(w, "  ID\tCollection time\tStatement\n")
		for _, b := range bundles {
			fmt.Fprintf(w, "  %d\t%s\t%s\n", b.ID, b.CollectedAt.UTC().Format(timeFmt), b.Statement)
		}
		_ = w.Flush()
		// When we show a list of bundles, we want an extra blank line.
		fmt.Println(buf.String())
	}

	// -- List outstanding activation requests --
	reqs, err := clisqlclient.StmtDiagListOutstandingRequests(ctx, conn)
	if err != nil {
		return err
	}

	buf.Reset()
	if len(reqs) == 0 {
		fmt.Printf("No outstanding activation requests.\n")
	} else {
		fmt.Printf("Outstanding activation requests:\n")
		w := tabwriter.NewWriter(&buf, 4, 0, 2, ' ', 0)
		fmt.Fprint(w, "  ID\tActivation time\tStatement\tMin execution latency\tExpires at\n")
		for _, r := range reqs {
			minExecLatency := "N/A"
			if r.MinExecutionLatency != 0 {
				minExecLatency = r.MinExecutionLatency.String()
			}
			expiresAt := "never"
			if !r.ExpiresAt.IsZero() {
				expiresAt = r.ExpiresAt.String()
			}
			fmt.Fprintf(
				w, "  %d\t%s\t%s\t%s\t%s\n",
				r.ID, r.RequestedAt.UTC().Format(timeFmt), r.Statement, minExecLatency, expiresAt,
			)
		}
		_ = w.Flush()
		fmt.Print(buf.String())
	}

	return nil
}

var stmtDiagDownloadCmd = &cobra.Command{
	Use:   "download <bundle id> [<filename>]",
	Short: "download statement diagnostics bundle into a zip file",
	Long: `Download statement diagnostics bundle into a zip file, using an ID returned by
the list command.`,
	Args: cobra.RangeArgs(1, 2),
	RunE: clierrorplus.MaybeDecorateError(runStmtDiagDownload),
}

func runStmtDiagDownload(cmd *cobra.Command, args []string) (resErr error) {
	id, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil || id < 0 {
		return errors.New("invalid bundle ID")
	}
	var filename string
	if len(args) > 1 {
		filename = args[1]
	} else {
		filename = fmt.Sprintf("stmt-bundle-%d.zip", id)
	}

	conn, err := makeSQLClient("cockroach statement-diag", useSystemDb)
	if err != nil {
		return err
	}
	defer func() { resErr = errors.CombineErrors(resErr, conn.Close()) }()

	if err := clisqlclient.StmtDiagDownloadBundle(
		context.Background(), conn, id, filename); err != nil {
		return err
	}
	fmt.Printf("Bundle saved to %q\n", filename)
	return nil
}

var stmtDiagDeleteCmd = &cobra.Command{
	Use:   "delete { --all | <bundle id> }",
	Short: "delete statement diagnostics bundles",
	Long: `Delete a statement diagnostics bundle using an ID returned by the list
command, or delete all bundles.`,
	Args: cobra.MaximumNArgs(1),
	RunE: clierrorplus.MaybeDecorateError(runStmtDiagDelete),
}

func runStmtDiagDelete(cmd *cobra.Command, args []string) (resErr error) {
	conn, err := makeSQLClient("cockroach statement-diag", useSystemDb)
	if err != nil {
		return err
	}
	defer func() { resErr = errors.CombineErrors(resErr, conn.Close()) }()

	ctx := context.Background()

	if stmtDiagCtx.all {
		if len(args) > 0 {
			return errors.New("extra arguments with --all")
		}
		return clisqlclient.StmtDiagDeleteAllBundles(ctx, conn)
	}
	if len(args) != 1 {
		return fmt.Errorf("accepts 1 arg, received %d", len(args))
	}

	id, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil || id < 0 {
		return errors.New("invalid ID")
	}

	return clisqlclient.StmtDiagDeleteBundle(ctx, conn, id)
}

var stmtDiagCancelCmd = &cobra.Command{
	Use:   "cancel { -all | <request id> }",
	Short: "cancel outstanding activation requests",
	Long: `Cancel an outstanding activation request, using an ID returned by the
list command, or cancel all outstanding requests.`,
	Args: cobra.MaximumNArgs(1),
	RunE: clierrorplus.MaybeDecorateError(runStmtDiagCancel),
}

func runStmtDiagCancel(cmd *cobra.Command, args []string) (resErr error) {
	conn, err := makeSQLClient("cockroach statement-diag", useSystemDb)
	if err != nil {
		return err
	}
	defer func() { resErr = errors.CombineErrors(resErr, conn.Close()) }()

	ctx := context.Background()

	if stmtDiagCtx.all {
		if len(args) > 0 {
			return errors.New("extra arguments with --all")
		}
		return clisqlclient.StmtDiagCancelAllOutstandingRequests(ctx, conn)
	}
	if len(args) != 1 {
		return fmt.Errorf("accepts 1 arg, received %d", len(args))
	}

	id, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil || id < 0 {
		return errors.New("invalid ID")
	}

	return clisqlclient.StmtDiagCancelOutstandingRequest(ctx, conn, id)
}

var stmtDiagCmds = []*cobra.Command{
	stmtDiagListCmd,
	stmtDiagDownloadCmd,
	stmtDiagDeleteCmd,
	stmtDiagCancelCmd,
}

func init() {
	stmtDiagCmd.AddCommand(stmtDiagCmds...)
}
