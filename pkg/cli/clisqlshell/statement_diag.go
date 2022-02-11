// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlshell

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/errors"
)

// handleStatementDiag handles the `\statement-diag` command.
func (c *cliState) handleStatementDiag(
	args []string, loopState, errState cliStateEnum,
) (resState cliStateEnum) {
	var cmd string
	if len(args) > 0 {
		cmd = args[0]
		args = args[1:]
	}

	var cmdErr error
	switch cmd {
	case "list":
		if len(args) > 0 {
			return c.invalidSyntax(errState)
		}
		cmdErr = c.statementDiagList()

	case "download":
		if len(args) < 1 || len(args) > 2 {
			return c.invalidSyntax(errState)
		}
		id, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			return c.invalidSyntaxf(
				errState, "%s", errors.Wrapf(err, "%q is not a valid bundle ID", args[1]),
			)
		}
		var filename string
		if len(args) > 1 {
			filename = args[1]
		} else {
			filename = fmt.Sprintf("stmt-bundle-%d.zip", id)
		}
		cmdErr = clisqlclient.StmtDiagDownloadBundle(
			context.Background(), c.conn, id, filename)
		if cmdErr == nil {
			fmt.Fprintf(c.iCtx.stdout, "Bundle saved to %q\n", filename)
		}

	default:
		return c.invalidSyntax(errState)
	}

	if cmdErr != nil {
		fmt.Fprintln(c.iCtx.stderr, cmdErr)
		c.exitErr = cmdErr
		return errState
	}
	return loopState
}

func (c *cliState) statementDiagList() error {
	const timeFmt = "2006-01-02 15:04:05 MST"

	// -- List bundles --
	bundles, err := clisqlclient.StmtDiagListBundles(context.Background(), c.conn)
	if err != nil {
		return err
	}

	if len(bundles) == 0 {
		fmt.Fprintf(c.iCtx.stdout, "No statement diagnostics bundles available.\n")
	} else {
		var buf bytes.Buffer
		fmt.Fprintf(c.iCtx.stdout, "Statement diagnostics bundles:\n")
		w := tabwriter.NewWriter(&buf, 4, 0, 2, ' ', 0)
		fmt.Fprint(w, "  ID\tCollection time\tStatement\n")
		for _, b := range bundles {
			fmt.Fprintf(w, "  %d\t%s\t%s\n", b.ID, b.CollectedAt.UTC().Format(timeFmt), b.Statement)
		}
		_ = w.Flush()
		_, _ = buf.WriteTo(c.iCtx.stdout)
	}
	fmt.Fprintln(c.iCtx.stdout)

	return nil
}
