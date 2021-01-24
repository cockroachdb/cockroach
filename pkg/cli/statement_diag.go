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
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var stmtDiagCmd = &cobra.Command{
	Use:   "statement-diag [command]",
	Short: "commands for managing statement diagnostics bundles",
	Long: `This set of commands can be used to manage and download statement diagnostic
bundles, and to cancel outstanding diagnostics activation requests. Statement
diagnostics can be activated from the UI or using EXPLAIN ANALYZE (DEBUG).`,
	RunE: usageAndErr,
}

var stmtDiagListCmd = &cobra.Command{
	Use:   "list [options]",
	Short: "list available bundles and outstanding activation requests",
	Long: `List statement diagnostics that are available for download and outstanding
diagnostics activation requests.`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runStmtDiagList),
}

func runStmtDiagList(cmd *cobra.Command, args []string) error {
	const timeFmt = "2006-01-02 15:04:05 MST"

	conn, err := makeSQLClient("cockroach statement-diag", useSystemDb)
	if err != nil {
		return err
	}
	defer conn.Close()

	// -- List bundles --

	rows, err := conn.Query(
		`SELECT id, statement_fingerprint, collected_at
		 FROM system.statement_diagnostics
		 WHERE error IS NULL
		 ORDER BY collected_at DESC`,
		nil, /* args */
	)
	if err != nil {
		return err
	}
	vals := make([]driver.Value, 3)
	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 4, 0, 2, ' ', 0)
	fmt.Fprint(w, "  ID\tCollection time\tStatement\n")
	num := 0
	for {
		if err := rows.Next(vals); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		id := vals[0].(int64)
		stmt := vals[1].(string)
		t := vals[2].(time.Time)
		fmt.Fprintf(w, "  %d\t%s\t%s\n", id, t.UTC().Format(timeFmt), stmt)
		num++
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if num == 0 {
		fmt.Printf("No statement diagnostics bundles available.\n")
	} else {
		fmt.Printf("Statement diagnostics bundles:\n")
		_ = w.Flush()
		// When we show a list of bundles, we want an extra blank line.
		fmt.Println(buf.String())
	}

	// -- List outstanding activation requests --

	rows, err = conn.Query(
		`SELECT id, statement_fingerprint, requested_at
		 FROM system.statement_diagnostics_requests
		 WHERE NOT completed
		 ORDER BY requested_at DESC`,
		nil, /* args */
	)
	if err != nil {
		return err
	}

	buf.Reset()
	w = tabwriter.NewWriter(&buf, 4, 0, 2, ' ', 0)
	fmt.Fprint(w, "  ID\tActivation time\tStatement\n")
	num = 0
	for {
		if err := rows.Next(vals); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		id := vals[0].(int64)
		stmt := vals[1].(string)
		t := vals[2].(time.Time)
		fmt.Fprintf(w, "  %d\t%s\t%s\n", id, t.UTC().Format(timeFmt), stmt)
		num++
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if num == 0 {
		fmt.Printf("No outstanding activation requests.\n")
	} else {
		fmt.Printf("Outstanding activation requests:\n")
		_ = w.Flush()
		fmt.Print(buf.String())
	}

	return nil
}

var stmtDiagDownloadCmd = &cobra.Command{
	Use:   "download <bundle id> <file> [options]",
	Short: "download statement diagnostics bundle into a zip file",
	Long: `Download statement diagnostics bundle into a zip file, using an ID returned by
the list command.`,
	Args: cobra.ExactArgs(2),
	RunE: MaybeDecorateGRPCError(runStmtDiagDownload),
}

func runStmtDiagDownload(cmd *cobra.Command, args []string) error {
	id, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil || id < 0 {
		return errors.New("invalid bundle id")
	}
	filename := args[1]

	conn, err := makeSQLClient("cockroach statement-diag", useSystemDb)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Retrieve the chunk IDs; these are stored in an INT ARRAY column.
	rows, err := conn.Query(
		"SELECT unnest(bundle_chunks) FROM system.statement_diagnostics WHERE id = $1",
		[]driver.Value{id},
	)
	if err != nil {
		return err
	}
	var chunkIDs []int64
	vals := make([]driver.Value, 1)
	for {
		if err := rows.Next(vals); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		chunkIDs = append(chunkIDs, vals[0].(int64))
	}
	if err := rows.Close(); err != nil {
		return err
	}

	if len(chunkIDs) == 0 {
		return errors.Newf("no statement diagnostics bundle with ID %d", id)
	}

	// Create the file and write out the chunks.
	out, err := os.Create(filename)
	if err != nil {
		return err
	}

	for _, chunkID := range chunkIDs {
		data, err := conn.QueryRow(
			"SELECT data FROM system.statement_bundle_chunks WHERE id = $1",
			[]driver.Value{chunkID},
		)
		if err != nil {
			_ = out.Close()
			return err
		}
		if _, err := out.Write(data[0].([]byte)); err != nil {
			_ = out.Close()
			return err
		}
	}

	return out.Close()
}

var stmtDiagDeleteCmd = &cobra.Command{
	Use:   "delete { --all | <bundle id> }",
	Short: "delete statement diagnostics bundles",
	Long: `Delete a statement diagnostics bundle using an ID returned by the list
command, or delete all bundles.`,
	Args: cobra.MaximumNArgs(1),
	RunE: MaybeDecorateGRPCError(runStmtDiagDelete),
}

func runStmtDiagDelete(cmd *cobra.Command, args []string) error {
	conn, err := makeSQLClient("cockroach statement-diag", useSystemDb)
	if err != nil {
		return err
	}
	defer conn.Close()

	if stmtDiagCtx.all {
		if len(args) > 0 {
			return errors.New("extra arguments with --all")
		}
		return runStmtDiagDeleteAll(conn)
	}
	if len(args) != 1 {
		return fmt.Errorf("accepts 1 arg, received %d", len(args))
	}

	id, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil || id < 0 {
		return errors.New("invalid id")
	}

	_, err = conn.QueryRow(
		"SELECT 1 FROM system.statement_diagnostics WHERE id = $1",
		[]driver.Value{id},
	)
	if err != nil {
		if err == io.EOF {
			return errors.Newf("no statement diagnostics bundle with ID %d", id)
		}
		return err
	}

	return conn.ExecTxn(func(conn *sqlConn) error {
		// Delete the request metadata.
		if err := conn.Exec(
			"DELETE FROM system.statement_diagnostics_requests WHERE statement_diagnostics_id = $1",
			[]driver.Value{id},
		); err != nil {
			return err
		}
		// Delete the bundle chunks.
		if err := conn.Exec(
			`DELETE FROM system.statement_bundle_chunks
			  WHERE id IN (
				  SELECT unnest(bundle_chunks) FROM system.statement_diagnostics WHERE id = $1
				)`,
			[]driver.Value{id},
		); err != nil {
			return err
		}
		// Finally, delete the diagnostics entry.
		return conn.Exec(
			"DELETE FROM system.statement_diagnostics WHERE id = $1",
			[]driver.Value{id},
		)
	})
}

func runStmtDiagDeleteAll(conn *sqlConn) error {
	return conn.ExecTxn(func(conn *sqlConn) error {
		// Delete the request metadata.
		if err := conn.Exec(
			"DELETE FROM system.statement_diagnostics_requests WHERE completed",
			nil,
		); err != nil {
			return err
		}
		// Delete all bundle chunks.
		if err := conn.Exec(
			`DELETE FROM system.statement_bundle_chunks WHERE true`,
			nil,
		); err != nil {
			return err
		}
		// Finally, delete the diagnostics entry.
		return conn.Exec(
			"DELETE FROM system.statement_diagnostics WHERE true",
			nil,
		)
	})
}

var stmtDiagCancelCmd = &cobra.Command{
	Use:   "cancel { -all | <request id> }",
	Short: "cancel outstanding activation requests",
	Long: `Cancel an outstanding activation request, using an ID returned by the
list command, or cancel all outstanding requests.`,
	Args: cobra.MaximumNArgs(1),
	RunE: MaybeDecorateGRPCError(runStmtDiagCancel),
}

func runStmtDiagCancel(cmd *cobra.Command, args []string) error {
	conn, err := makeSQLClient("cockroach statement-diag", useSystemDb)
	if err != nil {
		return err
	}
	defer conn.Close()

	if stmtDiagCtx.all {
		if len(args) > 0 {
			return errors.New("extra arguments with --all")
		}
		return conn.Exec(
			"DELETE FROM system.statement_diagnostics_requests WHERE NOT completed",
			nil,
		)
	}
	if len(args) != 1 {
		return fmt.Errorf("accepts 1 arg, received %d", len(args))
	}

	id, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil || id < 0 {
		return errors.New("invalid id")
	}

	_, err = conn.QueryRow(
		"DELETE FROM system.statement_diagnostics_requests WHERE id = $1 RETURNING id",
		[]driver.Value{id},
	)
	if err != nil {
		if err == io.EOF {
			return errors.Newf("no outstanding activation requests with ID %d", id)
		}
		return err
	}
	return nil
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
