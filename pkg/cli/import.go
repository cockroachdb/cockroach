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
	"context"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var importDumpFileCmd = &cobra.Command{
	Use:   "db <format> <source>",
	Short: "import a pgdump or mysqldump file into CockroachDB",
	Long: `
Uploads and imports a local dump file into the cockroach cluster via userfile storage.
`,
	Args: cobra.MinimumNArgs(2),
	RunE: maybeShoutError(runDumpFileImport),
}

var importDumpTableCmd = &cobra.Command{
	Use:   "table <table> <format> <source>",
	Short: "import a table from a pgdump or mysqldump file into CockroachDB",
	Long: `
Uploads and imports a table from the local dump file into the cockroach cluster via userfile storage.
`,
	Args: cobra.MinimumNArgs(3),
	RunE: maybeShoutError(runDumpTableImport),
}

func runDumpTableImport(cmd *cobra.Command, args []string) error {
	tableName := args[0]
	importFormat := strings.ToLower(args[1])
	source := args[2]
	conn, err := makeSQLClient("cockroach import table", useDefaultDb)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx := context.Background()
	return runImport(ctx, conn, importFormat, source, tableName, true /* isTableImport */)
}

func runDumpFileImport(cmd *cobra.Command, args []string) error {
	importFormat := strings.ToLower(args[0])
	source := args[1]
	conn, err := makeSQLClient("cockroach import db", useDefaultDb)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx := context.Background()
	return runImport(ctx, conn, importFormat, source, "", false /* isTableImport */)
}

func runImport(
	ctx context.Context, conn *sqlConn, importFormat, source, tableName string, isTableImport bool,
) error {
	if err := conn.ensureConn(); err != nil {
		return err
	}

	reader, err := openUserFile(source)
	if err != nil {
		return err
	}
	defer reader.Close()

	connURL, err := url.Parse(conn.url)
	if err != nil {
		return err
	}

	// Resolve the userfile destination to upload the dump file to.
	userfileDestinationURI := constructUserfileDestinationURI(source, "",
		connURL.User.Username())
	unescapedUserfileURL, err := url.PathUnescape(userfileDestinationURI)
	if err != nil {
		return err
	}

	defer func() {
		// Delete the file chunks which were written as part of this IMPORT.
		_, _ = deleteUserFile(ctx, conn, unescapedUserfileURL)
	}()

	_, err = uploadUserFile(ctx, conn, reader, source, userfileDestinationURI)
	if err != nil {
		return errors.Wrap(err, "failed to upload file to userfile before importing")
	}

	ex := conn.conn.(driver.ExecerContext)
	importCompletedMesssage := func() {
		if isTableImport {
			fmt.Printf("successfully imported table %s from %s file %s\n", tableName, importFormat,
				source)
		} else {
			fmt.Printf("successfully imported %s file %s\n", importFormat, source)
		}
	}

	switch importFormat {
	case "pgdump":
		var err error
		optionsClause := fmt.Sprintf("WITH max_row_size='%d'", importCtx.maxRowSize)
		if importCtx.skipForeignKeys {
			optionsClause = optionsClause + ", skip_foreign_keys"
		}
		if isTableImport {
			_, err = ex.ExecContext(ctx, fmt.Sprintf(`IMPORT TABLE %s FROM PGDUMP '%s' %s`,
				tableName, unescapedUserfileURL, optionsClause), nil)
		} else {
			_, err = ex.ExecContext(ctx, fmt.Sprintf(`IMPORT PGDUMP '%s' %s`, unescapedUserfileURL,
				optionsClause), nil)
		}
		if err != nil {
			return err
		}
	case "mysqldump":
		var err error
		var optionsClause string
		if importCtx.skipForeignKeys {
			optionsClause = " WITH skip_foreign_keys"
		}
		if isTableImport {
			_, err = ex.ExecContext(ctx, fmt.Sprintf(`IMPORT TABLE %s FROM MYSQLDUMP '%s'%s`,
				tableName, unescapedUserfileURL, optionsClause), nil)
		} else {
			_, err = ex.ExecContext(ctx, fmt.Sprintf(`IMPORT MYSQLDUMP '%s'%s`, unescapedUserfileURL,
				optionsClause), nil)
		}
		if err != nil {
			return err
		}
	default:
		return errors.New("unsupported import format")
	}

	importCompletedMesssage()
	return nil
}

var importCmds = []*cobra.Command{
	importDumpTableCmd,
	importDumpFileCmd,
}

var importCmd = &cobra.Command{
	Use:   "import [command]",
	Short: "import a db or table from a local PGDUMP or MYSQLDUMP file",
	Long:  "import a db or table from a local PGDUMP or MYSQLDUMP file",
	RunE:  usageAndErr,
}

func init() {
	importCmd.AddCommand(importCmds...)
}
