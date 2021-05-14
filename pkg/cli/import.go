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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
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

// importCLITestingKnobs are set when the CLI import command is run from a unit
// test. Since import is a CCL feature there is currently no infrastructure to
// test it without replicating a lot of the test utility methods found in
// pkg/cli, in pkg/cliccl.
// Considering IMPORT is a well tested feature, the testing knobs allow us to
// bypass the run of an actual IMPORT but test all CLI logic upto the point
// where we run the IMPORT query.
type importCLITestingKnobs struct {
	// returnQuery when set to true, ensures that the fully constructed IMPORT SQL
	// query is printed to stdout, instead of being run.
	returnQuery      bool
	pauseAfterUpload chan struct{}
	uploadComplete   chan struct{}
}

var importCLIKnobs importCLITestingKnobs

type importMode int

const (
	multiTable importMode = iota
	singleTable
)

func setImportCLITestingKnobs() (importCLITestingKnobs, func()) {
	importCLIKnobs = importCLITestingKnobs{
		pauseAfterUpload: make(chan struct{}, 1),
		uploadComplete:   make(chan struct{}, 1),
		returnQuery:      true,
	}

	return importCLIKnobs, func() {
		importCLIKnobs = importCLITestingKnobs{}
	}
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
	return runImport(ctx, conn, importFormat, source, tableName, singleTable)
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
	return runImport(ctx, conn, importFormat, source, "", multiTable)
}

func runImport(
	ctx context.Context, conn *sqlConn, importFormat, source, tableName string, mode importMode,
) error {
	if err := conn.ensureConn(); err != nil {
		return err
	}

	connURL, err := url.Parse(conn.url)
	if err != nil {
		return err
	}

	username, err := security.MakeSQLUsernameFromUserInput(connURL.User.Username(), security.UsernameCreation)
	if err != nil {
		return err
	}

	// Resolve the userfile destination to upload the dump file to.
	userfileDestinationURI := constructUserfileDestinationURI(source, "", username)
	unescapedUserfileURL, err := url.PathUnescape(userfileDestinationURI)
	if err != nil {
		return err
	}

	defer func() {
		// Delete the file chunks which were written as part of this IMPORT.
		_, _ = deleteUserFile(ctx, conn, unescapedUserfileURL)
	}()

	_, err = uploadUserFile(ctx, conn, source, userfileDestinationURI)
	if err != nil {
		return errors.Wrap(err, "failed to upload file to userfile before importing")
	}

	if importCLIKnobs.uploadComplete != nil {
		importCLIKnobs.uploadComplete <- struct{}{}
	}

	if importCLIKnobs.pauseAfterUpload != nil {
		<-importCLIKnobs.pauseAfterUpload
	}

	ex := conn.conn.(driver.ExecerContext)
	importCompletedMesssage := func() {
		switch mode {
		case singleTable:
			fmt.Printf("successfully imported table %s from %s file %s\n", tableName, importFormat,
				source)
		case multiTable:
			fmt.Printf("successfully imported %s file %s\n", importFormat, source)
		}
	}

	var importQuery string
	switch importFormat {
	case "pgdump":
		optionsClause := fmt.Sprintf("WITH max_row_size='%d'", importCtx.maxRowSize)
		if importCtx.skipForeignKeys {
			optionsClause = optionsClause + ", skip_foreign_keys"
		}
		if importCtx.rowLimit > 0 {
			optionsClause = fmt.Sprintf("%s, row_limit='%d'", optionsClause, importCtx.rowLimit)
		}
		if importCtx.ignoreUnsupported {
			optionsClause = fmt.Sprintf("%s, ignore_unsupported_statements", optionsClause)
		}
		if importCtx.ignoreUnsupportedLog != "" {
			optionsClause = fmt.Sprintf("%s, log_ignored_statements=%s", optionsClause,
				importCtx.ignoreUnsupportedLog)
		}
		switch mode {
		case singleTable:
			importQuery = fmt.Sprintf(`IMPORT TABLE %s FROM PGDUMP '%s' %s`, tableName,
				unescapedUserfileURL, optionsClause)
		case multiTable:
			importQuery = fmt.Sprintf(`IMPORT PGDUMP '%s' %s`, unescapedUserfileURL, optionsClause)
		}
	case "mysqldump":
		var optionsClause string
		if importCtx.skipForeignKeys {
			optionsClause = " WITH skip_foreign_keys"
		}
		if importCtx.rowLimit > 0 {
			optionsClause = fmt.Sprintf("%s, row_limit='%d'", optionsClause, importCtx.rowLimit)
		}
		switch mode {
		case singleTable:
			importQuery = fmt.Sprintf(`IMPORT TABLE %s FROM MYSQLDUMP '%s'%s`, tableName,
				unescapedUserfileURL, optionsClause)
		case multiTable:
			importQuery = fmt.Sprintf(`IMPORT MYSQLDUMP '%s'%s`, unescapedUserfileURL,
				optionsClause)
		}
	default:
		return errors.New("unsupported import format")
	}

	purl, err := pgurl.Parse(conn.url)
	if err != nil {
		return err
	}

	if importCLIKnobs.returnQuery {
		fmt.Print(importQuery + "\n")
		fmt.Print(purl.GetDatabase())
		return nil
	}

	_, err = ex.ExecContext(ctx, importQuery, nil)
	if err != nil {
		return err
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
