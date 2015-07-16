// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc berhault (marc@cockroachlabs.com)

package cli

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	// Import cockroach driver.
	_ "github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/util"
	"github.com/spf13/cobra"
	"golang.org/x/crypto/ssh/terminal"
)

func makeSQLClient() *sql.DB {
	// TODO(pmattis): Initialize the user to something more
	// reasonable. Perhaps Context.Addr should be considered a URL.
	db, err := sql.Open("cockroach",
		fmt.Sprintf("%s://root@%s?certs=%s",
			Context.RequestScheme(),
			Context.Addr,
			Context.Certs))
	if err != nil {
		fmt.Fprintf(osStderr, "failed to initialize SQL client: %s\n", err)
		osExit(1)
	}
	return db
}

// sqlShellCmd opens a sql shell.
var sqlShellCmd = &cobra.Command{
	Use:   "sql [options]",
	Short: "open a sql shell",
	Long: `
Open a sql shell running against the cockroach database at --addr.
`,
	Run: runTerm,
}

// processOneLine takes a line from the terminal, runs it,
// and displays the result.
// TODO(marc): handle multi-line, this will require ';' terminated statements.
func processOneLine(db *sql.DB, line string, w io.Writer) (bool, error) {
	// Look for the first word in the statement and use it to decide what to do.
	words := strings.Split(line, " ")
	if len(words) == 0 {
		return false, nil
	}

	// This is a really terrible way of figuring out how to handle
	// the input.
	command := strings.ToUpper(words[0])
	if command == "EXIT" || command == "QUIT" {
		return true, nil
	}
	// Issues a query and examine returned Rows.
	rows, err := db.Query(line)
	if err != nil {
		return false, util.Errorf("query error: %s", err)
	}

	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return false, util.Errorf("rows.Columns() error: %s", err)
	}

	if len(cols) == 0 {
		// This operation did not return rows, just show success.
		fmt.Fprintf(w, "OK\n")
		return false, nil
	}

	// Format all rows using tabwriter.
	tw := new(tabwriter.Writer)
	tw.Init(w, 0, 8, 0, '\t', 0)
	fmt.Fprintf(tw, "%s\n", strings.Join(cols, "\t"))
	strs := make([]string, len(cols))
	vals := make([]interface{}, len(cols))
	for rows.Next() {
		for i := range vals {
			vals[i] = &strs[i]
		}
		if err := rows.Scan(vals...); err != nil {
			return false, util.Errorf("scan error: %s", err)
		}
		fmt.Fprintf(tw, "%s\n", strings.Join(strs, "\t"))
	}
	_ = tw.Flush()
	return false, nil
}

func runTerm(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Usage()
		return
	}

	db := makeSQLClient()

	readWriter := struct {
		io.Reader
		io.Writer
	}{
		Reader: os.Stdin,
		Writer: os.Stdout,
	}

	// We need to switch to raw mode. Unfortunately, this masks
	// signals-from-keyboard, meaning that ctrl-C cannot be caught.
	oldState, err := terminal.MakeRaw(0)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = terminal.Restore(0, oldState)
	}()

	term := terminal.NewTerminal(readWriter, "> ")
	for {
		line, err := term.ReadLine()
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "Input error: %s\n", err)
			}
			break
		}
		if len(line) == 0 {
			continue
		}

		shouldExit, err := processOneLine(db, line, term)
		if err != nil {
			fmt.Fprintf(term, "Error: %s\n", err)
		}
		if shouldExit {
			break
		}
	}
}
