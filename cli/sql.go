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
	"time"

	// Import cockroach driver.
	_ "github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/util"
	"github.com/peterh/liner"
	"github.com/spf13/cobra"
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

type sqlValue struct {
	value string
}

func (s *sqlValue) Scan(value interface{}) error {
	switch v := value.(type) {
	case nil:
		s.value = "NULL"
	case string:
		s.value = v
	case []byte:
		s.value = string(v)
	case time.Time:
		s.value = v.String()
	case int64:
		s.value = fmt.Sprintf("%d", v)
	case float64:
		s.value = fmt.Sprintf("%f", v)
	default:
		s.value = fmt.Sprintf("%v", value)
	}
	return nil
}

// processOneLine takes a line from the terminal, runs it,
// and displays the result.
// TODO(marc): handle multi-line, this will require ';' terminated statements.
func processOneLine(db *sql.DB, line string) error {
	// Issues a query and examine returned Rows.
	rows, err := db.Query(line)
	if err != nil {
		return util.Errorf("query error: %s", err)
	}

	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return util.Errorf("rows.Columns() error: %s", err)
	}

	if len(cols) == 0 {
		// This operation did not return rows, just show success.
		fmt.Printf("OK\n")
		return nil
	}

	// Format all rows using tabwriter.
	tw := tabwriter.NewWriter(os.Stdout, 0, 8, 0, '\t', 0)
	fmt.Fprintf(tw, "%s\n", strings.Join(cols, "\t"))
	sqlVals := make([]sqlValue, len(cols))
	vals := make([]interface{}, len(cols))
	for rows.Next() {
		for i := range sqlVals {
			vals[i] = &sqlVals[i]
		}
		if err := rows.Scan(vals...); err != nil {
			return util.Errorf("scan error: %s", err)
		}
		for _, v := range sqlVals {
			fmt.Fprintf(tw, "%s\t", v.value)
		}
		fmt.Fprintf(tw, "\n")
	}
	_ = tw.Flush()
	return nil
}

func runTerm(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Usage()
		return
	}

	db := makeSQLClient()

	liner := liner.NewLiner()
	defer func() {
		_ = liner.Close()
	}()

	for {
		l, err := liner.Prompt("> ")
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "Input error: %s\n", err)
			}
			break
		}
		if len(l) == 0 {
			continue
		}
		liner.AppendHistory(l)

		if err := processOneLine(db, l); err != nil {
			fmt.Printf("Error: %s\n", err)
		}
	}
}
