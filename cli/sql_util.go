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

	"github.com/olekukonko/tablewriter"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/driver"
)

func makeSQLClient() *sql.DB {
	// Use the sql administrator by default (root user).
	// TODO(marc): allow passing on the commandline.
	db, err := sql.Open("cockroach",
		fmt.Sprintf("%s://%s@%s?certs=%s",
			context.HTTPRequestScheme(),
			security.RootUser,
			context.Addr,
			context.Certs))
	if err != nil {
		fmt.Fprintf(osStderr, "failed to initialize SQL client: %s\n", err)
		osExit(1)
	}
	return db
}

// runQuery takes a 'query' with optional 'parameters'.
// It runs the sql query and writes pretty output to osStdout.
func runQuery(db *sql.DB, query string, parameters ...interface{}) error {
	rows, err := db.Query(query, parameters...)
	if err != nil {
		return fmt.Errorf("query error: %s", err)
	}

	defer rows.Close()
	return printQueryOutput(rows)
}

// printQueryOutput takes a set of sql rows and writes a pretty table
// to osStdout, or "OK" if no rows are returned.
// 'rows' should be closed by the caller.
func printQueryOutput(rows *sql.Rows) error {
	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("rows.Columns() error: %s", err)
	}

	if len(cols) == 0 {
		// This operation did not return rows, just show success.
		fmt.Fprintln(osStdout, "OK")
		return nil
	}

	// Initialize tablewriter and set column names as the header row.
	table := tablewriter.NewWriter(osStdout)
	table.SetAutoFormatHeaders(false)
	table.SetHeader(cols)

	// Stringify all data and append rows to tablewriter.
	vals := make([]interface{}, len(cols))
	rowStrings := make([]string, len(cols))
	for rows.Next() {
		for i := range vals {
			vals[i] = new(driver.NullString)
		}
		if err := rows.Scan(vals...); err != nil {
			return fmt.Errorf("scan error: %s", err)
		}
		for i, v := range vals {
			rowStrings[i] = fmt.Sprint(v)
		}
		if err := table.Append(rowStrings); err != nil {
			return err
		}
	}

	table.Render()
	return nil
}
