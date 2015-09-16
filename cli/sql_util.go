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
	"time"

	"github.com/olekukonko/tablewriter"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
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

// fmtMap is a mapping from column name to a function that takes the raw input,
// and outputs the string to be displayed.
type fmtMap map[string]func(interface{}) string

// runQuery takes a 'query' with optional 'parameters'.
// It runs the sql query and writes pretty output to osStdout.
func runQuery(db *sql.DB, query string, parameters ...interface{}) error {
	rows, err := db.Query(query, parameters...)
	if err != nil {
		return fmt.Errorf("query error: %s", err)
	}

	defer rows.Close()
	return printQueryOutput(rows, nil)
}

// runQueryWithFormat takes a 'query' with optional 'parameters'.
// It runs the sql query and writes pretty output to osStdout.
func runQueryWithFormat(db *sql.DB, format fmtMap, query string, parameters ...interface{}) error {
	rows, err := db.Query(query, parameters...)
	if err != nil {
		return fmt.Errorf("query error: %s", err)
	}

	defer rows.Close()
	return printQueryOutput(rows, format)
}

// printQueryOutput takes a set of sql rows and writes a pretty table
// to osStdout, or "OK" if no rows are returned.
// 'rows' should be closed by the caller.
// If 'formatter' is not nil, the values with column name
// found in the map are run through the corresponding callback.
func printQueryOutput(rows *sql.Rows, format fmtMap) error {
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
	table.SetAutoWrapText(false)
	table.SetHeader(cols)

	// Stringify all data and append rows to tablewriter.
	vals := make([]interface{}, len(cols))
	for i := range vals {
		vals[i] = new(interface{})
	}
	rowStrings := make([]string, len(cols))
	for rows.Next() {
		if err := rows.Scan(vals...); err != nil {
			return fmt.Errorf("scan error: %s", err)
		}
		for i, v := range vals {
			if f, ok := format[cols[i]]; ok {
				rowStrings[i] = f(*v.(*interface{}))
			} else {
				rowStrings[i] = formatVal(*v.(*interface{}))
			}
		}
		if err := table.Append(rowStrings); err != nil {
			return err
		}
	}

	table.Render()
	return nil
}

func formatVal(val interface{}) string {
	switch t := val.(type) {
	case time.Time:
		return t.Format(parser.TimestampWithOffsetZoneFormat)
	}
	// Note that this prints a Go-syntax representation of the value.
	// This is to ensure that binary protobufs print escaped.
	return fmt.Sprintf("%#v", val)
}
