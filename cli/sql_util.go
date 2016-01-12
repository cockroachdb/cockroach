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
// permissions and limitations under the License.
//
// Author: Marc berhault (marc@cockroachlabs.com)

package cli

import (
	"database/sql"
	"fmt"
	"io"

	// Import postgres driver.
	_ "github.com/lib/pq"

	"github.com/cockroachdb/cockroach/security"
	"github.com/olekukonko/tablewriter"
)

func makeSQLClient() (*sql.DB, string) {
	// Use the sql administrator by default (root user).
	if len(connURL) == 0 {
		sslOptions := ""
		if context.Insecure {
			sslOptions = "sslmode=disable"
		} else {
			sslOptions = fmt.Sprintf("sslmode=verify-full&sslcert=%s&sslkey=%s&sslrootcert=%s",
				security.ClientCertPath(context.Certs, connUser),
				security.ClientKeyPath(context.Certs, connUser),
				security.CACertPath(context.Certs))
		}
		connURL = fmt.Sprintf("postgresql://%s@%s:%s/%s?%s",
			connUser, connHost, connPort, connDBName, sslOptions)
	}
	db, err := sql.Open("postgres", connURL)
	if err != nil {
		panicf("failed to initialize SQL client: %s", err)
	}
	return db, connURL
}

// fmtMap is a mapping from column name to a function that takes the raw input,
// and outputs the string to be displayed.
type fmtMap map[string]func(interface{}) string

// runQuery takes a 'query' with optional 'parameters'.
// It runs the sql query and returns a list of columns names and a list of rows.
func runQuery(db *sql.DB, query string, parameters ...interface{}) (
	[]string, [][]string, error) {
	return runQueryWithFormat(db, nil, query, parameters...)
}

// runQuery takes a 'query' with optional 'parameters'.
// It runs the sql query and returns a list of columns names and a list of rows.
// If 'format' is not nil, the values with column name
// found in the map are run through the corresponding callback.
func runQueryWithFormat(db *sql.DB, format fmtMap, query string, parameters ...interface{}) (
	[]string, [][]string, error) {
	rows, err := db.Query(query, parameters...)
	if err != nil {
		return nil, nil, fmt.Errorf("query error: %s", err)
	}

	defer rows.Close()
	return sqlRowsToStrings(rows, format)
}

// runPrettyQueryWithFormat takes a 'query' with optional 'parameters'.
// It runs the sql query and writes pretty output to 'w'.
func runPrettyQuery(db *sql.DB, w io.Writer, query string, parameters ...interface{}) error {
	return runPrettyQueryWithFormat(db, w, nil, query, parameters...)
}

// runPrettyQueryWithFormat takes a 'query' with optional 'parameters'.
// It runs the sql query and writes pretty output to 'w'.
// If 'format' is not nil, the values with column name
// found in the map are run through the corresponding callback.
func runPrettyQueryWithFormat(db *sql.DB, w io.Writer, format fmtMap, query string, parameters ...interface{}) error {
	cols, allRows, err := runQueryWithFormat(db, format, query, parameters...)
	if err != nil {
		return err
	}
	return printQueryOutput(w, cols, allRows)
}

// sqlRowsToStrings turns 'rows' into a list of rows, each of which
// is a  list of column values.
// 'rows' should be closed by the caller.
// If 'format' is not nil, the values with column name
// found in the map are run through the corresponding callback.
// It returns the header row followed by all data rows.
// If both the header row and list of rows are empty, it means no row
// information was returned (eg: statement was not a query).
func sqlRowsToStrings(rows *sql.Rows, format fmtMap) ([]string, [][]string, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, fmt.Errorf("rows.Columns() error: %s", err)
	}

	if len(cols) == 0 {
		return nil, nil, nil
	}

	vals := make([]interface{}, len(cols))
	for i := range vals {
		vals[i] = new(interface{})
	}

	allRows := [][]string{}
	for rows.Next() {
		rowStrings := make([]string, len(cols))
		if err := rows.Scan(vals...); err != nil {
			return nil, nil, fmt.Errorf("scan error: %s", err)
		}
		for i, v := range vals {
			if f, ok := format[cols[i]]; ok {
				rowStrings[i] = f(*v.(*interface{}))
			} else {
				rowStrings[i] = formatVal(*v.(*interface{}))
			}
		}
		allRows = append(allRows, rowStrings)
	}

	return cols, allRows, nil
}

// printQueryOutput takes a list of column names and a list of row contents
// writes a pretty table to 'w', or "OK" if empty.
func printQueryOutput(w io.Writer, cols []string, allRows [][]string) error {
	if len(cols) == 0 {
		// This operation did not return rows, just show success.
		fmt.Fprintln(w, "OK")
		return nil
	}

	// Initialize tablewriter and set column names as the header row.
	table := tablewriter.NewWriter(w)
	table.SetAutoFormatHeaders(false)
	table.SetAutoWrapText(false)
	table.SetHeader(cols)

	for _, row := range allRows {
		if err := table.Append(row); err != nil {
			return err
		}
	}

	table.Render()
	return nil
}

func formatVal(val interface{}) string {
	switch t := val.(type) {
	case nil:
		return "NULL"
	case []byte:
		// Ensure that binary protobufs print escaped.
		return fmt.Sprintf("%q", t)
	}
	return fmt.Sprint(val)
}
