// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlutils

import (
	gosql "database/sql"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v4"
)

// RowsToDataDrivenOutput converts a gosql.Rows object into an appropriate
// string for usage in data driven tests.
func RowsToDataDrivenOutput(rows *gosql.Rows) (string, error) {
	// Find out how many output columns there are.
	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	// Allocate a buffer of *interface{} to write results into.
	elemsI := make([]interface{}, len(cols))
	for i := range elemsI {
		elemsI[i] = new(interface{})
	}
	elems := make([]string, len(cols))

	// Build string output of the row data.
	var output strings.Builder
	for rows.Next() {
		if err := rows.Scan(elemsI...); err != nil {
			return "", err
		}
		for i, elem := range elemsI {
			val := *(elem.(*interface{}))
			switch t := val.(type) {
			case []byte:
				// The postgres wire protocol does not distinguish between
				// strings and byte arrays, but our tests do. In order to do
				// The Right Thing™, we replace byte arrays which are valid
				// UTF-8 with strings. This allows byte arrays which are not
				// valid UTF-8 to print as a list of bytes (e.g. `[124 107]`)
				// while printing valid strings naturally.
				if str := string(t); utf8.ValidString(str) {
					elems[i] = str
				}
			default:
				elems[i] = fmt.Sprintf("%v", val)
			}
		}
		output.WriteString(strings.Join(elems, " "))
		output.WriteString("\n")
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return output.String(), nil
}

// PGXRowsToDataDrivenOutput converts a pgx.Rows object into an appropriate
// string for usage in data driven tests.
func PGXRowsToDataDrivenOutput(rows pgx.Rows) (string, error) {
	// Find out how many output columns there are.
	cols := rows.FieldDescriptions()
	// Allocate a buffer of *interface{} to write results into.
	elemsI := make([]interface{}, len(cols))
	for i := range elemsI {
		elemsI[i] = new(interface{})
	}
	elems := make([]string, len(cols))

	// Build string output of the row data.
	var output strings.Builder
	for rows.Next() {
		if err := rows.Scan(elemsI...); err != nil {
			return "", err
		}
		for i, elem := range elemsI {
			val := *(elem.(*interface{}))
			switch t := val.(type) {
			case []byte:
				// The postgres wire protocol does not distinguish between
				// strings and byte arrays, but our tests do. In order to do
				// The Right Thing™, we replace byte arrays which are valid
				// UTF-8 with strings. This allows byte arrays which are not
				// valid UTF-8 to print as a list of bytes (e.g. `[124 107]`)
				// while printing valid strings naturally.
				if str := string(t); utf8.ValidString(str) {
					elems[i] = str
				}
			default:
				elems[i] = fmt.Sprintf("%v", val)
			}
		}
		output.WriteString(strings.Join(elems, " "))
		output.WriteString("\n")
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return output.String(), nil
}
