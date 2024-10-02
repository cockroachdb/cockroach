// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlexec

import (
	"database/sql/driver"
	"io"

	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
)

func getAllRowStrings(rows clisqlclient.Rows, showMoreChars bool) ([][]string, error) {
	var allRows [][]string

	for {
		rowStrings, err := getNextRowStrings(rows, showMoreChars)
		if err != nil {
			return nil, err
		}
		if rowStrings == nil {
			break
		}
		allRows = append(allRows, rowStrings)
	}

	return allRows, nil
}

func getNextRowStrings(rows clisqlclient.Rows, showMoreChars bool) ([]string, error) {
	cols := rows.Columns()
	var vals []driver.Value
	if len(cols) > 0 {
		vals = make([]driver.Value, len(cols))
	}

	err := rows.Next(vals)
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// These are expected to already be formatted strings, but if not, try to
	// print them anyway.
	rowStrings := make([]string, len(cols))
	for i, v := range vals {
		rowStrings[i] = FormatVal(v, showMoreChars, showMoreChars)
	}
	return rowStrings, nil
}
