// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlexec

import (
	"database/sql/driver"
	"io"

	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
)

func getAllRowStrings(
	rows clisqlclient.Rows, colTypes []string, showMoreChars bool,
) ([][]string, error) {
	var allRows [][]string

	for {
		rowStrings, err := getNextRowStrings(rows, colTypes, showMoreChars)
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

func getNextRowStrings(
	rows clisqlclient.Rows, colTypes []string, showMoreChars bool,
) ([]string, error) {
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

	rowStrings := make([]string, len(cols))
	for i, v := range vals {
		rowStrings[i] = FormatVal(v, colTypes[i], showMoreChars, showMoreChars)
	}
	return rowStrings, nil
}
