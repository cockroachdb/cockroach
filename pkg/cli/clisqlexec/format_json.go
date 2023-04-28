// Copyright 2023 The Cockroach Authors.
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
	"encoding/json"
	"io"
)

type ndjsonReporter struct {
	cols []string
}

func (n *ndjsonReporter) describe(w io.Writer, cols []string) error {
	n.cols = cols
	return nil
}

func (n *ndjsonReporter) beforeFirstRow(w io.Writer, allRows RowStrIter) error {
	return nil
}

func (n *ndjsonReporter) iter(w, ew io.Writer, rowIdx int, row []string) error {
	retMap := make(map[string]string, len(row))
	for i := range row {
		retMap[n.cols[i]] = row[i]
	}
	out, err := json.Marshal(retMap)
	if err != nil {
		return err
	}
	if _, err := w.Write(out); err != nil {
		return err
	}
	if _, err := w.Write(newLineChar); err != nil {
		return err
	}
	return nil
}

var newLineChar = []byte("\n")

func (n *ndjsonReporter) doneRows(w io.Writer, seenRows int) error {
	return nil
}

func (n *ndjsonReporter) doneNoRows(w io.Writer) error {
	return nil
}
