// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"encoding/csv"
	"os"

	"github.com/cockroachdb/errors"
)

// appendCSVRow appends a row to a CSV file, writing the header first if the
// file does not yet exist.
func appendCSVRow(filename string, header []string, row []string) error {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrap(err, "opening CSV file")
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return errors.Wrap(err, "stat CSV file")
	}

	w := csv.NewWriter(f)

	if info.Size() == 0 {
		if err := w.Write(header); err != nil {
			return errors.Wrap(err, "writing CSV header")
		}
	}

	if err := w.Write(row); err != nil {
		return errors.Wrap(err, "writing CSV row")
	}

	w.Flush()
	return errors.Wrap(w.Error(), "flushing CSV writer")
}
