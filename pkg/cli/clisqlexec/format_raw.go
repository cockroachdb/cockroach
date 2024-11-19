// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlexec

import (
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/util"
)

type rawReporter struct{}

func (p *rawReporter) describe(w io.Writer, cols []string) error {
	fmt.Fprintf(w, "# %d column%s\n", len(cols),
		util.Pluralize(int64(len(cols))))
	return nil
}

func (p *rawReporter) iter(w, _ io.Writer, rowIdx int, row []string) error {
	fmt.Fprintf(w, "# row %d\n", rowIdx+1)
	for _, r := range row {
		fmt.Fprintf(w, "## %d\n%s\n", len(r), r)
	}
	return nil
}

func (p *rawReporter) beforeFirstRow(_ io.Writer, _ RowStrIter) error { return nil }
func (p *rawReporter) doneNoRows(_ io.Writer) error                   { return nil }

func (p *rawReporter) doneRows(w io.Writer, nRows int) error {
	fmt.Fprintf(w, "# %d row%s\n", nRows, util.Pluralize(int64(nRows)))
	return nil
}
