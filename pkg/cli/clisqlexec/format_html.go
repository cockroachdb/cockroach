// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlexec

import (
	"fmt"
	"html"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util"
)

type htmlReporter struct {
	nCols int

	escape   bool
	rowStats bool
}

func (p *htmlReporter) describe(w io.Writer, cols []string) error {
	p.nCols = len(cols)
	fmt.Fprint(w, "<table>\n<thead><tr>")
	if p.rowStats {
		fmt.Fprint(w, "<th>row</th>")
	}
	for _, col := range cols {
		if p.escape {
			col = html.EscapeString(col)
		}
		fmt.Fprintf(w, "<th>%s</th>", strings.Replace(col, "\n", "<br/>", -1))
	}
	fmt.Fprintln(w, "</tr></thead>")
	return nil
}

func (p *htmlReporter) beforeFirstRow(w io.Writer, _ RowStrIter) error {
	fmt.Fprintln(w, "<tbody>")
	return nil
}

func (p *htmlReporter) iter(w, _ io.Writer, rowIdx int, row []string) error {
	fmt.Fprint(w, "<tr>")
	if p.rowStats {
		fmt.Fprintf(w, "<td>%d</td>", rowIdx+1)
	}
	for _, r := range row {
		if p.escape {
			r = html.EscapeString(r)
		}
		fmt.Fprintf(w, "<td>%s</td>", strings.Replace(r, "\n", "<br/>", -1))
	}
	fmt.Fprintln(w, "</tr>")
	return nil
}

func (p *htmlReporter) doneNoRows(w io.Writer) error {
	fmt.Fprintln(w, "</table>")
	return nil
}

func (p *htmlReporter) doneRows(w io.Writer, nRows int) error {
	fmt.Fprintln(w, "</tbody>")
	if p.rowStats {
		fmt.Fprintf(w, "<tfoot><tr><td colspan=%d>%d row%s</td></tr></tfoot>",
			p.nCols+1, nRows, util.Pluralize(int64(nRows)))
	}
	fmt.Fprintln(w, "</table>")
	return nil
}
