// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlexec

import (
	"bytes"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/olekukonko/tablewriter"
)

type asciiTableReporter struct {
	rows  int
	table *tablewriter.Table
	buf   bytes.Buffer
	w     *tabwriter.Writer

	tableBorderMode int
}

func newASCIITableReporter(tableBorderMode int) *asciiTableReporter {
	n := &asciiTableReporter{tableBorderMode: tableBorderMode}
	// 4-wide columns, 1 character minimum width.
	n.w = tabwriter.NewWriter(&n.buf, 4, 0, 1, ' ', 0)
	return n
}

func (p *asciiTableReporter) describe(w io.Writer, cols []string) error {
	if len(cols) > 0 {
		// Ensure that tabs are converted to spaces and newlines are
		// doubled so they can be recognized in the output.
		expandedCols := make([]string, len(cols))
		for i, c := range cols {
			p.buf.Reset()
			fmt.Fprint(p.w, c)
			_ = p.w.Flush()
			expandedCols[i] = p.buf.String()
		}

		// Initialize tablewriter and set column names as the header row.
		p.table = tablewriter.NewWriter(w)
		p.table.SetAutoFormatHeaders(false)
		p.table.SetAutoWrapText(false)
		var outsideBorders, insideLines bool
		// The following table border modes are taken from psql.
		// https://www.postgresql.org/docs/12/app-psql.html
		switch p.tableBorderMode {
		case 0:
			outsideBorders, insideLines = false, false
		case 1:
			outsideBorders, insideLines = false, true
		case 2:
			outsideBorders, insideLines = true, false
		case 3:
			outsideBorders, insideLines = true, true
		}
		p.table.SetBorder(outsideBorders)
		p.table.SetRowLine(insideLines)
		p.table.SetReflowDuringAutoWrap(false)
		p.table.SetHeader(expandedCols)
		p.table.SetTrimWhiteSpaceAtEOL(true)
		// This width is sufficient to show a "standard text line width"
		// on the screen when viewed as a single column on a 80-wide terminal.
		//
		// It's also wide enough for the output of SHOW CREATE on
		// moderately long column definitions (e.g. including FK
		// constraints).
		p.table.SetColWidth(72)
	}
	return nil
}

func (p *asciiTableReporter) beforeFirstRow(w io.Writer, iter RowStrIter) error {
	if p.table == nil {
		return nil
	}

	p.table.SetColumnAlignment(iter.Align())
	return nil
}

// asciiTableWarnRows is the number of rows at which a warning is
// printed during buffering in memory.
const asciiTableWarnRows = 10000

func (p *asciiTableReporter) iter(_, ew io.Writer, _ int, row []string) error {
	if p.table == nil {
		return nil
	}

	if p.rows == asciiTableWarnRows {
		fmt.Fprintf(ew,
			"warning: buffering more than %d result rows in client "+
				"- RAM usage growing, consider another formatter instead\n",
			asciiTableWarnRows)
	}

	for i, r := range row {
		p.buf.Reset()
		fmt.Fprint(p.w, r)
		_ = p.w.Flush()
		row[i] = p.buf.String()
	}
	p.table.Append(row)
	p.rows++
	return nil
}

func (p *asciiTableReporter) doneRows(w io.Writer, seenRows int) error {
	if p.table != nil {
		p.table.Render()
		p.table = nil
	} else {
		// A simple delimiter, like in psql.
		fmt.Fprintln(w, "--")
	}

	fmt.Fprintf(w, "(%d row%s)\n", seenRows, util.Pluralize(int64(seenRows)))
	return nil
}

func (p *asciiTableReporter) doneNoRows(_ io.Writer) error {
	p.table = nil
	return nil
}
