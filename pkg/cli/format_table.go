// Copyright 2017 The Cockroach Authors.
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

package cli

import (
	"encoding/csv"
	"fmt"
	"html"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/olekukonko/tablewriter"
	"github.com/pkg/errors"
)

// rowStrIter is an iterator interface for the printQueryOutput function. It is
// used so that results can be streamed to the row formatters as they arrive
// to the CLI.
type rowStrIter interface {
	Next() (row []string, err error)
	ToSlice() (allRows [][]string, err error)
}

// rowSliceIter is an implementation of the rowStrIter interface and it is used
// to wrap a slice of rows that have already been completely buffered into
// memory.
type rowSliceIter struct {
	allRows [][]string
	index   int
}

func (iter *rowSliceIter) Next() (row []string, err error) {
	if iter.index >= len(iter.allRows) {
		return nil, io.EOF
	}
	row = iter.allRows[iter.index]
	iter.index = iter.index + 1
	return row, nil
}

func (iter *rowSliceIter) ToSlice() ([][]string, error) {
	return iter.allRows, nil
}

// newRowSliceIter is an implementation of the rowStrIter interface and it is
// used when the rows have not been buffered into memory yet and we want to
// stream them to the row formatters as they arrive over the network.
func newRowSliceIter(allRows [][]string) *rowSliceIter {
	return &rowSliceIter{
		allRows: allRows,
		index:   0,
	}
}

type rowIter struct {
	rows          *sqlRows
	showMoreChars bool
}

func (iter *rowIter) Next() (row []string, err error) {
	nextRowString, err := getNextRowStrings(iter.rows, iter.showMoreChars)
	if err != nil {
		return nil, err
	}
	if nextRowString == nil {
		return nil, io.EOF
	}
	return nextRowString, nil
}

func (iter *rowIter) ToSlice() ([][]string, error) {
	return getAllRowStrings(iter.rows, iter.showMoreChars)
}

func newRowIter(rows *sqlRows, showMoreChars bool) *rowIter {
	return &rowIter{
		rows:          rows,
		showMoreChars: showMoreChars,
	}
}

// rowReporter is used to render result sets.
// - describe is called once in any case with the result column set.
// - beforeFirstRow is called once upon the first row encountered.
// - iter is called for every row, including the first (called after beforeFirstRowFn).
// - doneRows is called once after the last row encountered (in case of no error).
//   This can also be called when there were no rows, if the rowsAffectedHook
//   passed to render() returns false.
// - doneNoRows is called once when there were no rows and the rowsAffectedHook
//   returns true.
type rowReporter interface {
	describe(w io.Writer, cols []string) error
	beforeFirstRow(w io.Writer) error
	iter(w io.Writer, rowIdx int, row []string) error
	doneRows(w io.Writer, seenRows int) error
	doneNoRows(w io.Writer) error
}

// render iterates using the rowReporter object.
// The caller can pass a noRowsHook helper that will be called in the
// case there were no result rows. The helper is guaranteed to be called
// after the iteration through iter.Next(). Used in runQueryAndFormatResults.
func render(
	r rowReporter, w io.Writer, cols []string, iter rowStrIter, noRowsHook func() (bool, error),
) error {
	if err := r.describe(w, cols); err != nil {
		return err
	}
	nRows := 0
	for {
		// Get a next row.
		row, err := iter.Next()
		if err == io.EOF {
			// No more rows.
			break
		}
		if err != nil {
			// Error: don't call doneFn.
			return err
		}
		if nRows == 0 {
			// First row? Report.
			if err := r.beforeFirstRow(w); err != nil {
				return err
			}
		}

		// Report every row including the first.
		if err := r.iter(w, nRows, row); err != nil {
			return err
		}

		nRows++
	}

	if nRows == 0 && noRowsHook != nil {
		handled, err := noRowsHook()
		if err != nil {
			return err
		}
		if handled {
			return r.doneNoRows(w)
		}
	}
	return r.doneRows(w, nRows)
}

type prettyReporter struct {
	cols  []string
	table *tablewriter.Table
}

func (p *prettyReporter) describe(w io.Writer, cols []string) error {
	p.cols = cols
	if len(cols) > 0 {
		// Initialize tablewriter and set column names as the header row.
		p.table = tablewriter.NewWriter(w)
		p.table.SetAutoFormatHeaders(false)
		p.table.SetAutoWrapText(false)
		p.table.SetHeader(p.cols)
	}
	return nil
}

func (p *prettyReporter) beforeFirstRow(w io.Writer) error { return nil }

func (p *prettyReporter) iter(_ io.Writer, _ int, row []string) error {
	if p.table == nil {
		return nil
	}

	for i, r := range row {
		row[i] = expandTabsAndNewLines(r)
	}
	p.table.Append(row)
	return nil
}

func (p *prettyReporter) doneRows(w io.Writer, seenRows int) error {
	if p.table != nil {
		p.table.Render()
	} else {
		// A simple delimiter, like in psql.
		fmt.Fprintln(w, "--")
	}

	fmt.Fprintf(w, "(%d row%s)\n", seenRows, util.Pluralize(int64(seenRows)))
	return nil
}

func (p *prettyReporter) doneNoRows(_ io.Writer) error { return nil }

type csvReporter struct {
	csvWriter *csv.Writer
}

func (p *csvReporter) describe(w io.Writer, cols []string) error {
	p.csvWriter = csv.NewWriter(w)
	if cliCtx.tableDisplayFormat == tableDisplayTSV {
		p.csvWriter.Comma = '\t'
	}
	if len(cols) == 0 {
		_ = p.csvWriter.Write([]string{"# no columns"})
	} else {
		_ = p.csvWriter.Write(cols)
	}
	return nil
}

func (p *csvReporter) iter(_ io.Writer, _ int, row []string) error {
	if len(row) == 0 {
		_ = p.csvWriter.Write([]string{"# empty"})
	} else {
		_ = p.csvWriter.Write(row)
	}
	return nil
}

func (p *csvReporter) beforeFirstRow(_ io.Writer) error { return nil }
func (p *csvReporter) doneNoRows(_ io.Writer) error     { return nil }

func (p *csvReporter) doneRows(w io.Writer, seenRows int) error {
	p.csvWriter.Flush()
	fmt.Fprintf(w, "# %d row%s\n", seenRows, util.Pluralize(int64(seenRows)))
	return nil
}

type rawReporter struct{}

func (p *rawReporter) describe(w io.Writer, cols []string) error {
	fmt.Fprintf(w, "# %d column%s\n", len(cols),
		util.Pluralize(int64(len(cols))))
	return nil
}

func (p *rawReporter) iter(w io.Writer, rowIdx int, row []string) error {
	fmt.Fprintf(w, "# row %d\n", rowIdx+1)
	for _, r := range row {
		fmt.Fprintf(w, "## %d\n%s\n", len(r), r)
	}
	return nil
}

func (p *rawReporter) beforeFirstRow(_ io.Writer) error { return nil }
func (p *rawReporter) doneNoRows(_ io.Writer) error     { return nil }

func (p *rawReporter) doneRows(w io.Writer, nRows int) error {
	fmt.Fprintf(w, "# %d row%s\n", nRows, util.Pluralize(int64(nRows)))
	return nil
}

type htmlReporter struct {
	nCols int
}

func (p *htmlReporter) describe(w io.Writer, cols []string) error {
	p.nCols = len(cols)
	fmt.Fprint(w, "<table>\n<thead><tr>")
	fmt.Fprint(w, "<th>row</th>")
	for _, col := range cols {
		fmt.Fprintf(w, "<th>%s</th>", html.EscapeString(col))
	}
	fmt.Fprintln(w, "</tr></head>")
	return nil
}

func (p *htmlReporter) beforeFirstRow(w io.Writer) error {
	fmt.Fprintln(w, "<tbody>")
	return nil
}

func (p *htmlReporter) iter(w io.Writer, rowIdx int, row []string) error {
	fmt.Fprintf(w, "<tr><td>%d</td>", rowIdx+1)
	for _, r := range row {
		fmt.Fprintf(w, "<td>%s</td>", strings.Replace(html.EscapeString(r), "\n", "<br/>", -1))
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
	fmt.Fprintf(w, "<tfoot><tr><td colspan=%d>%d row%s</td></tr></tfoot></table>\n",
		p.nCols+1, nRows, util.Pluralize(int64(nRows)))
	return nil
}

type recordReporter struct {
	cols        []string
	maxColWidth int
}

func (p *recordReporter) describe(w io.Writer, cols []string) error {
	p.cols = cols
	for _, col := range cols {
		colLen := utf8.RuneCountInString(col)
		if colLen > p.maxColWidth {
			p.maxColWidth = colLen
		}
	}
	return nil
}

func (p *recordReporter) iter(w io.Writer, rowIdx int, row []string) error {
	if len(p.cols) == 0 {
		// No record details; a summary will be printed at the end.
		return nil
	}
	fmt.Fprintf(w, "-[ RECORD %d ]\n", rowIdx+1)
	for j, r := range row {
		lines := strings.Split(r, "\n")
		for l, line := range lines {
			colLabel := p.cols[j]
			if l > 0 {
				colLabel = ""
			}
			// Note: special characters, including a vertical bar, in
			// the colLabel are not escaped here. This is in accordance
			// with the same behavior in PostgreSQL.
			fmt.Fprintf(w, "%-*s | %s\n", p.maxColWidth, colLabel, line)
		}
	}
	return nil
}

func (p *recordReporter) doneRows(w io.Writer, seenRows int) error {
	if len(p.cols) == 0 {
		fmt.Fprintf(w, "(%d row%s)\n", seenRows, util.Pluralize(int64(seenRows)))
	}
	return nil
}

func (p *recordReporter) beforeFirstRow(_ io.Writer) error { return nil }
func (p *recordReporter) doneNoRows(_ io.Writer) error     { return nil }

type sqlReporter struct {
	noColumns bool
}

func (p *sqlReporter) describe(w io.Writer, cols []string) error {
	fmt.Fprint(w, "CREATE TABLE results (\n")
	for i, col := range cols {
		s := parser.Name(col)
		fmt.Fprintf(w, "  %s STRING", s.String())
		if i < len(cols)-1 {
			fmt.Fprint(w, ",")
		}
		fmt.Fprint(w, "\n")
	}
	fmt.Fprint(w, ");\n\n")
	p.noColumns = (len(cols) == 0)
	return nil
}

func (p *sqlReporter) iter(w io.Writer, _ int, row []string) error {
	if p.noColumns {
		fmt.Fprintln(w, "INSERT INTO results(rowid) VALUES (DEFAULT);")
		return nil
	}

	fmt.Fprint(w, "INSERT INTO results VALUES (")
	for i, r := range row {
		s := parser.DString(r)
		fmt.Fprintf(w, "%s", s.String())
		if i < len(row)-1 {
			fmt.Fprint(w, ", ")
		}
	}
	fmt.Fprint(w, ");\n")
	return nil
}

func (p *sqlReporter) beforeFirstRow(_ io.Writer) error  { return nil }
func (p *sqlReporter) doneNoRows(_ io.Writer) error      { return nil }
func (p *sqlReporter) doneRows(_ io.Writer, _ int) error { return nil }

func makeReporter() (rowReporter, error) {
	switch cliCtx.tableDisplayFormat {
	case tableDisplayPretty:
		return &prettyReporter{}, nil

	case tableDisplayTSV:
		fallthrough
	case tableDisplayCSV:
		return &csvReporter{}, nil

	case tableDisplayRaw:
		return &rawReporter{}, nil

	case tableDisplayHTML:
		return &htmlReporter{}, nil

	case tableDisplayRecords:
		return &recordReporter{}, nil

	case tableDisplaySQL:
		return &sqlReporter{}, nil

	default:
		return nil, errors.Errorf("unhandled display format: %d", cliCtx.tableDisplayFormat)
	}
}

// printQueryOutput takes a list of column names and a list of row
// contents writes a formatted table to 'w'.
func printQueryOutput(w io.Writer, cols []string, allRows rowStrIter) error {
	reporter, err := makeReporter()
	if err != nil {
		return err
	}
	return render(reporter, w, cols, allRows, nil)
}
