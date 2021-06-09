// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bytes"
	"fmt"
	"html"
	"io"
	"reflect"
	"strings"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/olekukonko/tablewriter"
)

// rowStrIter is an iterator interface for the PrintQueryOutput function. It is
// used so that results can be streamed to the row formatters as they arrive
// to the CLI.
type rowStrIter interface {
	Next() (row []string, err error)
	ToSlice() (allRows [][]string, err error)
	Align() []int
}

// RowSliceIter is an implementation of the rowStrIter interface and it is used
// to wrap a slice of rows that have already been completely buffered into
// memory.
type RowSliceIter struct {
	allRows [][]string
	index   int
	align   []int
}

// Next returns next row of RowSliceIter.
func (iter *RowSliceIter) Next() (row []string, err error) {
	if iter.index >= len(iter.allRows) {
		return nil, io.EOF
	}
	row = iter.allRows[iter.index]
	iter.index = iter.index + 1
	return row, nil
}

// ToSlice returns all rows of RowSliceIter.
func (iter *RowSliceIter) ToSlice() ([][]string, error) {
	return iter.allRows, nil
}

// Align returns alignment setting of RowSliceIter.
func (iter *RowSliceIter) Align() []int {
	return iter.align
}

func convertAlign(align string) []int {
	result := make([]int, len(align))
	for i, v := range align {
		switch v {
		case 'l':
			result[i] = tablewriter.ALIGN_LEFT
		case 'r':
			result[i] = tablewriter.ALIGN_RIGHT
		case 'c':
			result[i] = tablewriter.ALIGN_CENTER
		default:
			result[i] = tablewriter.ALIGN_DEFAULT
		}
	}
	return result
}

// NewRowSliceIter is an implementation of the rowStrIter interface and it is
// used when the rows have not been buffered into memory yet and we want to
// stream them to the row formatters as they arrive over the network.
func NewRowSliceIter(allRows [][]string, align string) *RowSliceIter {
	return &RowSliceIter{
		allRows: allRows,
		index:   0,
		align:   convertAlign(align),
	}
}

type rowIter struct {
	rows          *sqlRows
	colTypes      []string
	showMoreChars bool
}

func (iter *rowIter) Next() (row []string, err error) {
	nextRowString, err := getNextRowStrings(iter.rows, iter.colTypes, iter.showMoreChars)
	if err != nil {
		return nil, err
	}
	if nextRowString == nil {
		return nil, io.EOF
	}
	return nextRowString, nil
}

func (iter *rowIter) ToSlice() ([][]string, error) {
	return getAllRowStrings(iter.rows, iter.colTypes, iter.showMoreChars)
}

func (iter *rowIter) Align() []int {
	cols := iter.rows.Columns()
	align := make([]int, len(cols))
	for i := range align {
		switch iter.rows.ColumnTypeScanType(i).Kind() {
		case reflect.String:
			align[i] = tablewriter.ALIGN_LEFT
		case reflect.Slice:
			align[i] = tablewriter.ALIGN_LEFT
		case reflect.Int64:
			align[i] = tablewriter.ALIGN_RIGHT
		case reflect.Float64:
			align[i] = tablewriter.ALIGN_RIGHT
		case reflect.Bool:
			align[i] = tablewriter.ALIGN_CENTER
		default:
			align[i] = tablewriter.ALIGN_DEFAULT
		}
	}
	return align
}

func newRowIter(rows *sqlRows, showMoreChars bool) *rowIter {
	return &rowIter{
		rows:          rows,
		colTypes:      rows.getColTypes(),
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
	beforeFirstRow(w io.Writer, allRows rowStrIter) error
	iter(w io.Writer, rowIdx int, row []string) error
	doneRows(w io.Writer, seenRows int) error
	doneNoRows(w io.Writer) error
}

// render iterates using the rowReporter object.
// The caller can pass a noRowsHook helper that will be called in the
// case there were no result rows and no error.
// This helper is guaranteed to be called
// after the iteration through iter.Next(). Used in runQueryAndFormatResults.
//
// The completedHook is called after the last row is received/passed to the
// rendered, but before the final rendering takes place. It is called
// regardless of whether an error occurred.
func render(
	r rowReporter,
	w io.Writer,
	cols []string,
	iter rowStrIter,
	completedHook func(),
	noRowsHook func() (bool, error),
) (err error) {
	described := false
	nRows := 0
	defer func() {
		// If the column headers are not printed yet, do it now.
		if !described {
			err = errors.WithSecondaryError(err, r.describe(w, cols))
		}

		// completedHook, if provided, is called unconditonally of error.
		if completedHook != nil {
			completedHook()
		}

		// We need to call doneNoRows/doneRows also unconditionally.
		var handled bool
		if nRows == 0 && noRowsHook != nil {
			handled, err = noRowsHook()
			if err != nil {
				return
			}
		}
		if handled {
			err = errors.WithSecondaryError(err, r.doneNoRows(w))
		} else {
			err = errors.WithSecondaryError(err, r.doneRows(w, nRows))
		}

		if err != nil && nRows > 0 {
			fmt.Fprintf(stderr, "(error encountered after some results were delivered)\n")
		}
	}()

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
			described = true
			if err = r.describe(w, cols); err != nil {
				return err
			}
			if err = r.beforeFirstRow(w, iter); err != nil {
				return err
			}
		}

		// Report every row including the first.
		if err = r.iter(w, nRows, row); err != nil {
			return err
		}

		nRows++
	}

	return nil
}

type asciiTableReporter struct {
	rows  int
	table *tablewriter.Table
	buf   bytes.Buffer
	w     *tabwriter.Writer
}

func newASCIITableReporter() *asciiTableReporter {
	n := &asciiTableReporter{}
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
		switch cliCtx.tableBorderMode {
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

func (p *asciiTableReporter) beforeFirstRow(w io.Writer, iter rowStrIter) error {
	if p.table == nil {
		return nil
	}

	p.table.SetColumnAlignment(iter.Align())
	return nil
}

// asciiTableWarnRows is the number of rows at which a warning is
// printed during buffering in memory.
const asciiTableWarnRows = 10000

func (p *asciiTableReporter) iter(_ io.Writer, _ int, row []string) error {
	if p.table == nil {
		return nil
	}

	if p.rows == asciiTableWarnRows {
		fmt.Fprintf(stderr,
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

type csvReporter struct {
	mu struct {
		syncutil.Mutex
		csvWriter *csv.Writer
	}
	stop chan struct{}
}

// csvFlushInterval is the maximum time between flushes of the
// buffered CSV/TSV data.
const csvFlushInterval = 5 * time.Second

func makeCSVReporter(w io.Writer, format tableDisplayFormat) (*csvReporter, func()) {
	r := &csvReporter{}
	r.mu.csvWriter = csv.NewWriter(w)
	if format == tableDisplayTSV {
		r.mu.csvWriter.Comma = '\t'
	}

	// Set up a flush daemon. This is useful when e.g. visualizing data
	// from change feeds.
	r.stop = make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(csvFlushInterval)
		for {
			select {
			case <-ticker.C:
				r.mu.Lock()
				r.mu.csvWriter.Flush()
				r.mu.Unlock()
			case <-r.stop:
				return
			}
		}
	}()
	cleanup := func() {
		close(r.stop)
	}
	return r, cleanup
}

func (p *csvReporter) describe(w io.Writer, cols []string) error {
	p.mu.Lock()
	if len(cols) == 0 {
		_ = p.mu.csvWriter.Write([]string{"# no columns"})
	} else {
		_ = p.mu.csvWriter.Write(cols)
	}
	p.mu.Unlock()
	return nil
}

func (p *csvReporter) iter(_ io.Writer, _ int, row []string) error {
	p.mu.Lock()
	if len(row) == 0 {
		_ = p.mu.csvWriter.Write([]string{"# empty"})
	} else {
		_ = p.mu.csvWriter.Write(row)
	}
	p.mu.Unlock()
	return nil
}

func (p *csvReporter) beforeFirstRow(_ io.Writer, _ rowStrIter) error { return nil }
func (p *csvReporter) doneNoRows(_ io.Writer) error                   { return nil }

func (p *csvReporter) doneRows(w io.Writer, seenRows int) error {
	p.mu.Lock()
	p.mu.csvWriter.Flush()
	p.mu.Unlock()
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

func (p *rawReporter) beforeFirstRow(_ io.Writer, _ rowStrIter) error { return nil }
func (p *rawReporter) doneNoRows(_ io.Writer) error                   { return nil }

func (p *rawReporter) doneRows(w io.Writer, nRows int) error {
	fmt.Fprintf(w, "# %d row%s\n", nRows, util.Pluralize(int64(nRows)))
	return nil
}

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

func (p *htmlReporter) beforeFirstRow(w io.Writer, _ rowStrIter) error {
	fmt.Fprintln(w, "<tbody>")
	return nil
}

func (p *htmlReporter) iter(w io.Writer, rowIdx int, row []string) error {
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

type recordReporter struct {
	cols        []string
	colRest     [][]string
	maxColWidth int
}

func (p *recordReporter) describe(w io.Writer, cols []string) error {
	for _, col := range cols {
		parts := strings.Split(col, "\n")
		for _, part := range parts {
			colLen := utf8.RuneCountInString(part)
			if colLen > p.maxColWidth {
				p.maxColWidth = colLen
			}
		}
		p.cols = append(p.cols, parts[0])
		p.colRest = append(p.colRest, parts[1:])
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
			lineCont := "+"
			if l == len(lines)-1 {
				lineCont = ""
			}
			// Note: special characters, including a vertical bar, in
			// the colLabel are not escaped here. This is in accordance
			// with the same behavior in PostgreSQL. However there is
			// special behavior for newlines.
			contChar := " "
			if len(p.colRest[j]) > 0 {
				contChar = "+"
			}
			fmt.Fprintf(w, "%-*s%s| %s%s\n", p.maxColWidth, colLabel, contChar, line, lineCont)
			for k, cont := range p.colRest[j] {
				if k == len(p.colRest[j])-1 {
					contChar = " "
				}
				fmt.Fprintf(w, "%-*s%s|\n", p.maxColWidth, cont, contChar)
			}
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

func (p *recordReporter) beforeFirstRow(_ io.Writer, _ rowStrIter) error { return nil }
func (p *recordReporter) doneNoRows(_ io.Writer) error                   { return nil }

type sqlReporter struct {
	noColumns bool
}

func (p *sqlReporter) describe(w io.Writer, cols []string) error {
	fmt.Fprint(w, "CREATE TABLE results (\n")
	for i, col := range cols {
		s := tree.Name(col)
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
		s := tree.DString(r)
		fmt.Fprintf(w, "%s", s.String())
		if i < len(row)-1 {
			fmt.Fprint(w, ", ")
		}
	}
	fmt.Fprint(w, ");\n")
	return nil
}

func (p *sqlReporter) beforeFirstRow(_ io.Writer, _ rowStrIter) error { return nil }
func (p *sqlReporter) doneNoRows(_ io.Writer) error                   { return nil }
func (p *sqlReporter) doneRows(w io.Writer, seenRows int) error {
	fmt.Fprintf(w, "-- %d row%s\n", seenRows, util.Pluralize(int64(seenRows)))
	return nil
}

// makeReporter instantiates a table formatter. It returns the
// formatter and a cleanup function that must be called in all cases
// when the formatting completes.
func makeReporter(w io.Writer) (rowReporter, func(), error) {
	switch cliCtx.tableDisplayFormat {
	case tableDisplayTable:
		return newASCIITableReporter(), nil, nil

	case tableDisplayTSV:
		fallthrough
	case tableDisplayCSV:
		reporter, cleanup := makeCSVReporter(w, cliCtx.tableDisplayFormat)
		return reporter, cleanup, nil

	case tableDisplayRaw:
		return &rawReporter{}, nil, nil

	case tableDisplayHTML:
		return &htmlReporter{escape: true, rowStats: true}, nil, nil

	case tableDisplayRecords:
		return &recordReporter{}, nil, nil

	case tableDisplaySQL:
		return &sqlReporter{}, nil, nil

	default:
		return nil, nil, errors.Errorf("unhandled display format: %d", cliCtx.tableDisplayFormat)
	}
}

// PrintQueryOutput takes a list of column names and a list of row
// contents writes a formatted table to 'w'.
func PrintQueryOutput(w io.Writer, cols []string, allRows rowStrIter) error {
	reporter, cleanup, err := makeReporter(w)
	if err != nil {
		return err
	}
	if cleanup != nil {
		defer cleanup()
	}
	return render(reporter, w, cols, allRows, nil, nil)
}
