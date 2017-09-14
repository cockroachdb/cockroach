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

	"database/sql/driver"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/olekukonko/tablewriter"
)

// rowStrIter is an iterator interface for the printQueryOutput function. It is
// used so that results can be streamed to the row formatters as they arrive
// to the CLI.
type rowStrIter interface {
	Next() (row []string, err error)
	ToSlice() (allRows [][]string, err error)
	Align() []int
}

// rowSliceIter is an implementation of the rowStrIter interface and it is used
// to wrap a slice of rows that have already been completely buffered into
// memory.
type rowSliceIter struct {
	allRows [][]string
	index   int
	align   []int
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

func (iter *rowSliceIter) Align() []int {
	return iter.align
}

// newRowSliceIter is an implementation of the rowStrIter interface and it is
// used when the rows have not been buffered into memory yet and we want to
// stream them to the row formatters as they arrive over the network.
func newRowSliceIter(allRows [][]string, align []int) *rowSliceIter {
	return &rowSliceIter{
		allRows: allRows,
		index:   0,
		align:   align,
	}
}

type rowIter struct {
	rows          *sqlRows
	showMoreChars bool
	align         []int
}

func (iter *rowIter) Next() (row []string, err error) {
	nextRow, err := getNextRow(iter.rows)
	if err != nil {
		return nil, err
	}
	if nextRow == nil {
		return nil, io.EOF
	}
	if len(iter.align) < len(nextRow) {
		iter.align = make([]int, len(nextRow))
	}
	return formatRow(nextRow, iter.showMoreChars, iter.align), nil
}

func (iter *rowIter) ToSlice() ([][]string, error) {
	return getAllRowStrings(iter.rows, iter.showMoreChars)
}

func (iter *rowIter) Align() []int {
	return iter.align
}

func newRowIter(rows *sqlRows, showMoreChars bool) *rowIter {
	return &rowIter{
		rows:          rows,
		showMoreChars: showMoreChars,
		align:         []int{},
	}
}

func alignVal(val driver.Value) int {
	switch val.(type) {
	case string:
		return tablewriter.ALIGN_LEFT
	case []byte:
		return tablewriter.ALIGN_LEFT
	case int64:
		return tablewriter.ALIGN_RIGHT
	case float64:
		return tablewriter.ALIGN_RIGHT
	case bool:
		return tablewriter.ALIGN_CENTER
	default:
		return tablewriter.ALIGN_DEFAULT
	}
}

// printQueryOutput takes a list of column names and a list of row contents
// writes a formatted table to 'w', or simply the tag if empty. Note that
// printQueryOutput expects the tag to already be properly formatted.
func printQueryOutput(w io.Writer, cols []string, allRows rowStrIter, tag string) error {
	if len(cols) == 0 {
		// This operation did not return rows, just show the tag.
		fmt.Fprintln(w, tag)
		return nil
	}

	switch cliCtx.tableDisplayFormat {
	case tableDisplayPretty:
		// Initialize tablewriter and set column names as the header row.
		table := tablewriter.NewWriter(w)
		table.SetAutoFormatHeaders(false)
		table.SetAutoWrapText(false)
		table.SetHeader(cols)
		nRows := 0
		for {
			row, err := allRows.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			for i, r := range row {
				row[i] = expandTabsAndNewLines(r)
			}
			table.Append(row)
			nRows++
		}
		table.SetColumnAlignment(allRows.Align())
		table.Render()
		fmt.Fprintf(w, "(%d row%s)\n", nRows, util.Pluralize(int64(nRows)))

	case tableDisplayTSV:
		fallthrough
	case tableDisplayCSV:
		allRowsSlice, err := allRows.ToSlice()
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "%d row%s\n", len(allRowsSlice),
			util.Pluralize(int64(len(allRowsSlice))))

		csvWriter := csv.NewWriter(w)
		if cliCtx.tableDisplayFormat == tableDisplayTSV {
			csvWriter.Comma = '\t'
		}
		_ = csvWriter.Write(cols)
		_ = csvWriter.WriteAll(allRowsSlice)

	case tableDisplayRaw:
		fmt.Fprintf(w, "# %d column%s\n", len(cols),
			util.Pluralize(int64(len(cols))))
		nRows := 0
		for {
			row, err := allRows.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			nRows++
			fmt.Fprintf(w, "# row %d\n", nRows)
			for _, r := range row {
				fmt.Fprintf(w, "## %d\n%s\n", len(r), r)
			}
		}
		fmt.Fprintf(w, "# %d row%s\n", nRows, util.Pluralize(int64(nRows)))

	case tableDisplayHTML:
		fmt.Fprint(w, "<table>\n<thead><tr>")
		for _, col := range cols {
			fmt.Fprintf(w, "<th>%s</th>", html.EscapeString(col))
		}
		fmt.Fprint(w, "</tr></head>\n<tbody>\n")
		for {
			row, err := allRows.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			fmt.Fprint(w, "<tr>")
			for _, r := range row {
				fmt.Fprintf(w, "<td>%s</td>", strings.Replace(html.EscapeString(r), "\n", "<br/>", -1))
			}
			fmt.Fprint(w, "</tr>\n")
		}
		fmt.Fprint(w, "</tbody>\n</table>\n")

	case tableDisplayRecords:
		maxColWidth := 0
		for _, col := range cols {
			colLen := utf8.RuneCountInString(col)
			if colLen > maxColWidth {
				maxColWidth = colLen
			}
		}

		for i := 0; ; i++ {
			row, err := allRows.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			fmt.Fprintf(w, "-[ RECORD %d ]\n", i+1)
			for j, r := range row {
				lines := strings.Split(r, "\n")
				for l, line := range lines {
					colLabel := cols[j]
					if l > 0 {
						colLabel = ""
					}
					// Note: special characters, including a vertical bar, in
					// the colLabel are not escaped here. This is in accordance
					// with the same behavior in PostgreSQL.
					fmt.Fprintf(w, "%-*s | %s\n", maxColWidth, colLabel, line)
				}
			}
		}

	case tableDisplaySQL:
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
		for {
			row, err := allRows.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
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
		}
	}
	return nil
}
