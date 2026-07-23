// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlexec

import (
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/errors"
	"github.com/olekukonko/tablewriter"
)

// RowStrIter is an iterator interface for the PrintQueryOutput function. It is
// used so that results can be streamed to the row formatters as they arrive
// to the CLI.
type RowStrIter interface {
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

// Next returns next row of rowSliceIter.
func (iter *rowSliceIter) Next() (row []string, err error) {
	if iter.index >= len(iter.allRows) {
		return nil, io.EOF
	}
	row = iter.allRows[iter.index]
	iter.index = iter.index + 1
	return row, nil
}

// ToSlice returns all rows of rowSliceIter.
func (iter *rowSliceIter) ToSlice() ([][]string, error) {
	return iter.allRows, nil
}

// Align returns alignment setting of rowSliceIter.
func (iter *rowSliceIter) Align() []int {
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
func NewRowSliceIter(allRows [][]string, align string) RowStrIter {
	return &rowSliceIter{
		allRows: allRows,
		index:   0,
		align:   convertAlign(align),
	}
}

type rowIter struct {
	rows          clisqlclient.Rows
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

func (iter *rowIter) Align() []int {
	cols := iter.rows.Columns()
	align := make([]int, len(cols))
	for i := range align {
		typName := iter.rows.ColumnTypeDatabaseTypeName(i)
		if typName == "" || strings.HasPrefix(typName, "_") {
			// All array types begin with "_" and user-defined types may not have a
			// type name available.
			align[i] = tablewriter.ALIGN_LEFT
			continue
		}
		switch typName {
		case "TEXT", "BYTEA", "CHAR", "BPCHAR", "NAME", "UUID":
			align[i] = tablewriter.ALIGN_LEFT
		case "INT2", "INT4", "INT8", "FLOAT4", "FLOAT8", "NUMERIC", "OID":
			align[i] = tablewriter.ALIGN_RIGHT
		case "BOOL":
			align[i] = tablewriter.ALIGN_CENTER
		default:
			align[i] = tablewriter.ALIGN_DEFAULT
		}
	}
	return align
}

func newRowIter(rows clisqlclient.Rows, showMoreChars bool) *rowIter {
	return &rowIter{
		rows:          rows,
		showMoreChars: showMoreChars,
	}
}

// rowReporter is used to render result sets.
//   - describe is called once in any case with the result column set.
//   - beforeFirstRow is called once upon the first row encountered.
//   - iter is called for every row, including the first (called after beforeFirstRowFn).
//   - doneRows is called once after the last row encountered (in case of no error).
//     This can also be called when there were no rows, if the rowsAffectedHook
//     passed to render() returns false.
//   - doneNoRows is called once when there were no rows and the rowsAffectedHook
//     returns true.
type rowReporter interface {
	describe(w io.Writer, cols []string) error
	beforeFirstRow(w io.Writer, allRows RowStrIter) error
	iter(w, ew io.Writer, rowIdx int, row []string) error
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
	w, ew io.Writer,
	cols []string,
	iter RowStrIter,
	completedHook func(),
	noRowsHook func() (bool, error),
) (retErr error) {
	described := false
	nRows := 0
	defer func() {
		// If the column headers are not printed yet, do it now.
		if !described {
			retErr = errors.CombineErrors(retErr, r.describe(w, cols))
		}

		// completedHook, if provided, is called unconditionally of error.
		if completedHook != nil {
			completedHook()
		}

		// We need to call doneNoRows/doneRows also unconditionally.
		var handled bool
		if nRows == 0 && noRowsHook != nil {
			var noRowsErr error
			handled, noRowsErr = noRowsHook()
			if noRowsErr != nil {
				retErr = errors.CombineErrors(retErr, noRowsErr)
				return
			}
		}
		if handled {
			retErr = errors.CombineErrors(retErr, r.doneNoRows(w))
		} else {
			retErr = errors.CombineErrors(retErr, r.doneRows(w, nRows))
		}

		if retErr != nil && nRows > 0 {
			fmt.Fprintf(ew, "(error encountered after some results were delivered)\n")
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
		if err = r.iter(w, ew, nRows, row); err != nil {
			return err
		}

		nRows++
	}

	return nil
}

// makeReporter instantiates a table formatter. It returns the
// formatter and a cleanup function that must be called in all cases
// when the formatting completes.
func (sqlExecCtx *Context) makeReporter(w io.Writer) (rowReporter, func(), error) {
	switch sqlExecCtx.TableDisplayFormat {
	case TableDisplayTable:
		return newASCIITableReporter(sqlExecCtx.TableBorderMode), nil, nil

	case TableDisplayTSV:
		fallthrough
	case TableDisplayCSV:
		reporter, cleanup := makeCSVReporter(w, sqlExecCtx.TableDisplayFormat)
		return reporter, cleanup, nil

	case TableDisplayNDJSON:
		fallthrough
	case TableDisplayJSON:
		return makeJSONReporter(sqlExecCtx.TableDisplayFormat), nil, nil

	case TableDisplayRaw:
		return &rawReporter{}, nil, nil

	case TableDisplayHTML:
		return &htmlReporter{escape: true, rowStats: true}, nil, nil

	case TableDisplayRawHTML:
		return &htmlReporter{escape: false, rowStats: false}, nil, nil

	case TableDisplayUnnumberedHTML:
		return &htmlReporter{escape: true, rowStats: false}, nil, nil

	case TableDisplayRecords:
		return &recordReporter{}, nil, nil

	case TableDisplaySQL:
		return &sqlReporter{}, nil, nil

	default:
		return nil, nil, errors.Errorf("unhandled display format: %d", sqlExecCtx.TableDisplayFormat)
	}
}

// PrintQueryOutput takes a list of column names and a list of row
// contents writes a formatted table to 'w'.
// Errors/warnings, if any, are written to 'ew'.
func (sqlExecCtx *Context) PrintQueryOutput(
	w, ew io.Writer, cols []string, allRows RowStrIter,
) error {
	reporter, cleanup, err := sqlExecCtx.makeReporter(w)
	if err != nil {
		return err
	}
	if cleanup != nil {
		defer cleanup()
	}
	return render(reporter, w, ew, cols, allRows, nil, nil)
}
