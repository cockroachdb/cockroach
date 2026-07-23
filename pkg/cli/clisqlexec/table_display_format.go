// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlexec

import (
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

// TableDisplayFormat identifies the format with which SQL tabular
// results should be displayed.
type TableDisplayFormat int

// The following constants identify the supported table formats.
const (
	// TableDisplayTSV has the values separated by tabs. It is like CSV
	// but using tabs instead of commas.
	TableDisplayTSV TableDisplayFormat = iota
	// TableDisplayCSV has the values separated by commas. Values that
	// contain commas themselves are enclosed in double quotes.
	TableDisplayCSV
	// TableDisplayTable is a tabular output format, that ensures that
	// all values in the same column are rendered with the same
	// width. This format follows what 'psql' does by default. It also
	// supports an additional customization option called 'border mode'.
	TableDisplayTable
	// TableDisplayRecords is a record-oriented format. It is somewhat
	// compatible with 'psql' "expanded display" mode.
	TableDisplayRecords
	// TableDisplayNDJSON reports results in an newlined-delimited JSON
	// format (https://github.com/ndjson/ndjson-spec).
	TableDisplayNDJSON
	// TableDisplayJSON reports results using JSON.
	TableDisplayJSON
	// TableDisplaySQL reports results using SQL statements that mimic
	// the creation of a SQL table containing the result values.
	TableDisplaySQL
	// TableDisplayHTML reports the results using a HTML table.  HTML
	// special characters inside the values are escaped.
	TableDisplayHTML
	// TableDisplayRawHTML is a variant of the HTML output format
	// supported specifically to generate CockroachDB's documentation.
	TableDisplayRawHTML
	// TableDisplayUnumberedHTML is a variant of the HTML output
	// format which does not include a row number prefix on each line.
	TableDisplayUnnumberedHTML
	// TableDisplayRaw is a special format optimized to ensure that the
	// values can be parsed accurately from the text output.
	TableDisplayRaw

	// TableDisplayLastFormat is a marker for the end of the list of
	// formats, for use in tests.
	TableDisplayLastFormat // this must remain at the end of the list.
)

// TableFormatHelp lists the possible values for the display format.
var TableFormatHelp = func() string {
	var buf strings.Builder
	comma := ""
	for tfmt := TableDisplayFormat(0); tfmt < TableDisplayLastFormat; tfmt++ {
		if tfmt == TableDisplayRawHTML {
			// Note: rawhtml is omitted intentionally from the user doc. It
			// is only supported for the 'gen settings-table' command.
			continue
		}
		buf.WriteString(comma)
		buf.WriteString(tfmt.String())
		comma = ", "
	}
	return buf.String()
}()

var _ pflag.Value = (*TableDisplayFormat)(nil)

// Type implements the pflag.Value interface.
func (f *TableDisplayFormat) Type() string { return "string" }

// String implements the pflag.Value interface.
func (f *TableDisplayFormat) String() string {
	switch *f {
	case TableDisplayTSV:
		return "tsv"
	case TableDisplayCSV:
		return "csv"
	case TableDisplayTable:
		return "table"
	case TableDisplayRecords:
		return "records"
	case TableDisplaySQL:
		return "sql"
	case TableDisplayNDJSON:
		return "ndjson"
	case TableDisplayJSON:
		return "json"
	case TableDisplayHTML:
		return "html"
	case TableDisplayRawHTML:
		return "rawhtml"
	case TableDisplayUnnumberedHTML:
		return "unnumbered-html"
	case TableDisplayRaw:
		return "raw"
	}
	return ""
}

// Set implements the pflag.Value interface.
func (f *TableDisplayFormat) Set(s string) error {
	switch s {
	case "tsv":
		*f = TableDisplayTSV
	case "csv":
		*f = TableDisplayCSV
	case "table":
		*f = TableDisplayTable
	case "records":
		*f = TableDisplayRecords
	case "sql":
		*f = TableDisplaySQL
	case "ndjson":
		*f = TableDisplayNDJSON
	case "json":
		*f = TableDisplayJSON
	case "html":
		*f = TableDisplayHTML
	case "rawhtml":
		*f = TableDisplayRawHTML
	case "unnumbered-html":
		*f = TableDisplayUnnumberedHTML
	case "raw":
		*f = TableDisplayRaw
	default:
		return errors.WithHintf(
			errors.Newf("invalid table display format: %s", s),
			"Possible values: %s.", TableFormatHelp)
	}
	return nil
}
