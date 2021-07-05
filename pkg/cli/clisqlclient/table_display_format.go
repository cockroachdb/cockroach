// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlclient

import (
	"fmt"

	"github.com/spf13/pflag"
)

// TableDisplayFormat identifies the format with which SQL tabular
// results should be displayed.
type TableDisplayFormat int

// The following constants identify the supported table formats.
const (
	TableDisplayTSV TableDisplayFormat = iota
	TableDisplayCSV
	TableDisplayTable
	TableDisplayRecords
	TableDisplaySQL
	TableDisplayHTML
	TableDisplayRawHTML
	TableDisplayRaw
	TableDisplayLastFormat // this must remain at the end of the list.
)

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
	case TableDisplayHTML:
		return "html"
	case TableDisplayRawHTML:
		return "rawhtml"
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
	case "html":
		*f = TableDisplayHTML
	case "rawhtml":
		*f = TableDisplayRawHTML
	case "raw":
		*f = TableDisplayRaw
	default:
		return fmt.Errorf("invalid table display format: %s "+
			// Note: rawhtml is omitted intentionally. It is
			// only supported for the 'gen settings-table' command.
			"(possible values: tsv, csv, table, records, sql, html, raw)", s)
	}
	return nil
}
