// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type columnEntry struct {
	column catalog.Column
	idx    int
}

type tableEntry struct {
	csvRow  []string
	columns []columnEntry
}

type csvEncoder struct {
	alloc                   tree.DatumAlloc
	virtualColumnVisibility changefeedbase.VirtualColumnVisibility

	buf    *bytes.Buffer
	writer *csv.Writer

	tableCache map[descpb.DescriptorVersion]tableEntry
}

var _ Encoder = &csvEncoder{}

func newCSVEncoder(opts changefeedbase.EncodingOptions) *csvEncoder {
	newBuf := bytes.NewBuffer([]byte{})
	newEncoder := &csvEncoder{
		virtualColumnVisibility: opts.VirtualColumns,
		buf:                     newBuf,
		writer:                  csv.NewWriter(newBuf),
	}
	newEncoder.writer.SkipNewline = true
	newEncoder.tableCache = make(map[descpb.DescriptorVersion]tableEntry)
	return newEncoder
}

func (e *csvEncoder) buildTableCacheEntry(row encodeRow) (tableEntry, error) {
	family, err := row.tableDesc.FindFamilyByID(row.familyID)
	if err != nil {
		return tableEntry{}, err
	}

	include := make(map[descpb.ColumnID]struct{}, len(family.ColumnIDs))
	var yes struct{}
	for _, colID := range family.ColumnIDs {
		include[colID] = yes
	}

	var columnCache []columnEntry

	for i, col := range row.tableDesc.PublicColumns() {
		_, inFamily := include[col.GetID()]
		virtual := col.IsVirtual() && e.virtualColumnVisibility == changefeedbase.OptVirtualColumnsNull
		if inFamily || virtual {
			columnCache = append(columnCache, columnEntry{
				column: col,
				idx:    i,
			})
		}
	}

	tableCSVRow := make([]string, 0, len(columnCache))

	entry := tableEntry{
		csvRow:  tableCSVRow,
		columns: columnCache,
	}

	return entry, nil
}

// EncodeKey implements the Encoder interface.
func (e *csvEncoder) EncodeKey(_ context.Context, row encodeRow) ([]byte, error) {
	return nil, nil
}

// EncodeValue implements the Encoder interface.
func (e *csvEncoder) EncodeValue(_ context.Context, row encodeRow) ([]byte, error) {
	if row.deleted {
		return nil, errors.Errorf(`cannot encode deleted rows into CSV format`)
	}

	var err error
	rowVersion := row.tableDesc.GetVersion()
	if _, ok := e.tableCache[rowVersion]; !ok {
		e.tableCache[rowVersion], err = e.buildTableCacheEntry(row)
		if err != nil {
			return nil, err
		}
	}

	entry := e.tableCache[rowVersion]
	entry.csvRow = entry.csvRow[:0]

	for _, colEntry := range entry.columns {
		datum := row.datums[colEntry.idx]
		if err := datum.EnsureDecoded(colEntry.column.GetType(), &e.alloc); err != nil {
			return nil, err
		}
		entry.csvRow = append(entry.csvRow, tree.AsString(datum.Datum))
	}

	e.buf.Reset()
	if err := e.writer.Write(entry.csvRow); err != nil {
		return nil, err
	}
	e.writer.Flush()
	return e.buf.Bytes(), nil
}

// EncodeResolvedTimestamp implements the Encoder interface.
func (e *csvEncoder) EncodeResolvedTimestamp(
	_ context.Context, _ string, resolved hlc.Timestamp,
) ([]byte, error) {
	return nil, errors.New("EncodeResolvedTimestamp is not supported with the CSV encoder")
}
