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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type csvEncoder struct {
	alloc                   tree.DatumAlloc
	virtualColumnVisibility string

	buf    *bytes.Buffer
	writer *csv.Writer

	include map[descpb.DescriptorVersion]map[descpb.ColumnID]struct{}
}

var _ Encoder = &csvEncoder{}

func newCSVEncoder(opts map[string]string) *csvEncoder {
	newBuf := bytes.NewBuffer([]byte{})
	newEncoder := &csvEncoder{
		virtualColumnVisibility: opts[changefeedbase.OptVirtualColumns],
		buf:                     newBuf,
		writer:                  csv.NewWriter(newBuf),
	}
	newEncoder.writer.SkipNewline = true
	newEncoder.include = make(map[descpb.DescriptorVersion]map[descpb.ColumnID]struct{})
	return newEncoder
}

func buildIncludeMap(row encodeRow) (map[descpb.ColumnID]struct{}, error) {
	family, err := row.tableDesc.FindFamilyByID(row.familyID)
	if err != nil {
		return nil, err
	}
	include := make(map[descpb.ColumnID]struct{}, len(family.ColumnIDs))
	var yes struct{}
	for _, colID := range family.ColumnIDs {
		include[colID] = yes
	}

	return include, nil
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
	if _, ok := e.include[rowVersion]; !ok {
		e.include[rowVersion], err = buildIncludeMap(row)
		if err != nil {
			return nil, err
		}
	}

	// TODO(sherman): Currently we store the values of the row into this csvRow
	// array and then call the CSV write function with this array passed in. This
	// could incur a loss in performance when the row is very large, as allocating
	// new memory can be a costly operation. Ideally, we should just write each
	// value in the row directly to the buffer, rather than storing each value in
	// an array and then writing the entire array to the buffer. There are no
	// methods in the CSV writer package that would support this, which is why
	// this has not been done so far. Keep an eye out for the performance of CSV
	// encoding for large rows and determine if it would be better to create our
	// own CSV writer that writes values directly to the buffer.
	var csvRow []string

	for i, col := range row.tableDesc.PublicColumns() {
		_, inFamily := e.include[rowVersion][col.GetID()]
		virtual := col.IsVirtual() && e.virtualColumnVisibility == string(changefeedbase.OptVirtualColumnsNull)
		if inFamily || virtual {
			datum := row.datums[i]
			if err := datum.EnsureDecoded(col.GetType(), &e.alloc); err != nil {
				return nil, err
			}
			csvRow = append(csvRow, tree.AsString(datum.Datum))
		}
	}

	e.buf.Reset()
	if err := e.writer.Write(csvRow); err != nil {
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
