// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"io"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/parquet"
)

type parquetWriter struct {
	inner      *parquet.Writer
	datumAlloc []tree.Datum
}

// newParquetWriterFromRow constructs a new parquet writer which outputs to
// the given sink. This function interprets the schema from the supplied row.
func newParquetWriterFromRow(
	row cdcevent.Row, sink io.Writer, opts ...parquet.Option,
) (*parquetWriter, error) {
	columnNames := make([]string, len(row.ResultColumns())+1)
	columnTypes := make([]*types.T, len(row.ResultColumns())+1)

	idx := 0
	if err := row.ForEachColumn().Col(func(col cdcevent.ResultColumn) error {
		columnNames[idx] = col.Name
		columnTypes[idx] = col.Typ
		idx += 1
		return nil
	}); err != nil {
		return nil, err
	}

	columnNames[idx] = parquetCrdbEventTypeColName
	columnTypes[idx] = types.String

	schemaDef, err := parquet.NewSchema(columnNames, columnTypes)
	if err != nil {
		return nil, err
	}

	writerConstructor := parquet.NewWriter
	if includeParquetTestMetadata {
		writerConstructor = parquet.NewWriterWithReaderMeta
	}

	writer, err := writerConstructor(schemaDef, sink, opts...)
	if err != nil {
		return nil, err
	}
	return &parquetWriter{inner: writer, datumAlloc: make([]tree.Datum, len(columnNames))}, nil
}

// addData writes the updatedRow, adding the row's event type. There is no guarantee
// that data will be flushed after this function returns.
func (w *parquetWriter) addData(updatedRow cdcevent.Row, prevRow cdcevent.Row) error {
	if err := populateDatums(updatedRow, prevRow, w.datumAlloc); err != nil {
		return err
	}

	return w.inner.AddRow(w.datumAlloc)
}

// Close closes the writer and flushes any buffered data to the sink.
func (w *parquetWriter) close() error {
	return w.inner.Close()
}

// populateDatums writes the appropriate datums into the datumAlloc slice.
func populateDatums(updatedRow cdcevent.Row, prevRow cdcevent.Row, datumAlloc []tree.Datum) error {
	datums := datumAlloc[:0]

	if err := updatedRow.ForEachColumn().Datum(func(d tree.Datum, _ cdcevent.ResultColumn) error {
		datums = append(datums, d)
		return nil
	}); err != nil {
		return err
	}
	datums = append(datums, getEventTypeDatum(updatedRow, prevRow).DString())
	return nil
}
