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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/parquet"
)

type parquetWriter struct {
	inner      *parquet.Writer
	datumAlloc []tree.Datum
}

// newParquetSchemaDefintion returns a parquet schema definition based on the
// cdcevent.Row and the number of cols in the schema.
func newParquetSchemaDefintion(row cdcevent.Row) (*parquet.SchemaDefinition, int, error) {
	numCols := len(row.ResultColumns()) + 1

	columnNames := make([]string, numCols)
	columnTypes := make([]*types.T, numCols)

	idx := 0
	if err := row.ForEachColumn().Col(func(col cdcevent.ResultColumn) error {
		columnNames[idx] = col.Name
		columnTypes[idx] = col.Typ
		idx += 1
		return nil
	}); err != nil {
		return nil, 0, err
	}

	if err := row.ForAllMeta().Col(func(col cdcevent.ResultColumn) error {
		columnNames[idx] = col.Name
		columnTypes[idx] = col.Typ
		idx += 1
		return nil
	}); err != nil {
		return nil, 0, err
	}

	columnNames[idx] = parquetCrdbEventTypeColName
	columnTypes[idx] = types.String

	schemaDef, err := parquet.NewSchema(columnNames, columnTypes)
	if err != nil {
		return nil, 0, err
	}
	return schemaDef, numCols, nil
}

// newParquetWriterFromRow constructs a new parquet writer which outputs to
// the given sink. This function interprets the schema from the supplied row.
func newParquetWriterFromRow(
	row cdcevent.Row,
	sink io.Writer,
	knobs *TestingKnobs, /* may be nil */
	opts ...parquet.Option,
) (*parquetWriter, error) {
	schemaDef, numCols, err := newParquetSchemaDefintion(row)
	if err != nil {
		return nil, err
	}

	writerConstructor := parquet.NewWriter

	if knobs.EnableParquetMetadata {
		if opts, err = addParquetTestMetadata(row, opts); err != nil {
			return nil, err
		}

		// To use parquet test utils for reading datums, the writer needs to be
		// configured with additional metadata.
		writerConstructor = parquet.NewWriterWithReaderMeta
	}

	writer, err := writerConstructor(schemaDef, sink, opts...)
	if err != nil {
		return nil, err
	}
	return &parquetWriter{inner: writer, datumAlloc: make([]tree.Datum, numCols)}, nil
}

// addData writes the updatedRow, adding the row's event type. There is no guarantee
// that data will be flushed after this function returns.
func (w *parquetWriter) addData(updatedRow cdcevent.Row, prevRow cdcevent.Row) error {
	if err := populateDatums(updatedRow, prevRow, w.datumAlloc); err != nil {
		return err
	}

	return w.inner.AddRow(w.datumAlloc)
}

// close closes the writer and flushes any buffered data to the sink.
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
	if err := updatedRow.ForAllMeta().Datum(func(d tree.Datum, _ cdcevent.ResultColumn) error {
		datums = append(datums, d)
		return nil
	}); err != nil {
		return err
	}
	datums = append(datums, getEventTypeDatum(updatedRow, prevRow).DString())
	return nil
}

// addParquetTestMetadata appends options to the provided options to configure the
// parquet writer to write metadata required by cdc test feed factories.
func addParquetTestMetadata(row cdcevent.Row, opts []parquet.Option) ([]parquet.Option, error) {
	keyCols := make([]string, 0)
	if err := row.ForEachKeyColumn().Col(func(col cdcevent.ResultColumn) error {
		keyCols = append(keyCols, col.Name)
		return nil
	}); err != nil {
		return opts, err
	}
	opts = append(opts, parquet.WithMetadata(map[string]string{"keyCols": strings.Join(keyCols, ",")}))

	allCols := make([]string, 0)
	if err := row.ForEachColumn().Col(func(col cdcevent.ResultColumn) error {
		allCols = append(allCols, col.Name)
		return nil
	}); err != nil {
		return opts, err
	}

	if err := row.ForAllMeta().Col(func(col cdcevent.ResultColumn) error {
		allCols = append(allCols, col.Name)
		return nil
	}); err != nil {
		return opts, err
	}
	allCols = append(allCols, parquetCrdbEventTypeColName)

	opts = append(opts, parquet.WithMetadata(map[string]string{"allCols": strings.Join(allCols, ",")}))
	return opts, nil
}
