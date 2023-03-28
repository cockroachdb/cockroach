// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachparquet

import (
	"io"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// A DatumRowIter iterates over all the datums in a row.
type DatumRowIter func(func(d tree.Datum) error) error

// A SchemaRowIter iterates over all the columns in a row.
type SchemaRowIter func(func(colName string, colType *types.T) error) error

// A Writer writes datums into an io.Writer sink. The Writer should be Close()ed
// before attempting to read from the output sink so parquet metadata is
// available.
type Writer struct {
	sch             *SchemaDefinition
	writer          *file.Writer
	maxRowGroupSize int64

	currentRowGroupSize   int64
	currentRowGroupWriter file.BufferedRowGroupWriter
}

// NewWriter constructs a new Writer which outputs to
// the given sink.
//
// maxRowGroupSize specifies the maximum number of rows to include
// in a row group when writing to the sink.
func NewWriter(sch *SchemaDefinition, sink io.Writer, maxRowGroupSize int64) (*Writer, error) {
	// TODO(#99028): support compression schemes
	// TODO(#99028): add options for more stability / performance (allocator, batch size, page size)
	opts := []parquet.WriterProperty{parquet.WithCreatedBy("cockroachdb"),
		parquet.WithMaxRowGroupLength(1)}
	props := parquet.NewWriterProperties(opts...)
	writer := file.NewParquetWriter(sink, sch.schema.Root(), file.WithWriterProps(props))
	return &Writer{
		sch:             sch,
		writer:          writer,
		maxRowGroupSize: maxRowGroupSize,
	}, nil
}

func (w *Writer) writeDatumToColChunk(d tree.Datum, colIdx int) error {
	cw, err := w.currentRowGroupWriter.Column(colIdx)
	if err != nil {
		return err
	}

	err = w.sch.cols[colIdx].colWriter(d, cw)
	if err != nil {
		return err
	}
	return nil
}

// SchemaDefinition returns the SchemaDefinition for this writer.
func (w *Writer) SchemaDefinition() *SchemaDefinition {
	return w.sch
}

// AddData writes a row. There is no guarantee that the row will
// immediately be flushed to the output sink.
//
// Datums should be in the same order as specified in the
// SchemaDefinition of the Writer.
func (w *Writer) AddData(it DatumRowIter) error {
	if w.currentRowGroupWriter == nil {
		w.currentRowGroupWriter = w.writer.AppendBufferedRowGroup()
	} else if w.currentRowGroupSize == w.maxRowGroupSize {
		if err := w.currentRowGroupWriter.Close(); err != nil {
			return err
		}
		w.currentRowGroupWriter = w.writer.AppendBufferedRowGroup()
		w.currentRowGroupSize = 0
	}

	cIdx := 0
	if err := it(func(d tree.Datum) error {
		if err := w.writeDatumToColChunk(d, cIdx); err != nil {
			return err
		}
		cIdx += 1
		return nil
	}); err != nil {
		return err
	}
	w.currentRowGroupSize += 1
	return nil
}

// Close closes the writer and flushes any buffered data to the sink.
// If the sink implements io.WriteCloser, it will be closed by this method.
func (w *Writer) Close() error {
	if err := w.currentRowGroupWriter.Close(); err != nil {
		return err
	}
	return w.writer.Close()
}
