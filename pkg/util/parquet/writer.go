// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package parquet

import (
	"io"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type config struct {
	maxRowGroupLength int64
	version           parquet.Version
}

func newConfig() *config {
	return &config{
		maxRowGroupLength: parquet.DefaultMaxRowGroupLen,
		version:           parquet.V2_6,
	}
}

// An Option is a configurable setting for the Writer.
type Option func(c *config) error

func (f Option) apply(c *config) error {
	return f(c)
}

// WithMaxRowGroupLength specifies the maximum number of rows to include
// in a row group when writing data.
func WithMaxRowGroupLength(l int64) Option {
	return func(c *config) error {
		if l <= 0 {
			return errors.AssertionFailedf("max group length must be greater than 0")
		}

		c.maxRowGroupLength = l
		return nil
	}
}

// WithVersion specifies the parquet version to use when writing data.
// Valid options are "v1.0", "v2.4", and "v2.6".
func WithVersion(v string) Option {
	return func(c *config) error {
		if _, ok := allowedVersions[v]; !ok {
			return errors.AssertionFailedf("invalid version string")
		}

		c.version = allowedVersions[v]
		return nil
	}
}

var allowedVersions = map[string]parquet.Version{
	"v1.0": parquet.V1_0,
	"v2.4": parquet.V1_0,
	"v2.6": parquet.V2_6,
}

// A Writer writes datums into an io.Writer sink. The Writer should be Close()ed
// before attempting to read from the output sink so all data is flushed and
// parquet metadata is written.
type Writer struct {
	sch    *SchemaDefinition
	writer *file.Writer
	cfg    *config

	ba *batchAlloc

	// rowWriterAlloc is a RowWriter which can be reused by this Writer
	// for sequential, synchronous calls to Writer.AddData.
	rowWriterAlloc *RowWriter

	currentRowGroupSize   int64
	currentRowGroupWriter file.BufferedRowGroupWriter
}

// NewWriter constructs a new Writer which outputs to
// the given sink.
//
// TODO(#99028): maxRowGroupSize should be a configuration Option, along with
// compression schemes, allocator, batch size, page size etc
func NewWriter(sch *SchemaDefinition, sink io.Writer, opts ...Option) (*Writer, error) {
	cfg := newConfig()
	for _, opt := range opts {
		err := opt.apply(cfg)
		if err != nil {
			return nil, err
		}
	}

	parquetOpts := []parquet.WriterProperty{parquet.WithCreatedBy("cockroachdb"),
		parquet.WithVersion(cfg.version)}
	props := parquet.NewWriterProperties(parquetOpts...)
	writer := file.NewParquetWriter(sink, sch.schema.Root(), file.WithWriterProps(props))

	w := &Writer{
		sch:    sch,
		writer: writer,
		cfg:    cfg,
		ba:     &batchAlloc{},
	}

	bitmapSize := (len(sch.cols) / 64) + 1
	// When reusing the alloc, we assert that the previous row was written by
	// checking that count == len(sch.cols). To ensure the assertion passes when
	// no rows were written yet, the count is initialized to len(sch.cols) instead
	// of 0
	w.rowWriterAlloc = &RowWriter{w: w, colCount: len(sch.cols), colIdxMap: make([]uint64, bitmapSize)}
	return w, nil
}

func (w *Writer) writeDatumToColChunk(d tree.Datum, colIdx int) error {
	cw, err := w.currentRowGroupWriter.Column(colIdx)
	if err != nil {
		return err
	}

	err = w.sch.cols[colIdx].colWriter(d, cw, w.ba)
	if err != nil {
		return err
	}
	return nil
}

// A RowWriter is used to write datums in a row.
type RowWriter struct {
	w *Writer
	// colIdxMap is a bitmap which records which indexes
	// in a row have been written to. Using an integer bitmap
	// allows us to reset it efficiently.
	colIdxMap []uint64
	colCount  int
}

func (r *RowWriter) idxIsWritten(idx int) bool {
	return r.colIdxMap[idx/64]&(1<<(idx%64)) != 0
}

func (r *RowWriter) writeIdx(idx int) {
	r.colIdxMap[idx/64] = r.colIdxMap[idx/64] | (1 << (idx % 64))
}

// WriteColumn writes a datum in a row. This should be called once for each
// column in a row.
func (r *RowWriter) WriteColumn(idx int, d tree.Datum) error {
	if idx >= len(r.w.sch.cols) {
		return errors.AssertionFailedf("column index %d out of bounds for"+
			" expected row size of %d", idx, len(r.w.sch.cols))
	}
	// Note that EquivalentOrNull only allows null equivalence if the receiver is null.
	if !d.ResolvedType().EquivalentOrNull(r.w.sch.cols[idx].typ, false) {
		return errors.AssertionFailedf("expected datum of type %s, but found datum"+
			"	of type: %s at column index %d", r.w.sch.cols[idx].typ.Name(), d.ResolvedType().Name(), idx)
	}
	if r.idxIsWritten(idx) {
		return errors.AssertionFailedf("previously wrote datum to row at idx %d", idx)
	}

	if err := r.w.writeDatumToColChunk(d, idx); err != nil {
		return err
	}

	r.colCount += 1
	r.writeIdx(idx)

	if r.colCount == len(r.w.sch.cols) {
		r.w.currentRowGroupSize += 1
	}
	return nil
}

func (r *RowWriter) allColumnsWritten() bool {
	return r.colCount == len(r.w.sch.cols)
}

func (r *RowWriter) reset() {
	r.colCount = 0
	for i := 0; i < len(r.colIdxMap); i++ {
		r.colIdxMap[i] = 0
	}
}

// AddData returns a RowWriter which can be used to write a single row. The
// returned RowWriter must be used to write all datums in the row before
// the next call to AddData.
func (w *Writer) AddData() (*RowWriter, error) {
	if w.currentRowGroupWriter == nil {
		w.currentRowGroupWriter = w.writer.AppendBufferedRowGroup()
	} else if w.currentRowGroupSize == w.cfg.maxRowGroupLength {
		if err := w.currentRowGroupWriter.Close(); err != nil {
			return nil, err
		}
		w.currentRowGroupWriter = w.writer.AppendBufferedRowGroup()
		w.currentRowGroupSize = 0
	}

	if !w.rowWriterAlloc.allColumnsWritten() {
		return nil, errors.AssertionFailedf("cannot add a new row before the previous row was written")
	}
	w.rowWriterAlloc.reset()
	return w.rowWriterAlloc, nil
}

// Close closes the writer and flushes any buffered data to the sink.
// If the sink implements io.WriteCloser, it will be closed by this method.
func (w *Writer) Close() error {
	if w.currentRowGroupWriter != nil {
		if err := w.currentRowGroupWriter.Close(); err != nil {
			return err
		}
	}
	return w.writer.Close()
}
