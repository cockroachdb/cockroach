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

		c.maxRowGroupLength = int64(l)
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
// before attempting to read from the output sink so parquet metadata is
// available.
type Writer struct {
	sch    *SchemaDefinition
	writer *file.Writer
	cfg    *config

	ba *batchAlloc

	// rowAlloc is a RowWriter which can be reused by this Writer
	// for sequential, synchronous calls to Writer.AddData.
	rowAlloc *RowWriter

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
	w.rowAlloc = &RowWriter{w: w, cIdx: 0}
	return w, nil
}

func (w *Writer) writeDatumToColChunk(d tree.Datum, colIdx int) error {
	if colIdx >= len(w.sch.cols) {
		return errors.AssertionFailedf("column index %d out of bounds for"+
			" array of size %d", colIdx, len(w.sch.cols))
	}
	// Note that EquivalentOrNull only allows null equivalence if the receiver is null.
	if !d.ResolvedType().EquivalentOrNull(w.sch.cols[colIdx].typ, false) {
		return errors.AssertionFailedf("expected datum of type %s, but found datum"+
			"	of type: %s at column index %d", w.sch.cols[colIdx].typ.Name(), d.ResolvedType().Name(), colIdx)
	}

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
	w    *Writer
	cIdx int
}

// WriteColumn writes a datum. This method should be called for all datums in a
// row in order. Calling this method with datums out of order or calling this
// method more times than there are columns will may in an error.
func (r *RowWriter) WriteColumn(d tree.Datum) error {
	if r.cIdx == len(r.w.sch.cols) {
		return errors.AssertionFailedf("added too many columns to row writer")
	}

	if err := r.w.writeDatumToColChunk(d, r.cIdx); err != nil {
		return err
	}
	r.cIdx += 1

	if r.cIdx == len(r.w.sch.cols) {
		r.w.currentRowGroupSize += 1
	}
	return nil
}

func (r *RowWriter) reset() *RowWriter {
	r.cIdx = 0
	return r
}

// AddData writes a row. There is no guarantee that the row will
// immediately be flushed to the output sink.
//
// Datums should be in the same order as specified in the
// SchemaDefinition of the Writer.
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

	return w.rowAlloc.reset(), nil
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
