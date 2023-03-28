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

// Config stores configurable options for the Writer.
type Config struct {
	maxRowGroupLength int64
	version           parquet.Version
}

func newConfig() *Config {
	return &Config{
		maxRowGroupLength: parquet.DefaultMaxRowGroupLen,
		version:           parquet.V2_6,
	}
}

type option interface {
	apply(c *Config) error
}

// WithMaxRowGroupLength specifies the maximum number of rows to include
// in a row group when writing data.
type WithMaxRowGroupLength int64

func (l WithMaxRowGroupLength) apply(c *Config) error {
	if l <= 0 {
		return errors.AssertionFailedf("max group length must be greater than 0")
	}

	c.maxRowGroupLength = int64(l)
	return nil
}

// WithVersion specifies the parquet version to use when writing data.
// Valid options are "v1.0", "v2.4", and "v2.6".
type WithVersion string

func (v WithVersion) apply(c *Config) error {
	if _, ok := allowedVersions[string(v)]; !ok {
		return errors.AssertionFailedf("invalid version string")
	}

	c.version = allowedVersions[string(v)]
	return nil
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
	cfg    *Config

	currentRowGroupSize   int64
	currentRowGroupWriter file.BufferedRowGroupWriter
}

// NewWriter constructs a new Writer which outputs to
// the given sink.
//
// TODO(#99028): maxRowGroupSize should be a configuration option, along with
// compression schemes, allocator, batch size, page size etc
func NewWriter(sch *SchemaDefinition, sink io.Writer, opts ...option) (*Writer, error) {
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

	return &Writer{
		sch:    sch,
		writer: writer,
		cfg:    cfg,
	}, nil
}

func (w *Writer) writeDatumToColChunk(d tree.Datum, colIdx int) error {
	if colIdx >= len(w.sch.cols) {
		return errors.AssertionFailedf("column index %d out of bounds for"+
			" array of size %d", colIdx, len(w.sch.cols))
	}
	// Note that EquivalentOrNull only allows null equivalence if the receiver is null.
	if !d.ResolvedType().EquivalentOrNull(w.sch.cols[colIdx].typ, false) {
		return errors.AssertionFailedf("expected datum of type %s, but found datum"+
			"	of type: %s", w.sch.cols[colIdx].typ.Name(), d.ResolvedType().Name())
	}

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

// Config returns the Config for this writer.
func (w *Writer) Config() *Config {
	return w.cfg
}

// AddData writes a row. There is no guarantee that the row will
// immediately be flushed to the output sink.
//
// Datums should be in the same order as specified in the
// SchemaDefinition of the Writer.
func (w *Writer) AddData(datums []tree.Datum) error {
	if w.currentRowGroupWriter == nil {
		w.currentRowGroupWriter = w.writer.AppendBufferedRowGroup()
	} else if w.currentRowGroupSize == w.cfg.maxRowGroupLength {
		if err := w.currentRowGroupWriter.Close(); err != nil {
			return err
		}
		w.currentRowGroupWriter = w.writer.AppendBufferedRowGroup()
		w.currentRowGroupSize = 0
	}

	for cIdx := 0; cIdx < len(datums); cIdx++ {
		if err := w.writeDatumToColChunk(datums[cIdx], cIdx); err != nil {
			return err
		}
	}
	w.currentRowGroupSize += 1
	return nil
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
