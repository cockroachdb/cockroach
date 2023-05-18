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
	"github.com/apache/arrow/go/v11/parquet/compress"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/metadata"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type config struct {
	maxRowGroupLength int64
	version           parquet.Version
	compression       compress.Compression

	// Arbitrary kv metadata.
	metadata metadata.KeyValueMetadata
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

// WithCompressionCodec specifies the compression codec to use when writing
// columns.
func WithCompressionCodec(compression CompressionCodec) Option {
	return func(c *config) error {
		if _, ok := compressionCodecToParquet[compression]; !ok {
			return errors.AssertionFailedf("invalid compression codec")
		}

		c.compression = compressionCodecToParquet[compression]
		return nil
	}
}

// WithMetadata adds arbitrary kv metadata to the parquet file which can be
// read by a reader.
func WithMetadata(m map[string]string) Option {
	return func(c *config) error {
		for k, v := range m {
			if err := c.metadata.Append(k, v); err != nil {
				return err
			}
		}
		return nil
	}
}

var allowedVersions = map[string]parquet.Version{
	"v1.0": parquet.V1_0,
	"v2.4": parquet.V1_0,
	"v2.6": parquet.V2_6,
}

// compressionCodecToParquet is a mapping between CompressionCodec values and
// compress.Codecs.
var compressionCodecToParquet = map[CompressionCodec]compress.Compression{
	CompressionNone:   compress.Codecs.Uncompressed,
	CompressionGZIP:   compress.Codecs.Gzip,
	CompressionZSTD:   compress.Codecs.Zstd,
	CompressionSnappy: compress.Codecs.Snappy,
	CompressionBrotli: compress.Codecs.Brotli,
}

// A CompressionCodec is the codec used to compress columns when writing
// parquet files.
type CompressionCodec int64

const (
	// CompressionNone represents no compression.
	CompressionNone CompressionCodec = iota + 1
	// CompressionGZIP is the GZIP compression codec.
	CompressionGZIP
	// CompressionZSTD is the ZSTD compression codec.
	CompressionZSTD
	// CompressionSnappy is the Snappy compression codec.
	CompressionSnappy
	// CompressionBrotli is the Brotli compression codec.
	CompressionBrotli
	// LZO and LZ4 are unsupported. See comments on compress.Codecs.Lzo
	// and compress.Codecs.Lz4.
)

// A Writer writes datums into an io.Writer sink. The Writer should be Close()ed
// before attempting to read from the output sink so all data is flushed and
// parquet metadata is written.
type Writer struct {
	sch    *SchemaDefinition
	writer *file.Writer
	cfg    config

	ba *batchAlloc

	// The current number of rows written to the row group writer.
	currentRowGroupSize   int64
	currentRowGroupWriter file.BufferedRowGroupWriter
	// Caches the file.ColumnChunkWriters for each datumColumn in the
	// schema definition.
	columnChunkWriterCache map[int][]file.ColumnChunkWriter
}

// NewWriter constructs a new Writer which outputs to
// the given sink.
//
// TODO(#99028): maxRowGroupSize should be a configuration Option, along with
// compression schemes, allocator, batch size, page size etc
func NewWriter(sch *SchemaDefinition, sink io.Writer, opts ...Option) (*Writer, error) {
	cfg := config{
		maxRowGroupLength: parquet.DefaultMaxRowGroupLen,
		version:           parquet.V2_6,
		compression:       compress.Codecs.Uncompressed,
		metadata:          metadata.KeyValueMetadata{},
	}
	for _, opt := range opts {
		err := opt.apply(&cfg)
		if err != nil {
			return nil, err
		}
	}

	parquetOpts := []parquet.WriterProperty{
		parquet.WithCreatedBy("cockroachdb"),
		parquet.WithVersion(cfg.version),
		parquet.WithCompression(cfg.compression),
	}
	props := parquet.NewWriterProperties(parquetOpts...)
	writer := file.NewParquetWriter(sink, sch.schema.Root(), file.WithWriterProps(props),
		file.WithWriteMetadata(cfg.metadata))

	return &Writer{
		sch:                    sch,
		writer:                 writer,
		cfg:                    cfg,
		ba:                     &batchAlloc{},
		columnChunkWriterCache: make(map[int][]file.ColumnChunkWriter, len(sch.cols)),
	}, nil
}

// getColChunkWriters generates an array of file.ColumnChunkWriter
// for the datumColumn at colIdx.
func (w *Writer) getColChunkWriters(colIdx int) ([]file.ColumnChunkWriter, error) {
	if w.columnChunkWriterCache[colIdx] != nil {
		return w.columnChunkWriterCache[colIdx], nil
	}
	colChunkWriters := make([]file.ColumnChunkWriter, 0, w.sch.cols[colIdx].numPhysicalCols)
	for i := w.sch.cols[colIdx].physicalColsStartIdx; i < w.sch.cols[colIdx].physicalColsStartIdx+w.sch.cols[colIdx].numPhysicalCols; i += 1 {
		cw, err := w.currentRowGroupWriter.Column(int(i))
		if err != nil {
			return nil, err
		}
		colChunkWriters = append(colChunkWriters, cw)
	}
	return colChunkWriters, nil
}

func (w *Writer) writeDatumToColChunk(d tree.Datum, datumColIdx int) (err error) {
	colChunkWriters, ok := w.columnChunkWriterCache[datumColIdx]
	if !ok {
		colChunkWriters, err = w.getColChunkWriters(datumColIdx)
		if err != nil {
			return err
		}
		w.columnChunkWriterCache[datumColIdx] = colChunkWriters
	}

	// tree.NewFmtCtx uses an underlying pool, so we can assume there is no
	// allocation here.
	fmtCtx := tree.NewFmtCtx(tree.FmtExport)
	defer fmtCtx.Close()
	if err = w.sch.cols[datumColIdx].colWriter.Write(d, colChunkWriters, w.ba, fmtCtx); err != nil {
		return err
	}

	return nil
}

// AddRow writes the supplied datums. There is no guarantee
// that they will be flushed to the sink after AddRow returns.
func (w *Writer) AddRow(datums []tree.Datum) error {
	if len(datums) != len(w.sch.cols) {
		return errors.AssertionFailedf("expected %d datums in row, got %d datums",
			len(w.sch.cols), len(datums))
	}

	if w.currentRowGroupWriter == nil {
		w.currentRowGroupWriter = w.writer.AppendBufferedRowGroup()
	} else if w.currentRowGroupSize == w.cfg.maxRowGroupLength {
		if err := w.currentRowGroupWriter.Close(); err != nil {
			return err
		}
		w.currentRowGroupWriter = w.writer.AppendBufferedRowGroup()
		w.currentRowGroupSize = 0
		w.columnChunkWriterCache = make(map[int][]file.ColumnChunkWriter, len(w.sch.cols))
	}

	for datumColIdx, d := range datums {
		if err := w.writeDatumToColChunk(d, datumColIdx); err != nil {
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
