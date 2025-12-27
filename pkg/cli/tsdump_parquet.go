// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/parquet"
	"github.com/cockroachdb/errors"
)

// maxParquetFileBytes is the maximum size of a single parquet file before
// splitting to a new file. Set to 1GiB.
const maxParquetFileBytes = 1 << 30

// parquetTSWriter implements the tsWriter interface for parquet output.
// It writes time series data to parquet files, splitting at 1GiB boundaries.
type parquetTSWriter struct {
	schema *parquet.SchemaDefinition
	writer *parquet.Writer
	buf    *bytes.Buffer

	// output is the destination for parquet data. For file output, this
	// writes to files named tsdump_0.parquet, tsdump_1.parquet, etc.
	// For stdout, data is written directly.
	output     io.Writer
	fileIndex  int
	toStdout   bool
	filePrefix string
}

// newParquetTSWriter creates a new parquet writer for time series data.
// If toStdout is true, output goes to stdout; otherwise, files are created
// with the given prefix (e.g., "tsdump" creates tsdump_0.parquet, etc.).
func newParquetTSWriter(toStdout bool, filePrefix string) (*parquetTSWriter, error) {
	// Define schema: name, source, timestamp_nanos, value
	columnNames := []string{"name", "source", "timestamp_nanos", "value"}
	columnTypes := []*types.T{types.String, types.String, types.Int, types.Float}

	schema, err := parquet.NewSchema(columnNames, columnTypes)
	if err != nil {
		return nil, errors.Wrap(err, "creating parquet schema")
	}

	w := &parquetTSWriter{
		schema:     schema,
		buf:        &bytes.Buffer{},
		toStdout:   toStdout,
		filePrefix: filePrefix,
	}

	// Initialize the first writer
	if err := w.initWriter(); err != nil {
		return nil, err
	}

	return w, nil
}

// initWriter initializes a new parquet writer, either to a new file or to
// the existing buffer for stdout.
func (w *parquetTSWriter) initWriter() error {
	if w.toStdout {
		w.output = os.Stdout
	} else {
		filename := fmt.Sprintf("%s_%d.parquet", w.filePrefix, w.fileIndex)
		f, err := os.Create(filename)
		if err != nil {
			return errors.Wrapf(err, "creating parquet file %s", filename)
		}
		w.output = f
		fmt.Fprintf(os.Stderr, "Writing to %s\n", filename)
	}

	w.buf.Reset()
	writer, err := parquet.NewWriter(
		w.schema,
		w.buf,
		parquet.WithCompressionCodec(parquet.CompressionZSTD),
	)
	if err != nil {
		return errors.Wrap(err, "creating parquet writer")
	}
	w.writer = writer
	return nil
}

// Emit implements the tsWriter interface.
func (w *parquetTSWriter) Emit(data *tspb.TimeSeriesData) error {
	// Write each datapoint as a separate row
	for _, dp := range data.Datapoints {
		datums := []tree.Datum{
			tree.NewDString(data.Name),
			tree.NewDString(data.Source),
			tree.NewDInt(tree.DInt(dp.TimestampNanos)),
			tree.NewDFloat(tree.DFloat(dp.Value)),
		}

		if err := w.writer.AddRow(datums); err != nil {
			return errors.Wrap(err, "writing parquet row")
		}

		// Check if we need to split to a new file (only for file output)
		if !w.toStdout && w.writer.BufferedBytesEstimate() >= maxParquetFileBytes {
			if err := w.rotateFile(); err != nil {
				return err
			}
		}
	}
	return nil
}

// rotateFile closes the current file and opens a new one.
func (w *parquetTSWriter) rotateFile() error {
	// Close current writer and flush to file
	if err := w.writer.Close(); err != nil {
		return errors.Wrap(err, "closing parquet writer")
	}

	// Write buffer to file
	if f, ok := w.output.(*os.File); ok {
		if _, err := w.buf.WriteTo(f); err != nil {
			return errors.Wrap(err, "writing to parquet file")
		}
		if err := f.Close(); err != nil {
			return errors.Wrap(err, "closing parquet file")
		}
	}

	// Start new file
	w.fileIndex++
	return w.initWriter()
}

// Flush implements the tsWriter interface.
func (w *parquetTSWriter) Flush() error {
	if w.writer == nil {
		return nil
	}

	// Close the parquet writer to finalize the file
	if err := w.writer.Close(); err != nil {
		return errors.Wrap(err, "closing parquet writer")
	}

	// Write buffer to output
	if _, err := w.buf.WriteTo(w.output); err != nil {
		return errors.Wrap(err, "writing parquet data")
	}

	// Close file if not stdout
	if f, ok := w.output.(*os.File); ok && !w.toStdout {
		if err := f.Close(); err != nil {
			return errors.Wrap(err, "closing parquet file")
		}
	}

	return nil
}

var _ tsWriter = &parquetTSWriter{}
