// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

const exportFilePatternPart = "%part%"
const exportFilePatternDefault = exportFilePatternPart + ".csv"

// csvSink wraps a csv writer, optionally a compressing writer shim and a cloud-
// sink writer together such that rows written to the CSV writer will be encoded
// and then written to the underlying sink, potentially passing though the
// compressor and with the bytes written being counted along the way. Closing
// a csvSink will flush the csv writer and compressor before closing the sink.
// A csvSink *must* be closed, either via Close or CloseWithError but can be
// closed more than once, so it is safe to defer a catch-all CloseWithError and
// then also Close() it upon successful completion.
type csvSink struct {
	csvWriter  *csv.Writer  // will have either csvSink or compressor as its sink.
	compressor *gzip.Writer // will have the csvSink itself as its sink.
	sink       cloud.WriteCloserWithError
	written    int
}

func (c *csvSink) Write(p []byte) (int, error) {
	n, err := c.sink.Write(p)
	c.written += n
	return n, err
}

// Write append record to csv file.
func (c *csvSink) WriteRow(record []string) error {
	return c.csvWriter.Write(record)
}

// Close closes the compressor writer which
// appends archive footers.
func (c *csvSink) Close() error {
	if c.sink == nil {
		return nil
	}
	c.csvWriter.Flush()
	err := c.csvWriter.Error()

	if c.compressor != nil {
		err = errors.CombineErrors(err, c.compressor.Close())
	}

	if err != nil {
		err = errors.CombineErrors(c.sink.CloseWithError(err), err)
	} else {
		err = c.sink.Close()
	}
	c.sink = nil
	return err
}

func (c *csvSink) CloseWithError(err error) error {
	if c.sink == nil {
		return nil
	}
	err = c.sink.CloseWithError(err)
	c.sink = nil
	return err
}

func (c *csvSink) reset() {
	c.written = 0
	if c.compressor != nil {
		c.compressor.Reset(c)
	}
}

func newCsvSink(sp execinfrapb.CSVWriterSpec) *csvSink {
	s := &csvSink{}
	switch sp.CompressionCodec {
	case execinfrapb.FileCompression_Gzip:
		s.compressor = gzip.NewWriter(s)
		s.csvWriter = csv.NewWriter(s.compressor)
	default:
		s.csvWriter = csv.NewWriter(s)
	}
	if sp.Options.Comma != 0 {
		s.csvWriter.Comma = sp.Options.Comma
	}
	return s
}

func newCSVWriterProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.CSVWriterSpec,
	input execinfra.RowSource,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	c := &csvWriter{
		flowCtx:     flowCtx,
		processorID: processorID,
		spec:        spec,
		input:       input,
		output:      output,
	}
	semaCtx := tree.MakeSemaContext()
	if err := c.out.Init(&execinfrapb.PostProcessSpec{}, c.OutputTypes(), &semaCtx, flowCtx.NewEvalCtx(), output); err != nil {
		return nil, err
	}
	return c, nil
}

type csvWriter struct {
	flowCtx     *execinfra.FlowCtx
	processorID int32
	spec        execinfrapb.CSVWriterSpec
	input       execinfra.RowSource
	out         execinfra.ProcOutputHelper
	output      execinfra.RowReceiver
}

var _ execinfra.Processor = &csvWriter{}

func (sp *csvWriter) OutputTypes() []*types.T {
	res := make([]*types.T, len(colinfo.ExportColumns))
	for i := range res {
		res[i] = colinfo.ExportColumns[i].Typ
	}
	return res
}

func (sp *csvWriter) MustBeStreaming() bool {
	return false
}

func (sp *csvWriter) fileName(spec execinfrapb.CSVWriterSpec, part string) string {
	pattern := exportFilePatternDefault
	if spec.NamePattern != "" {
		pattern = spec.NamePattern
	}

	name := strings.Replace(pattern, exportFilePatternPart, part, -1)

	switch sp.spec.CompressionCodec {
	case execinfrapb.FileCompression_Gzip:
		return name + ".gz"
	}
	return name
}

func (sp *csvWriter) Run(ctx context.Context) {
	ctx, span := tracing.ChildSpan(ctx, "csvWriter")
	defer span.Finish()

	err := func() error {
		typs := sp.input.OutputTypes()
		sp.input.Start(ctx)
		input := execinfra.MakeNoMetadataRowSource(sp.input, sp.output)

		alloc := &rowenc.DatumAlloc{}

		writer := newCsvSink(sp.spec)
		// Ensure that we always close even on early error returns. NB: csvExporter
		// allows double-Close, so if we close normally to flush and complete an
		// upload, this will be a noop, but if we take an early return, it should
		// close the uploader with a non-nil error (unexpected EOF works) to abort
		// the write.
		defer func() {
			_ = writer.CloseWithError(io.ErrUnexpectedEOF)
		}()

		var nullsAs string
		if sp.spec.Options.NullEncoding != nil {
			nullsAs = *sp.spec.Options.NullEncoding
		}

		f := tree.NewFmtCtx(tree.FmtExport)
		defer f.Close()

		csvRow := make([]string, len(typs))

		es, err := sp.flowCtx.Cfg.ExternalStorageFromURI(ctx, sp.spec.Destination, sp.spec.User())
		if err != nil {
			return err
		}
		defer es.Close()

		nodeID, err := sp.flowCtx.EvalCtx.NodeID.OptionalNodeIDErr(47970)
		if err != nil {
			return err
		}

		chunk := 0
		done := false
		for {
			part := fmt.Sprintf("n%d.%d", nodeID, chunk)
			chunk++
			filename := sp.fileName(sp.spec, part)
			var rows int64

			for {
				// If the bytes.Buffer sink exceeds the target size of a CSV file, we
				// flush before exporting any additional rows.
				if int64(writer.written) >= sp.spec.ChunkSize {
					break
				}
				if sp.spec.ChunkRows > 0 && rows >= sp.spec.ChunkRows {
					break
				}
				row, err := input.NextRow()
				if err != nil {
					return err
				}
				if row == nil {
					done = true
					break
				}
				rows++

				if writer.sink == nil {
					w, err := es.Writer(ctx, filename)
					if err != nil {
						return err
					}
					writer.sink = w
				}

				for i, ed := range row {
					if ed.IsNull() {
						if sp.spec.Options.NullEncoding != nil {
							csvRow[i] = nullsAs
							continue
						} else {
							return errors.New("NULL value encountered during EXPORT, " +
								"use `WITH nullas` to specify the string representation of NULL")
						}
					}
					if err := ed.EnsureDecoded(typs[i], alloc); err != nil {
						return err
					}
					ed.Datum.Format(f)
					csvRow[i] = f.String()
					f.Reset()
				}
				if err := writer.WriteRow(csvRow); err != nil {
					return err
				}
			}
			if rows < 1 {
				break
			}
			// Close writer to flush and write the any buffered data.
			if err := writer.Close(); err != nil {
				return errors.Wrapf(err, "failed to close exporting writer")
			}
			wrote := writer.written
			writer.reset()

			res := rowenc.EncDatumRow{
				rowenc.DatumToEncDatum(
					types.String,
					tree.NewDString(filename),
				),
				rowenc.DatumToEncDatum(
					types.Int,
					tree.NewDInt(tree.DInt(rows)),
				),
				rowenc.DatumToEncDatum(
					types.Int,
					tree.NewDInt(tree.DInt(wrote)),
				),
			}

			cs, err := sp.out.EmitRow(ctx, res)
			if err != nil {
				return err
			}
			if cs != execinfra.NeedMoreRows {
				// TODO(dt): presumably this is because our recv already closed due to
				// another error... so do we really need another one?
				return errors.New("unexpected closure of consumer")
			}
			if done {
				break
			}
		}

		return nil
	}()

	// TODO(dt): pick up tracing info in trailing meta
	execinfra.DrainAndClose(
		ctx, sp.output, err, func(context.Context) {} /* pushTrailingMeta */, sp.input)
}

func init() {
	rowexec.NewCSVWriterProcessor = newCSVWriterProcessor
}
