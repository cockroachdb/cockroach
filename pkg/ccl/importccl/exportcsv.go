// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

const exportFilePatternPart = "%part%"
const exportFilePatternDefault = exportFilePatternPart + ".csv"

// csvExporter data structure to augment the compression
// and csv writer, encapsulating the internals to make
// exporting oblivious for the consumers
type csvExporter struct {
	compressor *gzip.Writer
	buf        *bytes.Buffer
	csvWriter  *csv.Writer
}

// Write append record to csv file
func (c *csvExporter) Write(record []string) error {
	return c.csvWriter.Write(record)
}

// Close closes the compressor writer which
// appends archive footers
func (c *csvExporter) Close() error {
	if c.compressor != nil {
		return c.compressor.Close()
	}
	return nil
}

// Flush flushes both csv and compressor writer if
// initialized
func (c *csvExporter) Flush() error {
	c.csvWriter.Flush()
	if c.compressor != nil {
		return c.compressor.Flush()
	}
	return nil
}

// ResetBuffer resets the buffer and compressor state.
func (c *csvExporter) ResetBuffer() {
	c.buf.Reset()
	if c.compressor != nil {
		// Brings compressor to its initial state
		c.compressor.Reset(c.buf)
	}
}

// Bytes results in the slice of bytes with compressed content
func (c *csvExporter) Bytes() []byte {
	return c.buf.Bytes()
}

// Len returns length of the buffer with content
func (c *csvExporter) Len() int {
	return c.buf.Len()
}

func (c *csvExporter) FileName(spec execinfrapb.CSVWriterSpec, part string) string {
	pattern := exportFilePatternDefault
	if spec.NamePattern != "" {
		pattern = spec.NamePattern
	}

	fileName := strings.Replace(pattern, exportFilePatternPart, part, -1)
	// TODO: add suffix based on compressor type
	if c.compressor != nil {
		fileName += ".gz"
	}
	return fileName
}

func newCSVExporter(sp execinfrapb.CSVWriterSpec) *csvExporter {
	buf := bytes.NewBuffer([]byte{})
	var exporter *csvExporter
	switch sp.CompressionCodec {
	case execinfrapb.FileCompression_Gzip:
		{
			writer := gzip.NewWriter(buf)
			exporter = &csvExporter{
				compressor: writer,
				buf:        buf,
				csvWriter:  csv.NewWriter(writer),
			}
		}
	default:
		{
			exporter = &csvExporter{
				buf:       buf,
				csvWriter: csv.NewWriter(buf),
			}
		}
	}
	if sp.Options.Comma != 0 {
		exporter.csvWriter.Comma = sp.Options.Comma
	}
	return exporter
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
		csvFileCh:   make(chan csvFile),
		resCh:       make(chan csvFile),
		emitResDone: make(chan struct{}),
	}
	semaCtx := tree.MakeSemaContext()
	if err := c.out.Init(&execinfrapb.PostProcessSpec{}, c.OutputTypes(), &semaCtx, flowCtx.NewEvalCtx(), output); err != nil {
		return nil, err
	}
	return c, nil
}

type csvFile struct {
	filename string
	content  []byte
	size     int
	rows     int64
}

type csvWriter struct {
	flowCtx     *execinfra.FlowCtx
	processorID int32
	spec        execinfrapb.CSVWriterSpec
	input       execinfra.RowSource
	out         execinfra.ProcOutputHelper
	output      execinfra.RowReceiver
	csvFileCh   chan csvFile
	resCh       chan csvFile
	emitResDone chan struct{}
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

func (sp *csvWriter) uploadCSVFile(ctx context.Context) error {
	conf, err := cloudimpl.ExternalStorageConfFromURI(sp.spec.Destination, sp.spec.User())
	if err != nil {
		return err
	}
	es, err := sp.flowCtx.Cfg.ExternalStorage(sp.flowCtx.EvalCtx.Context, conf)
	if err != nil {
		return err
	}

	for file := range sp.csvFileCh {
		if err := es.WriteFile(ctx, file.filename, bytes.NewReader(file.content)); err != nil {
			return err
		}
		select {
		case <-sp.emitResDone:
			// Goroutine emitting the result rows has closed unexpectedly.
			return nil
		case sp.resCh <- file:
		}
	}
	return nil
}

func (sp *csvWriter) emitResultRow(ctx context.Context) error {
	for res := range sp.resCh {
		res := rowenc.EncDatumRow{
			rowenc.DatumToEncDatum(
				types.String,
				tree.NewDString(res.filename),
			),
			rowenc.DatumToEncDatum(
				types.Int,
				tree.NewDInt(tree.DInt(res.rows)),
			),
			rowenc.DatumToEncDatum(
				types.Int,
				tree.NewDInt(tree.DInt(res.size)),
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
	}
	return nil
}

func (sp *csvWriter) Run(ctx context.Context) {
	ctx, span := tracing.ChildSpan(ctx, "csvWriter")
	defer span.Finish()

	numWorkers := 10
	// Leave enough space for the upload workers and the goroutine emitting a row
	// to buffer errors. We will only use the first when reporting an error.
	errCh := make(chan error, numWorkers+1)
	defer close(errCh)
	group := ctxgroup.WithContext(ctx)
	// Start workers responsible for uploading generated CSV files to external
	// storage.
	group.GoCtx(func(ctx context.Context) error {
		// Once all the workers are done uploading files there will be no more
		// result rows to be emitted.
		defer close(sp.resCh)
		return ctxgroup.GroupWorkers(ctx, numWorkers, func(ctx context.Context, _ int) error {
			if err := sp.uploadCSVFile(ctx); err != nil {
				errCh <- err
				return err
			}
			return nil
		})
	})

	// Start goroutine responsible for emitting rows corresponding to the files
	// that have been uploaded by the above workers.
	group.GoCtx(func(ctx context.Context) error {
		defer close(sp.emitResDone)
		if err := sp.emitResultRow(ctx); err != nil {
			errCh <- err
			return err
		}
		return nil
	})

	err := func() error {
		// No more CSV files will be uploaded once this function returns.
		defer close(sp.csvFileCh)
		typs := sp.input.OutputTypes()
		sp.input.Start(ctx)
		input := execinfra.MakeNoMetadataRowSource(sp.input, sp.output)

		alloc := &rowenc.DatumAlloc{}

		writer := newCSVExporter(sp.spec)

		var nullsAs string
		if sp.spec.Options.NullEncoding != nil {
			nullsAs = *sp.spec.Options.NullEncoding
		}
		f := tree.NewFmtCtx(tree.FmtExport)
		defer f.Close()

		csvRow := make([]string, len(typs))

		chunk := 0
		done := false
		for {
			var rows int64
			writer.ResetBuffer()
			for {
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
				if err := writer.Write(csvRow); err != nil {
					return err
				}
			}
			if rows < 1 {
				break
			}
			if err := writer.Flush(); err != nil {
				return errors.Wrap(err, "failed to flush csv writer")
			}

			nodeID, err := sp.flowCtx.EvalCtx.NodeID.OptionalNodeIDErr(47970)
			if err != nil {
				return err
			}

			part := fmt.Sprintf("n%d.%d", nodeID, chunk)
			chunk++
			filename := writer.FileName(sp.spec, part)
			// Close writer to ensure buffer and any compression footer is flushed.
			err = writer.Close()
			if err != nil {
				return errors.Wrapf(err, "failed to close exporting writer")
			}

			size := writer.Len()

			// Check for an error reported by the upload workers or goroutine emitting
			// result rows. If an error is reported we can stop reading and converting
			// chunks.
			select {
			case err := <-errCh:
				return err
			default:
			}

			// Send CSV file to the upload workers.
			f := csvFile{
				filename: filename,
				content:  make([]byte, len(writer.Bytes())),
				size:     size,
				rows:     rows,
			}
			copy(f.content, writer.Bytes())
			sp.csvFileCh <- f

			if done {
				break
			}
		}

		return nil
	}()
	if err != nil {
		_ = group.Wait()
		execinfra.DrainAndClose(
			ctx, sp.output, err, func(context.Context) {} /* pushTrailingMeta */, sp.input)
		return
	}

	err = group.Wait()
	// TODO(dt): pick up tracing info in trailing meta
	execinfra.DrainAndClose(
		ctx, sp.output, err, func(context.Context) {} /* pushTrailingMeta */, sp.input)
}

func init() {
	rowexec.NewCSVWriterProcessor = newCSVWriterProcessor
}
