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
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

const exportFilePatternPart = "%part%"
const exportFilePatternDefault = exportFilePatternPart + ".csv"
const exportLoggingPrefix = "export-csv"

var logEveryNumChunkSetting = settings.RegisterIntSetting(
	"bulkio.export.logging_frequency",
	"the number of chunks after which we will log timing information",
	30)

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
	timings     struct {
		totalTimeDecoding                 time.Duration
		totalTimeWritingToCSV             time.Duration
		totalTimeFlushing                 time.Duration
		totalTimeWritingToExternalStorage time.Duration
		totalTimeEmitingRow               time.Duration
		totalChunkTime                    time.Duration
	}
}

var _ execinfra.Processor = &csvWriter{}

func (sp *csvWriter) OutputTypes() []*types.T {
	res := make([]*types.T, len(colinfo.ExportColumns))
	for i := range res {
		res[i] = colinfo.ExportColumns[i].Typ
	}
	return res
}

func (sp *csvWriter) Run(ctx context.Context) {
	ctx, span := tracing.ChildSpan(ctx, "csvWriter")
	defer tracing.FinishSpan(span)

	err := func() error {
		typs := sp.input.OutputTypes()
		sp.input.Start(ctx)
		input := execinfra.MakeNoMetadataRowSource(sp.input, sp.output)

		alloc := &rowenc.DatumAlloc{}

		writer := newCSVExporter(sp.spec)

		nullsAs := ""
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
			chunkBegin := timeutil.Now()
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

				rowDecodeBegin := timeutil.Now()
				for i, ed := range row {
					if ed.IsNull() {
						csvRow[i] = nullsAs
						continue
					}
					if err := ed.EnsureDecoded(typs[i], alloc); err != nil {
						return err
					}
					ed.Datum.Format(f)
					csvRow[i] = f.String()
					f.Reset()
				}
				sp.timings.totalTimeDecoding += timeutil.Since(rowDecodeBegin)
				csvRowWriteBegin := timeutil.Now()
				if err := writer.Write(csvRow); err != nil {
					return err
				}
				sp.timings.totalTimeWritingToCSV += timeutil.Since(csvRowWriteBegin)
			}
			if rows < 1 {
				break
			}
			writeFlushBegin := timeutil.Now()
			if err := writer.Flush(); err != nil {
				return errors.Wrap(err, "failed to flush csv writer")
			}
			sp.timings.totalTimeFlushing += timeutil.Since(writeFlushBegin)

			externalStorageWriteBegin := timeutil.Now()
			conf, err := cloudimpl.ExternalStorageConfFromURI(sp.spec.Destination, sp.spec.User)
			if err != nil {
				return err
			}
			es, err := sp.flowCtx.Cfg.ExternalStorage(ctx, conf)
			if err != nil {
				return err
			}
			defer es.Close()

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

			if err := es.WriteFile(ctx, filename, bytes.NewReader(writer.Bytes())); err != nil {
				return err
			}
			sp.timings.totalTimeWritingToExternalStorage += timeutil.Since(externalStorageWriteBegin)
			emittingRowBegin := timeutil.Now()
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
					tree.NewDInt(tree.DInt(size)),
				),
			}

			cs, err := sp.out.EmitRow(ctx, res)
			if err != nil {
				return err
			}
			sp.timings.totalTimeEmitingRow += timeutil.Since(emittingRowBegin)
			if cs != execinfra.NeedMoreRows {
				// TODO(dt): presumably this is because our recv already closed due to
				// another error... so do we really need another one?
				return errors.New("unexpected closure of consumer")
			}
			sp.timings.totalChunkTime += timeutil.Since(chunkBegin)

			if log.V(2) {
				if int64(chunk)%logEveryNumChunkSetting.Get(&sp.flowCtx.EvalCtx.Settings.SV) == 0 {
					sp.logTimings(ctx, chunk)
				}
			}

			if done {
				sp.logTimings(ctx, chunk)
				break
			}
		}

		return nil
	}()

	// TODO(dt): pick up tracing info in trailing meta
	execinfra.DrainAndClose(
		ctx, sp.output, err, func(context.Context) {} /* pushTrailingMeta */, sp.input)
}

func (sp *csvWriter) logTimings(ctx context.Context, chunk int) {
	log.Infof(ctx, "%s: processed %d chunks, %d rows in %s", exportLoggingPrefix, chunk,
		int64(chunk)*sp.spec.ChunkRows, sp.timings.totalChunkTime.String())
	log.Infof(ctx, "%s: time spent decoding %s, writing to csv %s, flushing %s, "+
		"writing to external storage %s, emitting results row %s", exportLoggingPrefix,
		sp.timings.totalTimeDecoding.String(), sp.timings.totalTimeWritingToCSV.String(),
		sp.timings.totalTimeFlushing.String(), sp.timings.totalTimeWritingToExternalStorage.
			String(), sp.timings.totalTimeEmitingRow.String())
	log.Infof(ctx, "%s: rate in sec/chunk - total %d, decoding %d, writing %d, flushing %d,"+
		" external storage %d, emitting result row %d", exportLoggingPrefix,
		int64(sp.timings.totalChunkTime.Seconds())/int64(chunk),
		int64(sp.timings.totalTimeDecoding.Seconds())/int64(chunk),
		int64(sp.timings.totalTimeWritingToCSV.Seconds())/int64(chunk),
		int64(sp.timings.totalTimeFlushing.Seconds())/int64(chunk),
		int64(sp.timings.totalTimeWritingToExternalStorage.Seconds())/int64(chunk),
		int64(sp.timings.totalTimeEmitingRow.Seconds())/int64(chunk))
}

func init() {
	rowexec.NewCSVWriterProcessor = newCSVWriterProcessor
}
