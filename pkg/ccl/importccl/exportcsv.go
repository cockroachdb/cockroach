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

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
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

	if err := utilccl.CheckEnterpriseEnabled(
		flowCtx.Cfg.Settings,
		flowCtx.Cfg.ClusterID.Get(),
		sql.ClusterOrganization.Get(&flowCtx.Cfg.Settings.SV),
		"EXPORT",
	); err != nil {
		return nil, err
	}

	c := &csvWriter{
		flowCtx:     flowCtx,
		processorID: processorID,
		spec:        spec,
		input:       input,
		output:      output,
	}
	if err := c.out.Init(&execinfrapb.PostProcessSpec{}, c.OutputTypes(), flowCtx.NewEvalCtx(), output); err != nil {
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
	res := make([]*types.T, len(sqlbase.ExportColumns))
	for i := range res {
		res[i] = sqlbase.ExportColumns[i].Typ
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

		alloc := &sqlbase.DatumAlloc{}

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

			conf, err := cloud.ExternalStorageConfFromURI(sp.spec.Destination)
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
			res := sqlbase.EncDatumRow{
				sqlbase.DatumToEncDatum(
					types.String,
					tree.NewDString(filename),
				),
				sqlbase.DatumToEncDatum(
					types.Int,
					tree.NewDInt(tree.DInt(rows)),
				),
				sqlbase.DatumToEncDatum(
					types.Int,
					tree.NewDInt(tree.DInt(size)),
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
