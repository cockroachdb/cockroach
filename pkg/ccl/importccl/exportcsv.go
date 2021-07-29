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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

const exportFilePatternPart = "%part%"
const exportFilePatternDefault = exportFilePatternPart + ".csv"

// TODO(dt): make this true for 21.2.
var parallelizedExport = settings.RegisterBoolSetting(
	"bulkio.export.pipelined_export.enabled",
	"enable the new EXPORT process that uses multiple threads to pipeline some of the processing steps",
	false,
)

// csvExporter data structure to augment the compression
// and csv writer, encapsulating the internals to make
// exporting oblivious for the consumers.
type csvExporter struct {
	compressor *gzip.Writer
	buf        *bytes.Buffer
	csvWriter  *csv.Writer
}

// Write append record to csv file.x
func (c *csvExporter) Write(record []string) error {
	return c.csvWriter.Write(record)
}

// Close closes the compressor writer which
// appends archive footers.
func (c *csvExporter) Close() error {
	if c.compressor != nil {
		return c.compressor.Close()
	}
	return nil
}

// Flush flushes both csv and compressor writer if
// initialized.
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
		// Brings compressor to its initial state.
		c.compressor.Reset(c.buf)
	}
}

// Bytes results in the slice of bytes with compressed content.
func (c *csvExporter) Bytes() []byte {
	return c.buf.Bytes()
}

// Len returns length of the buffer with content.
func (c *csvExporter) Len() int {
	return c.buf.Len()
}

func csvFileName(spec execinfrapb.CSVWriterSpec, part string) string {
	pattern := exportFilePatternDefault
	if spec.NamePattern != "" {
		pattern = spec.NamePattern
	}

	fileName := strings.Replace(pattern, exportFilePatternPart, part, -1)

	switch spec.CompressionCodec {
	case execinfrapb.FileCompression_Gzip:
		fileName += ".gz"
	default:
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

	if parallelizedExport.Get(&flowCtx.Cfg.Settings.SV) {
		c.batchCh = make(chan *[]string)

		// Handing batches of 1k rows from the string serializer to the writer should
		// minimize the chan overhead, but we may want to lower more closely comply
		// with small requested file row/byte sizes.
		batchRows := 1000
		if spec.ChunkRows > 0 && spec.ChunkRows < int64(batchRows) {
			batchRows = int(spec.ChunkRows)
		} else if spec.ChunkSize > 0 && spec.ChunkSize < 1<<20 {
			batchRows = 10
		}

		c.batchPool = sync.Pool{
			New: func() interface{} {
				batch := make([]string, batchRows*len(input.OutputTypes()))
				return &batch
			},
		}
	}

	semaCtx := tree.MakeSemaContext()
	if err := c.out.Init(&execinfrapb.PostProcessSpec{}, c.OutputTypes(), &semaCtx, flowCtx.NewEvalCtx()); err != nil {
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
	// batchCh carries batches of stringified rows, flattened from [][]string.
	batchCh       chan *[]string
	batchPool     sync.Pool
	batchMemLimit int64
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

func (sp *csvWriter) Run(ctx context.Context) {
	ctx, span := tracing.ChildSpan(ctx, "csvWriter")
	defer span.Finish()

	var err error
	if sp.batchCh != nil {
		err = sp.runPipelined(ctx)
	} else {
		err = sp.singleThreadRun(ctx)
	}

	// TODO(dt): pick up tracing info in trailing meta
	execinfra.DrainAndClose(
		ctx, sp.output, err, func(context.Context) {} /* pushTrailingMeta */, sp.input)
}

func (sp *csvWriter) runPipelined(ctx context.Context) error {

	acct := sp.flowCtx.EvalCtx.Mon.MakeBoundAccount()
	defer acct.Close(ctx)

	sp.batchMemLimit = 8 << 20
	const inFlightBatches = 2
	if err := acct.Grow(ctx, sp.batchMemLimit*inFlightBatches); err != nil {
		return errors.Wrap(err, "not enough available memory to run EXPORT")
	}

	grp := ctxgroup.WithContext(ctx)
	grp.GoCtx(sp.writeFiles)
	grp.GoCtx(sp.stringify)
	return grp.Wait()
}

func (sp *csvWriter) singleThreadRun(ctx context.Context) error {
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
			// If the bytes.Buffer sink exceeds the target size of a CSV file, we
			// flush before exporting any additional rows.
			if int64(writer.buf.Len()) >= sp.spec.ChunkSize {
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

		conf, err := cloud.ExternalStorageConfFromURI(sp.spec.Destination, sp.spec.User())
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
		filename := csvFileName(sp.spec, part)
		// Close writer to ensure buffer and any compression footer is flushed.
		err = writer.Close()
		if err != nil {
			return errors.Wrapf(err, "failed to close exporting writer")
		}

		size := writer.Len()

		if err := cloud.WriteFile(ctx, es, filename, bytes.NewReader(writer.Bytes())); err != nil {
			return err
		}
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

		cs, err := sp.out.EmitRow(ctx, res, sp.output)
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
}

// stringify converts incoming rows from sql datums to the strings that will be
// in each cell of a csv, sending produced [][]string batches to sp.batchCh.
func (sp *csvWriter) stringify(ctx context.Context) error {
	defer close(sp.batchCh)

	typs := sp.input.OutputTypes()
	sp.input.Start(ctx)
	input := execinfra.MakeNoMetadataRowSource(sp.input, sp.output)

	alloc := &rowenc.DatumAlloc{}

	var nullsAs string
	if sp.spec.Options.NullEncoding != nil {
		nullsAs = *sp.spec.Options.NullEncoding
	}
	f := tree.NewFmtCtx(tree.FmtExport)
	defer f.Close()

	before := timeutil.Now()

	done := false
	for {
		batch := *sp.batchPool.Get().(*[]string)
		var batchPos int
		var batchMemSize int
		for {
			row, err := input.NextRow()
			if err != nil {
				return err
			}
			if row == nil {
				done = true
				break
			}

			for i, ed := range row {
				if ed.IsNull() {
					if sp.spec.Options.NullEncoding != nil {
						batch[batchPos+i] = nullsAs
						batchMemSize += len(nullsAs)
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
				batch[batchPos+i] = f.String()
				batchMemSize += len(batch[batchPos+i])
				f.Reset()
			}
			batchPos += len(row)

			if batchPos == len(batch) || int64(batchMemSize) >= sp.batchMemLimit {
				break
			}
		}

		batch = batch[:batchPos]

		if len(batch) > 0 {
			sp.batchCh <- &batch
			if log.V(1) {
				now := timeutil.Now()
				log.VEventf(ctx, 1, "accumulated %d rows / %db in %02.fs", len(batch)/len(typs), batchMemSize, now.Sub(before).Seconds())
				before = now
			}
		}
		if done {
			break
		}
	}
	return nil
}

// writeFiles reads batches of stringified rows off sp.batchCh and writes them
// to the csv file, optionally compressing them as well.
func (sp *csvWriter) writeFiles(ctx context.Context) error {
	ctxDone := ctx.Done()
	writer := newCSVExporter(sp.spec)

	conf, err := cloud.ExternalStorageConfFromURI(sp.spec.Destination, sp.spec.User())
	if err != nil {
		return err
	}
	es, err := sp.flowCtx.Cfg.ExternalStorage(ctx, conf)
	if err != nil {
		return err
	}
	defer es.Close()

	chunk := 0
	for {
		var rows int
		writer.ResetBuffer()
		beforeAccumulate := timeutil.Now()

		for {
			flush := false
			select {
			case <-ctxDone:
				return ctx.Err()
			case batchPtr := <-sp.batchCh:
				if batchPtr == nil {
					flush = true
					break
				}
				batch := *batchPtr
				rowWidth := len(sp.input.OutputTypes())
				for rowStart := 0; rowStart < len(batch); rowStart += rowWidth {
					if err := writer.Write(batch[rowStart : rowStart+rowWidth]); err != nil {
						return err
					}
				}
				rows += len(batch) / rowWidth
				batch = batch[:cap(batch)]
				sp.batchPool.Put(&batch)
				// If the bytes.Buffer sink exceeds the target size of a CSV file, we
				// flush before exporting any additional rows.
				if int64(writer.buf.Len()) >= sp.spec.ChunkSize {
					flush = true
				}
				if sp.spec.ChunkRows > 0 && int64(rows) >= sp.spec.ChunkRows {
					flush = true
				}
			}
			if flush {
				break
			}
		}

		if rows == 0 {
			break
		}

		if err := writer.Flush(); err != nil {
			return errors.Wrap(err, "failed to flush csv writer")
		}

		// Close writer to ensure buffer and any compression footer is flushed.
		if err := writer.Close(); err != nil {
			return errors.Wrapf(err, "failed to close exporting writer")
		}

		size := writer.Len()

		nodeID, err := sp.flowCtx.EvalCtx.NodeID.OptionalNodeIDErr(47970)
		if err != nil {
			return err
		}

		part := fmt.Sprintf("n%d.%d", nodeID, chunk)
		chunk++
		filename := csvFileName(sp.spec, part)
		beforeUpload := timeutil.Now()
		log.VEventf(ctx, 1, "accumulated %d rows / %d bytes in %02.fs", rows, size, beforeUpload.Sub(beforeAccumulate).Seconds())
		if err := cloud.WriteFile(ctx, es, filename, bytes.NewReader(writer.Bytes())); err != nil {
			return err
		}
		log.VEventf(ctx, 1, "uploaded %d rows / %d bytes in %02.fs", rows, size, timeutil.Since(beforeUpload).Seconds())

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

		cs, err := sp.out.EmitRow(ctx, res, sp.output)
		if err != nil {
			return err
		}
		if cs != execinfra.NeedMoreRows {
			// TODO(dt): presumably this is because our recv already closed due to
			// another error... so do we really need another one?
			return errors.New("unexpected closure of consumer")
		}
		rows = 0
	}

	return nil
}

func init() {
	rowexec.NewCSVWriterProcessor = newCSVWriterProcessor
}
