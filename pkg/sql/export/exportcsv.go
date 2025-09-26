// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package export

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/unique"
	"github.com/cockroachdb/errors"
)

const (
	exportFilePatternPart    = "%part%"
	exportFilePatternDefault = exportFilePatternPart + ".csv"
)

// csvExporter data structure to augment the compression
// and csv writer, encapsulating the internals to make
// exporting oblivious for the consumers.
type csvExporter struct {
	compressor *gzip.Writer
	buf        *bytes.Buffer
	csvWriter  *csv.Writer
}

// Write append record to csv file.
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

func (c *csvExporter) FileName(spec execinfrapb.ExportSpec, part string) string {
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

func newCSVExporter(sp execinfrapb.ExportSpec) *csvExporter {
	buf := bytes.NewBuffer([]byte{})
	var exporter *csvExporter
	switch sp.Format.Compression {
	case roachpb.IOFileFormat_Gzip:
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
	if sp.Format.Csv.Comma != 0 {
		exporter.csvWriter.Comma = sp.Format.Csv.Comma
	}
	return exporter
}

// exportState represents the state of the processor.
type exportState int

const (
	_ exportState = iota
	// exportNewChunk indicates that the chunk state needs to be reset for the
	// new one. The processor always transitions into the exportContinueChunk
	// state next.
	exportNewChunk
	// exportContinueChunk indicates that the processor is currently reading
	// rows from its input and buffers them up to be written as a single file.
	// Once the desired chunk size is reached, the processor transitions into the
	// exportFlushChunk state. If input is exhausted and no rows have been
	// buffered, it transitions into the exportDone state.
	exportContinueChunk
	// exportFlushChunk indicates that the processor writes a new file into the
	// external storage, producing a single output row to its consumer with
	// details about that file. The processor always transitions into the
	// exportNewChunk state next.
	exportFlushChunk
	// exportDone is the final state of the processor at which point it only
	// drains its input.
	exportDone
)

type csvWriter struct {
	execinfra.ProcessorBase

	spec       execinfrapb.ExportSpec
	input      execinfra.RowSource
	inputTypes []*types.T
	uniqueID   int64

	writer  *csvExporter
	f       *tree.FmtCtx
	nullsAs string

	runningState exportState
	done         bool
	csvRow       []string
	chunk        int
	rows         int64

	alloc tree.DatumAlloc
}

var _ execinfra.RowSourcedProcessor = &csvWriter{}
var _ execopnode.OpNode = &csvWriter{}

func NewCSVWriterProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.ExportSpec,
	post *execinfrapb.PostProcessSpec,
	input execinfra.RowSource,
) (execinfra.Processor, error) {
	sp := &csvWriter{
		spec:       spec,
		input:      input,
		inputTypes: input.OutputTypes(),
		uniqueID:   unique.GenerateUniqueInt(unique.ProcessUniqueID(flowCtx.EvalCtx.NodeID.SQLInstanceID())),
		writer:     newCSVExporter(spec),
		f:          tree.NewFmtCtx(tree.FmtExport),
		csvRow:     make([]string, len(input.OutputTypes())),
	}
	if spec.Format.Csv.NullEncoding != nil {
		sp.nullsAs = *spec.Format.Csv.NullEncoding
	}
	if err := sp.Init(
		ctx, sp, post, colinfo.ExportColumnTypes, flowCtx, processorID, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{sp.input},
		},
	); err != nil {
		return nil, err
	}
	return sp, nil
}

func (sp *csvWriter) Start(ctx context.Context) {
	ctx = sp.StartInternal(ctx, "csvWriter")
	sp.input.Start(ctx)
	if sp.spec.HeaderRow {
		if err := sp.writer.Write(sp.spec.ColNames); err != nil {
			sp.MoveToDraining(err)
		}
	}
	sp.runningState = exportNewChunk
}

func (sp *csvWriter) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for sp.State == execinfra.StateRunning {
		switch sp.runningState {
		case exportNewChunk:
			sp.writer.ResetBuffer()
			sp.rows = 0
			sp.runningState = exportContinueChunk
			continue

		case exportContinueChunk:
			// If the bytes.Buffer sink exceeds the target size of a CSV file,
			// we flush before exporting any additional rows.
			if int64(sp.writer.buf.Len()) >= sp.spec.ChunkSize {
				sp.runningState = exportFlushChunk
				continue
			}
			if sp.spec.ChunkRows > 0 && sp.rows >= sp.spec.ChunkRows {
				sp.runningState = exportFlushChunk
				continue
			}
			row, meta := sp.input.Next()
			if meta != nil {
				return nil, meta
			}
			if row == nil {
				sp.done = true
				if sp.rows > 0 {
					sp.runningState = exportFlushChunk
				} else {
					sp.runningState = exportDone
				}
				continue
			}
			sp.rows++
			for i, ed := range row {
				if ed.IsNull() {
					if sp.spec.Format.Csv.NullEncoding != nil {
						sp.csvRow[i] = sp.nullsAs
						continue
					} else {
						sp.MoveToDraining(errors.New("NULL value encountered during EXPORT, " +
							"use `WITH nullas` to specify the string representation of NULL"))
						return nil, sp.DrainHelper()
					}
				}
				if err := ed.EnsureDecoded(sp.inputTypes[i], &sp.alloc); err != nil {
					sp.MoveToDraining(err)
					return nil, sp.DrainHelper()
				}
				ed.Datum.Format(sp.f)
				sp.csvRow[i] = sp.f.String()
				sp.f.Reset()
			}
			if err := sp.writer.Write(sp.csvRow); err != nil {
				sp.MoveToDraining(err)
				return nil, sp.DrainHelper()
			}
			// continue building the current chunk

		case exportFlushChunk:
			if err := sp.writer.Flush(); err != nil {
				sp.MoveToDraining(errors.Wrap(err, "failed to flush csv writer"))
				return nil, sp.DrainHelper()
			}
			row, err := sp.exportFile()
			if err != nil {
				sp.MoveToDraining(err)
				return nil, sp.DrainHelper()
			}
			sp.runningState = exportNewChunk
			if outRow := sp.ProcessRowHelper(row); outRow != nil {
				return outRow, nil
			}
			// Either find that we're no longer in StateRunning, or proceed to
			// the next chunk.

		case exportDone:
			sp.MoveToDraining(nil /* err */)

		default:
			log.Dev.Fatalf(sp.Ctx(), "unsupported state: %d", sp.runningState)
		}
	}

	return nil, sp.DrainHelper()
}

func (sp *csvWriter) exportFile() (rowenc.EncDatumRow, error) {
	conf, err := cloud.ExternalStorageConfFromURI(sp.spec.Destination, sp.spec.User())
	if err != nil {
		return nil, err
	}
	es, err := sp.FlowCtx.Cfg.ExternalStorage(sp.Ctx(), conf)
	if err != nil {
		return nil, err
	}
	defer es.Close()

	part := fmt.Sprintf("n%d.%d", sp.uniqueID, sp.chunk)
	sp.chunk++
	filename := sp.writer.FileName(sp.spec, part)
	// Close writer to ensure buffer and any compression footer is flushed.
	err = sp.writer.Close()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to close exporting writer")
	}

	size := sp.writer.Len()

	if err := cloud.WriteFile(sp.Ctx(), es, filename, bytes.NewReader(sp.writer.Bytes())); err != nil {
		return nil, err
	}
	return rowenc.EncDatumRow{
		rowenc.DatumToEncDatum(types.String, tree.NewDString(filename)),
		rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(sp.rows))),
		rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(size))),
	}, nil
}

func (sp *csvWriter) ConsumerClosed() {
	if sp.InternalClose() {
		_ = sp.writer.Close()
		sp.f.Close()
	}
}

func (sp *csvWriter) ChildCount(verbose bool) int {
	if _, ok := sp.input.(execopnode.OpNode); ok {
		return 1
	}
	return 0
}

func (sp *csvWriter) Child(nth int, verbose bool) execopnode.OpNode {
	if nth == 0 {
		if n, ok := sp.input.(execopnode.OpNode); ok {
			return n
		}
		panic("input to csvWriter is not an execopnode.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}
