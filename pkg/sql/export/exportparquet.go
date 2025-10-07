// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package export

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execopnode"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/parquet"
	"github.com/cockroachdb/cockroach/pkg/util/unique"
	"github.com/cockroachdb/errors"
)

const exportParquetFilePatternDefault = exportFilePatternPart + ".parquet"

func fileName(spec execinfrapb.ExportSpec, part string) string {
	pattern := exportParquetFilePatternDefault
	if spec.NamePattern != "" {
		pattern = spec.NamePattern
	}

	fileName := strings.Replace(pattern, exportFilePatternPart, part, -1)
	suffix := ""
	switch spec.Format.Compression {
	case roachpb.IOFileFormat_Gzip:
		suffix = ".gz"
	case roachpb.IOFileFormat_Snappy:
		suffix = ".snappy"
	}
	fileName += suffix
	return fileName
}

type parquetWriterProcessor struct {
	execinfra.ProcessorBase

	spec        execinfrapb.ExportSpec
	input       execinfra.RowSource
	inputTypes  []*types.T
	uniqueID    int64
	sch         *parquet.SchemaDefinition
	compression parquet.CompressionCodec
	memAcc      *mon.BoundAccount

	runningState exportState
	done         bool
	// New writer is created for each chunk.
	writer        *parquet.Writer
	chunk         int
	rows          int64
	buf           bytes.Buffer
	datumRowAlloc tree.Datums

	alloc tree.DatumAlloc
}

var _ execinfra.RowSourcedProcessor = &parquetWriterProcessor{}
var _ execopnode.OpNode = &parquetWriterProcessor{}

func NewParquetWriterProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.ExportSpec,
	post *execinfrapb.PostProcessSpec,
	input execinfra.RowSource,
) (execinfra.Processor, error) {
	sp := &parquetWriterProcessor{
		spec:          spec,
		input:         input,
		inputTypes:    input.OutputTypes(),
		uniqueID:      unique.GenerateUniqueInt(unique.ProcessUniqueID(flowCtx.EvalCtx.NodeID.SQLInstanceID())),
		datumRowAlloc: make([]tree.Datum, len(spec.ColNames)),
	}
	var err error
	sp.sch, err = parquet.NewSchema(spec.ColNames, sp.inputTypes)
	if err != nil {
		return nil, err
	}
	// TODO: util/parquet supports more compression formats. The
	// exporter can be updated to supported these too.
	switch spec.Format.Compression {
	case roachpb.IOFileFormat_Snappy:
		sp.compression = parquet.CompressionSnappy
	case roachpb.IOFileFormat_Gzip:
		sp.compression = parquet.CompressionGZIP
	case roachpb.IOFileFormat_Auto, roachpb.IOFileFormat_None:
		sp.compression = parquet.CompressionNone
	default:
		return nil, pgerror.Newf(pgcode.FeatureNotSupported,
			"parquet writer does not support compression format %s", spec.Format.Compression)
	}
	mon := flowCtx.Mon
	if flowCtx.TestingKnobs().Export != nil {
		knobs := flowCtx.TestingKnobs().Export.(*TestingKnobs)
		if knobs != nil && knobs.MemoryMonitor != nil {
			mon = knobs.MemoryMonitor
		}
	}
	memAcc := mon.MakeBoundAccount()
	sp.memAcc = &memAcc
	if err = sp.Init(
		ctx, sp, post, colinfo.ExportColumnTypes, flowCtx, processorID, nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{sp.input},
		},
	); err != nil {
		return nil, err
	}
	return sp, nil
}

func (sp *parquetWriterProcessor) Start(ctx context.Context) {
	ctx = sp.StartInternal(ctx, "parquetWriter")
	sp.input.Start(ctx)
	sp.runningState = exportNewChunk
}

// TODO(yuzefovich): there is a lot of duplication between two EXPORT
// processors, consider unifying them.
func (sp *parquetWriterProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for sp.State == execinfra.StateRunning {
		switch sp.runningState {
		case exportNewChunk:
			sp.buf.Reset()
			writer, err := parquet.NewWriter(sp.sch, &sp.buf, parquet.WithCompressionCodec(sp.compression))
			if err != nil {
				sp.MoveToDraining(err)
				return nil, sp.DrainHelper()
			}
			sp.writer = writer
			sp.rows = 0
			sp.runningState = exportContinueChunk
			continue

		case exportContinueChunk:
			// If the bytes.Buffer sink exceeds the target size of a Parquet
			// file, we flush before exporting any additional rows.
			if int64(sp.buf.Len()) >= sp.spec.ChunkSize {
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
			sp.datumRowAlloc = sp.datumRowAlloc[:0]
			for i, ed := range row {
				// Make a best-effort attempt to capture the memory used by the
				// parquet writer when encoding datums to write to files. In
				// many cases, the datum will be encoded to bytes and these
				// bytes will be buffered until the file is closed.
				datumAllocSize := int64(float64(ed.Size()) * eventMemoryMultipier.Get(&sp.FlowCtx.Cfg.Settings.SV))
				if err := sp.memAcc.Grow(sp.Ctx(), datumAllocSize); err != nil {
					sp.MoveToDraining(err)
					return nil, sp.DrainHelper()
				}
				if err := ed.EnsureDecoded(sp.inputTypes[i], &sp.alloc); err != nil {
					sp.MoveToDraining(err)
					return nil, sp.DrainHelper()
				}
				// If we're encoding a DOidWrapper, then we want to cast the
				// wrapped datum. Note that we don't use eval.UnwrapDatum since
				// we're not interested in evaluating the placeholders.
				sp.datumRowAlloc = append(sp.datumRowAlloc, tree.UnwrapDOidWrapper(ed.Datum))
			}
			if err := sp.writer.AddRow(sp.datumRowAlloc); err != nil {
				sp.MoveToDraining(err)
				return nil, sp.DrainHelper()
			}
			// continue building the current chunk

		case exportFlushChunk:
			// Flushes data to the buffer.
			err := sp.writer.Close()
			sp.writer = nil
			if err != nil {
				sp.MoveToDraining(errors.Wrap(err, "failed to close parquet writer"))
				return nil, sp.DrainHelper()
			}
			sp.memAcc.Clear(sp.Ctx())
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

func (sp *parquetWriterProcessor) exportFile() (rowenc.EncDatumRow, error) {
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
	filename := fileName(sp.spec, part)

	size := sp.buf.Len()

	if err := cloud.WriteFile(sp.Ctx(), es, filename, &sp.buf); err != nil {
		return nil, err
	}
	return rowenc.EncDatumRow{
		rowenc.DatumToEncDatumUnsafe(types.String, tree.NewDString(filename)),
		rowenc.DatumToEncDatumUnsafe(types.Int, tree.NewDInt(tree.DInt(sp.rows))),
		rowenc.DatumToEncDatumUnsafe(types.Int, tree.NewDInt(tree.DInt(size))),
	}, nil
}

func (sp *parquetWriterProcessor) ConsumerClosed() {
	if sp.InternalClose() {
		if sp.writer != nil {
			_ = sp.writer.Close()
		}
		sp.memAcc.Close(sp.Ctx())
	}
}

func (sp *parquetWriterProcessor) ChildCount(verbose bool) int {
	if _, ok := sp.input.(execopnode.OpNode); ok {
		return 1
	}
	return 0
}

func (sp *parquetWriterProcessor) Child(nth int, verbose bool) execopnode.OpNode {
	if nth == 0 {
		if n, ok := sp.input.(execopnode.OpNode); ok {
			return n
		}
		panic("input to parquetWriterProcessor is not an execopnode.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}
