// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/parquet"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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

func newParquetWriterProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.ExportSpec,
	post *execinfrapb.PostProcessSpec,
	input execinfra.RowSource,
) (execinfra.Processor, error) {
	c := &parquetWriterProcessor{
		flowCtx:     flowCtx,
		processorID: processorID,
		spec:        spec,
		input:       input,
	}
	semaCtx := tree.MakeSemaContext(nil /* resolver */)
	if err := c.out.Init(ctx, post, colinfo.ExportColumnTypes, &semaCtx, flowCtx.EvalCtx, flowCtx); err != nil {
		return nil, err
	}
	return c, nil
}

type parquetWriterProcessor struct {
	flowCtx     *execinfra.FlowCtx
	processorID int32
	spec        execinfrapb.ExportSpec
	input       execinfra.RowSource
	out         execinfra.ProcOutputHelper
}

var _ execinfra.Processor = &parquetWriterProcessor{}

func (sp *parquetWriterProcessor) OutputTypes() []*types.T {
	return sp.out.OutputTypes
}

// MustBeStreaming currently never gets called by the parquetWriterProcessor as
// the function only applies to implementation.
func (sp *parquetWriterProcessor) MustBeStreaming() bool {
	return false
}

func (sp *parquetWriterProcessor) Run(ctx context.Context, output execinfra.RowReceiver) {
	ctx, span := tracing.ChildSpan(ctx, "parquetWriter")
	defer span.Finish()

	knobs := sp.testingKnobsOrNil()
	mon := sp.flowCtx.Mon
	if knobs != nil && knobs.MemoryMonitor != nil {
		mon = knobs.MemoryMonitor
	}
	memAcc := mon.MakeBoundAccount()
	defer memAcc.Close(ctx)

	instanceID := sp.flowCtx.EvalCtx.NodeID.SQLInstanceID()
	uniqueID := unique.GenerateUniqueInt(unique.ProcessUniqueID(instanceID))

	err := func() error {
		typs := sp.input.OutputTypes()
		sp.input.Start(ctx)
		input := execinfra.MakeNoMetadataRowSource(sp.input, output)
		alloc := &tree.DatumAlloc{}
		datumRowAlloc := make([]tree.Datum, len(sp.spec.ColNames))

		var buf bytes.Buffer
		sch, err := parquet.NewSchema(sp.spec.ColNames, typs)
		if err != nil {
			return err
		}

		// TODO: util/parquet supports more compression formats. The
		// exporter can be updated to supported these too.
		var compression parquet.CompressionCodec
		switch sp.spec.Format.Compression {
		case roachpb.IOFileFormat_Snappy:
			compression = parquet.CompressionSnappy
		case roachpb.IOFileFormat_Gzip:
			compression = parquet.CompressionGZIP
		case roachpb.IOFileFormat_Auto, roachpb.IOFileFormat_None:
			compression = parquet.CompressionNone
		default:
			return pgerror.Newf(pgcode.FeatureNotSupported,
				"parquet writer does not support compression format %s", sp.spec.Format.Compression)
		}

		chunk := 0
		done := false
		for {
			var rows int64
			buf.Reset()
			writer, err := parquet.NewWriter(sch, &buf, parquet.WithCompressionCodec(compression))
			if err != nil {
				return err
			}
			cummulativeAllocSize := int64(0)
			for {
				// If the bytes.Buffer sink exceeds the target size of a Parquet file, we
				// flush before exporting any additional rows.
				if int64(buf.Len()) >= sp.spec.ChunkSize {
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
				datumRowAlloc = datumRowAlloc[:0]
				for i, ed := range row {
					// Make a best-effort attempt to capture the memory used by the parquet writer
					// when encoding datums to write to files. In many cases, the datum will be
					// encoded to bytes and these bytes will be buffered until the file is closed.
					datumAllocSize := int64(float64(ed.Size()) * eventMemoryMultipier.Get(&sp.flowCtx.Cfg.Settings.SV))
					cummulativeAllocSize += datumAllocSize
					if err := memAcc.Grow(ctx, datumAllocSize); err != nil {
						return err
					}
					if !ed.IsNull() {
						if err := ed.EnsureDecoded(typs[i], alloc); err != nil {
							return err
						}
					}
					// If we're encoding a DOidWrapper, then we want to cast
					// the wrapped datum. Note that we don't use
					// eval.UnwrapDatum since we're not interested in
					// evaluating the placeholders.
					datumRowAlloc = append(datumRowAlloc, tree.UnwrapDOidWrapper(ed.Datum))
				}
				if err := writer.AddRow(datumRowAlloc); err != nil {
					return err
				}
			}
			if rows < 1 {
				break
			}
			// Flushes data to the buffer.
			if err := writer.Close(); err != nil {
				return errors.Wrap(err, "failed to close parquet writer")
			}
			memAcc.Shrink(ctx, cummulativeAllocSize)

			res, err := func() (rowenc.EncDatumRow, error) {
				conf, err := cloud.ExternalStorageConfFromURI(sp.spec.Destination, sp.spec.User())
				if err != nil {
					return nil, err
				}
				es, err := sp.flowCtx.Cfg.ExternalStorage(ctx, conf)
				if err != nil {
					return nil, err
				}
				defer es.Close()

				part := fmt.Sprintf("n%d.%d", uniqueID, chunk)
				chunk++
				filename := fileName(sp.spec, part)

				size := buf.Len()

				if err := cloud.WriteFile(ctx, es, filename, &buf); err != nil {
					return nil, err
				}
				return rowenc.EncDatumRow{
					rowenc.DatumToEncDatum(types.String, tree.NewDString(filename)),
					rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(rows))),
					rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(size))),
				}, nil
			}()
			if err != nil {
				return err
			}

			cs, err := sp.out.EmitRow(ctx, res, output)
			if err != nil {
				return err
			}
			if cs != execinfra.NeedMoreRows {
				// We don't return an error here because we want the error (if any) that
				// actually caused the consumer to enter a closed/draining state to take precendence.
				return nil
			}
			if done {
				break
			}
		}

		return nil
	}()

	execinfra.DrainAndClose(ctx, sp.flowCtx, sp.input, output, err)
}

// Resume is part of the execinfra.Processor interface.
func (sp *parquetWriterProcessor) Resume(output execinfra.RowReceiver) {
	panic("not implemented")
}

// Close is part of the execinfra.Processor interface.
func (*parquetWriterProcessor) Close(context.Context) {}

// Resume is part of the execinfra.Processor interface.
func (sp *parquetWriterProcessor) testingKnobsOrNil() *ExportTestingKnobs {
	if sp.flowCtx.TestingKnobs().Export == nil {
		return nil
	}
	return sp.flowCtx.TestingKnobs().Export.(*ExportTestingKnobs)
}

func init() {
	rowexec.NewParquetWriterProcessor = newParquetWriterProcessor
}
