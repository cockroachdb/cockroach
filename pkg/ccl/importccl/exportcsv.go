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
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

// exportHeader is the header for EXPORT stmt results.
var exportHeader = sqlbase.ResultColumns{
	{Name: "filename", Typ: types.String},
	{Name: "rows", Typ: types.Int},
	{Name: "bytes", Typ: types.Int},
}

const (
	exportOptionDelimiter = "delimiter"
	exportOptionNullAs    = "nullas"
	exportOptionChunkSize = "chunk_rows"
	exportOptionFileName  = "filename"
)

var exportOptionExpectValues = map[string]sql.KVStringOptValidate{
	exportOptionChunkSize: sql.KVStringOptRequireValue,
	exportOptionDelimiter: sql.KVStringOptRequireValue,
	exportOptionFileName:  sql.KVStringOptRequireValue,
	exportOptionNullAs:    sql.KVStringOptRequireValue,
}

const exportChunkSizeDefault = 100000
const exportFilePatternPart = "%part%"
const exportFilePatternDefault = exportFilePatternPart + ".csv"

// exportPlanHook implements sql.PlanHook.
func exportPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, error) {
	exportStmt, ok := stmt.(*tree.Export)
	if !ok {
		return nil, nil, nil, nil
	}

	fileFn, err := p.TypeAsString(exportStmt.File, "EXPORT")
	if err != nil {
		return nil, nil, nil, err
	}

	if exportStmt.FileFormat != "CSV" {
		return nil, nil, nil, errors.Errorf("unsupported export format: %q", exportStmt.FileFormat)
	}

	optsFn, err := p.TypeAsStringOpts(exportStmt.Options, exportOptionExpectValues)
	if err != nil {
		return nil, nil, nil, err
	}

	sel, err := p.Select(ctx, exportStmt.Query, nil)
	if err != nil {
		return nil, nil, nil, err
	}

	fn := func(ctx context.Context, plans []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, exportStmt.StatementTag())
		defer tracing.FinishSpan(span)

		if err := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(), "EXPORT",
		); err != nil {
			return err
		}

		if err := p.RequireSuperUser(ctx, "EXPORT"); err != nil {
			return err
		}

		if !p.ExtendedEvalContext().TxnImplicit {
			return errors.Errorf("EXPORT cannot be used inside a transaction")
		}

		file, err := fileFn()
		if err != nil {
			return err
		}

		opts, err := optsFn()
		if err != nil {
			return err
		}

		csvOpts := roachpb.CSVOptions{}

		if override, ok := opts[exportOptionDelimiter]; ok {
			csvOpts.Comma, err = util.GetSingleRune(override)
			if err != nil {
				return pgerror.NewError(pgerror.CodeInvalidParameterValueError, "invalid delimiter")
			}
		}

		if override, ok := opts[exportOptionNullAs]; ok {
			csvOpts.NullEncoding = &override
		}

		chunk := exportChunkSizeDefault
		if override, ok := opts[exportOptionChunkSize]; ok {
			chunk, err = strconv.Atoi(override)
			if err != nil {
				return pgerror.NewError(pgerror.CodeInvalidParameterValueError, err.Error())
			}
			if chunk < 1 {
				return pgerror.NewError(pgerror.CodeInvalidParameterValueError, "invalid csv chunk size")
			}
		}

		out := distsqlrun.ProcessorCoreUnion{CSVWriter: &distsqlrun.CSVWriterSpec{
			Destination: file,
			NamePattern: exportFilePatternDefault,
			Options:     csvOpts,
			ChunkRows:   int64(chunk),
		}}

		rows := sqlbase.NewRowContainer(
			p.ExtendedEvalContext().Mon.MakeBoundAccount(), sqlbase.ColTypeInfoFromColTypes(sql.ExportPlanResultTypes), 0,
		)
		rw := sql.NewRowResultWriter(rows)

		if err := sql.PlanAndRunExport(
			ctx, p.DistSQLPlanner(), p.ExecCfg(), p.Txn(), p.ExtendedEvalContext(), plans[0], out, rw,
		); err != nil {
			return err
		}
		for i := 0; i < rows.Len(); i++ {
			resultsCh <- rows.At(i)
		}
		rows.Close(ctx)
		return rw.Err()
	}

	return fn, exportHeader, []sql.PlanNode{sel}, nil
}

func newCSVWriterProcessor(
	flowCtx *distsqlrun.FlowCtx,
	processorID int32,
	spec distsqlrun.CSVWriterSpec,
	input distsqlrun.RowSource,
	output distsqlrun.RowReceiver,
) (distsqlrun.Processor, error) {
	c := &csvWriter{
		flowCtx:     flowCtx,
		processorID: processorID,
		spec:        spec,
		input:       input,
		output:      output,
	}
	if err := c.out.Init(&distsqlrun.PostProcessSpec{}, sql.ExportPlanResultTypes, flowCtx.NewEvalCtx(), output); err != nil {
		return nil, err
	}
	return c, nil
}

type csvWriter struct {
	flowCtx     *distsqlrun.FlowCtx
	processorID int32
	spec        distsqlrun.CSVWriterSpec
	input       distsqlrun.RowSource
	out         distsqlrun.ProcOutputHelper
	output      distsqlrun.RowReceiver
}

var _ distsqlrun.Processor = &csvWriter{}

func (sp *csvWriter) OutputTypes() []sqlbase.ColumnType {
	return sql.ExportPlanResultTypes
}

func (sp *csvWriter) Run(ctx context.Context, wg *sync.WaitGroup) {
	ctx, span := tracing.ChildSpan(ctx, "csvWriter")
	defer tracing.FinishSpan(span)

	if wg != nil {
		defer wg.Done()
	}

	err := func() error {
		pattern := exportFilePatternDefault
		if sp.spec.NamePattern != "" {
			pattern = sp.spec.NamePattern
		}

		types := sp.input.OutputTypes()
		sp.input.Start(ctx)
		input := distsqlrun.MakeNoMetadataRowSource(sp.input, sp.output)

		alloc := &sqlbase.DatumAlloc{}

		var buf bytes.Buffer
		writer := csv.NewWriter(&buf)
		if sp.spec.Options.Comma != 0 {
			writer.Comma = sp.spec.Options.Comma
		}
		nullsAs := ""
		if sp.spec.Options.NullEncoding != nil {
			nullsAs = *sp.spec.Options.NullEncoding
		}
		f := tree.NewFmtCtxWithBuf(tree.FmtParseDatums)
		defer f.Close()

		csvRow := make([]string, len(types))

		chunk := 0
		done := false
		for {
			var rows int64
			buf.Reset()
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
					if err := ed.EnsureDecoded(&types[i], alloc); err != nil {
						return err
					}
					ed.Datum.Format(&f.FmtCtx)
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
			writer.Flush()

			conf, err := storageccl.ExportStorageConfFromURI(sp.spec.Destination)
			if err != nil {
				return err
			}
			es, err := storageccl.MakeExportStorage(ctx, conf, sp.flowCtx.Settings)
			if err != nil {
				return err
			}
			defer es.Close()

			size := buf.Len()

			part := fmt.Sprintf("n%d.%d", sp.flowCtx.EvalCtx.NodeID, chunk)
			chunk++
			filename := strings.Replace(pattern, exportFilePatternPart, part, -1)
			if err := es.WriteFile(ctx, filename, bytes.NewReader(buf.Bytes())); err != nil {
				return err
			}
			res := sqlbase.EncDatumRow{
				sqlbase.DatumToEncDatum(
					sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING},
					tree.NewDString(filename),
				),
				sqlbase.DatumToEncDatum(
					sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT},
					tree.NewDInt(tree.DInt(rows)),
				),
				sqlbase.DatumToEncDatum(
					sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT},
					tree.NewDInt(tree.DInt(size)),
				),
			}

			cs, err := sp.out.EmitRow(ctx, res)
			if err != nil {
				return err
			}
			if cs != distsqlrun.NeedMoreRows {
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
	distsqlrun.DrainAndClose(
		ctx, sp.output, err, func(context.Context) {} /* pushTrailingMeta */, sp.input)
}

func init() {
	sql.AddPlanHook(exportPlanHook)
	distsqlrun.NewCSVWriterProcessor = newCSVWriterProcessor
}
